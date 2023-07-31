package db

import (
	"database/sql"

	"github.com/google/uuid"
	"github.com/opencontainers/go-digest"
)

const imageIdentifierSqlFieldsOrdered string = "digest, tag, registry_scheme, registry_hostname, repository"

// const imageIdentifierSqlFieldsQuery string = "digest=? AND tag=? AND registry_scheme=? AND registry_hostname=? AND repository=?"
const imageIdentifierSqlFieldsQueryTag string = "digest IS NULL AND tag=? AND registry_scheme=? AND registry_hostname=? AND repository=?"
const imageIdentifierSqlFieldsQueryDigest string = "digest=? AND tag IS NULL AND registry_scheme=? AND registry_hostname=? AND repository=?"
const imageIdentifierSqlFieldsQs string = "?,?,?,?,?"
const imageSqlFieldsOrdered = "id, digest, tag, registry_scheme, registry_hostname, repository"

type ImageID = uuid.UUID

type Image struct {
	ID             ImageID
	RegistryScheme string
	RegistryHost   string

	Repository string
	Tag        string
	Digest     digest.Digest
}

// CreateImage takes in an image and creates it in the database. The ID field is ignored.
// It returns the same image with the ID field set, according to its assigned database ID.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func CreateImage(tx *sql.Tx, image Image) (Image, error) {
	out, err := CreateImages(tx, []Image{image})
	if err != nil {
		return Image{}, err
	}
	return out[0], nil
}

// CreateImages takes in a slice of images and creates them in the database. The ID field is ignored.
// It returns the same slice of images with the ID field set, according to their assigned database ID.
// Unless all images are created, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func CreateImages(tx *sql.Tx, images []Image) ([]Image, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	const stmnt string = "INSERT INTO images (id, " + imageIdentifierSqlFieldsOrdered + ") VALUES (" + imageIdentifierSqlFieldsQs + ",?) RETURNING " + imageSqlFieldsOrdered
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	defer prepStmnt.Close()

	out := make([]Image, 0, len(images))
	for _, image := range images {
		digest := sql.NullString{
			String: image.Digest.String(),
			Valid:  image.Digest != "",
		}
		tag := sql.NullString{
			String: image.Tag,
			Valid:  image.Tag != "",
		}
		image.ID = ImageID(uuid.New())
		row := prepStmnt.QueryRow(image.ID, digest, tag, image.RegistryScheme, image.RegistryHost, image.Repository)
		image, err := parseImageFromRow(row)
		if err != nil {
			return nil, err
		}
		out = append(out, image)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

// Gets an image from the database by comparing the identifiers,
// i.e. registry, repository, tag and digest. Id is ignored.
// If no image is found, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetImageByValue(tx *sql.Tx, image Image) (Image, error) {
	images, err := GetImagesByValues(tx, []Image{image})
	if err != nil {
		return Image{}, err
	}
	return images[0], nil
}

// Gets a slice of images from the database by comparing the identifiers,
// i.e. registry, repository, tag and digest. Id is ignored.
// Unless all images are found, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetImagesByValues(tx *sql.Tx, images []Image) ([]Image, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		var err error
		tx, err = GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	const stmntTag string = "SELECT " + imageSqlFieldsOrdered + " FROM images WHERE " + imageIdentifierSqlFieldsQueryTag
	const stmntDigest string = "SELECT " + imageSqlFieldsOrdered + " FROM images WHERE " + imageIdentifierSqlFieldsQueryDigest
	prepStmntTag, err := tx.Prepare(stmntTag)
	if err != nil {
		return nil, err
	}
	defer prepStmntTag.Close()
	prepStmntDigest, err := tx.Prepare(stmntDigest)
	if err != nil {
		return nil, err
	}
	defer prepStmntDigest.Close()

	out := make([]Image, 0, len(images))
	for _, image := range images {
		var row *sql.Row
		if image.Digest != "" {
			row = prepStmntDigest.QueryRow(image.Digest.String(), image.RegistryScheme, image.RegistryHost, image.Repository)
		} else {
			row = prepStmntTag.QueryRow(image.Tag, image.RegistryScheme, image.RegistryHost, image.Repository)
		}
		image, err := parseImageFromRow(row)
		if err != nil {
			return nil, err
		}
		out = append(out, image)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

// parseImageFromRow takes in input a row from the images table and scans it into an Image struct.
// The row must contain the exact fields of the constant imageSqlFieldsOrdered, in the same order.
func parseImageFromRow(row scannableRow) (Image, error) {
	var image Image
	var digestStr sql.NullString
	var tag sql.NullString
	err := row.Scan(&image.ID, &digestStr, &tag, &image.RegistryScheme, &image.RegistryHost, &image.Repository)
	if err != nil {
		return Image{}, err
	}
	// Handle the null values
	if digestStr.Valid {
		image.Digest, err = digest.Parse(digestStr.String)
		if err != nil {
			return Image{}, err
		}
	}
	if tag.Valid {
		image.Tag = tag.String
	}
	return image, nil
}
