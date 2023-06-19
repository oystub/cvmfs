package localdb

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"

	_ "embed"

	dockerutil "github.com/cvmfs/ducc/docker-api"
	"github.com/cvmfs/ducc/lib"
	_ "github.com/mattn/go-sqlite3"
)

var db *sql.DB

const db_schema_version = 1

//go:embed db_schema_v1.sql
var db_schema string

func Init(databasePath string) error {
	var err error
	db, err = sql.Open("sqlite3", databasePath)
	if err != nil {
		fmt.Println("Failed to open DB:", err)
		return err
	}

	// Create the tables if they don't exist
	// Check the user version of the DB to see if we need to create the tables
	var userVersion int
	err = db.QueryRow("PRAGMA user_version").Scan(&userVersion)
	if err != nil {
		fmt.Println("Failed to get user version of the DB:", err)
		return err
	}
	if userVersion == db_schema_version {
		// The DB is already initialized
		return nil
	} else if userVersion != 0 {
		// The DB is not empty, but the schema version is not the one we expect
		fmt.Println("DB schema version is", userVersion, "but we expect", db_schema_version, ".")
		return errors.New("DB schema version is not the one we expect")
	}

	// The DB is empty, we need to create the tables
	fmt.Println("Creating tables in the DB...")
	_, err = db.Exec(db_schema)

	if err != nil {
		fmt.Println("Failed to create tables in the DB:", err)
		return err
	}

	return nil
}

func Close() {
	db.Close()
}

func AddOrUpdateWish(wish lib.Wish2) (int64, error) {
	tx, err := db.Begin()
	defer tx.Rollback() // The rollback will be ignored if the tx has been committed later in the function

	alreadyExists, id, err := wishExists(tx, wish)
	if err != nil {
		return lib.INVALID_ID, err
	}
	if alreadyExists {
		// The wish already exists, but should be updated with potential new values
		log.Println("Wish already exists, updating it...")
		stmnt := "UPDATE wishes SET createLayers = ?, createThinImage = ?, createPodman = ?, createFlat = ? WHERE id = ?"
		_, err := tx.Exec(stmnt, wish.CreateLayers, wish.CreateThinImage, wish.CreatePodman, wish.CreateFlat, id)
		if err != nil {
			return lib.INVALID_ID, err
		}
	} else {
		log.Println("Wish does not exist, inserting it...")
		stmnt := "INSERT INTO wishes (cvmfsRepo, inputUri, outputUri, source, createLayers, createThinImage, createPodman, createFlat) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
		res, err := tx.Exec(stmnt, wish.CvmfsRepo, wish.InputUri, wish.OutputUri, wish.Source, wish.CreateLayers, wish.CreateThinImage, wish.CreatePodman, wish.CreateFlat)
		if err != nil {
			return lib.INVALID_ID, err
		}
		id, err = res.LastInsertId()
		if err != nil {
			return lib.INVALID_ID, err
		}
	}

	// We have updated or inserted the wish, and ready to commit
	err = tx.Commit()
	if err != nil {
		log.Println("Failed to commit transaction:", err)
		return lib.INVALID_ID, err
	}
	return id, nil

}

func wishExists(tx *sql.Tx, wish lib.Wish2) (bool, int64, error) {
	// We say that a wish already exists if there is already one in the DB with the same cvmfsRepo, inputUri, outputUri and source
	stmnt := "SELECT id FROM wishes WHERE cvmfsRepo = ? AND inputUri = ? AND outputUri = ? AND source = ?"
	rows, err := tx.Query(stmnt, wish.CvmfsRepo, wish.InputUri, wish.OutputUri, wish.Source)
	if err != nil {
		return false, lib.INVALID_ID, err
	}
	defer rows.Close()

	var id int64
	for rows.Next() {
		err = rows.Scan(&id)
		if err != nil {
			return false, lib.INVALID_ID, err
		}
	}
	err = rows.Err()
	if err != nil {
		return false, lib.INVALID_ID, err
	}
	if id == 0 {
		return false, lib.INVALID_ID, nil
	}
	return true, id, nil
}

func GetWishById(id int64) (lib.Wish2, error) {
	stmnt := "SELECT id, cvmfsRepo, inputUri, outputUri, source, createLayers, createThinImage, createPodman, createFlat, webhookEnabled, fullSyncIntervalSec, lastConfigUpdate, lastFullSync FROM wishes WHERE id = ?"
	rows, err := db.Query(stmnt, id)
	if err != nil {
		return lib.Wish2{}, err
	}
	defer rows.Close()

	var wish lib.Wish2
	for rows.Next() {
		err = rows.Scan(&wish.Id, &wish.CvmfsRepo, &wish.InputUri, &wish.OutputUri, &wish.Source, &wish.CreateLayers, &wish.CreateThinImage, &wish.CreatePodman, &wish.CreateFlat, &wish.WebhookEnabled, &wish.FullSyncIntervalSec, &wish.LastConfigUpdate, &wish.LastFullSync)
		if err != nil {
			return lib.Wish2{}, err
		}
	}
	err = rows.Err()
	if err != nil {
		return lib.Wish2{}, err
	}
	return wish, nil
}

func GetAllWishes() ([]lib.Wish2, error) {
	stmnt := "SELECT * FROM wishes"
	rows, err := db.Query(stmnt)
	if err != nil {
		fmt.Println("Failed to get wishes from the DB:", err)
		return nil, err
	}
	defer rows.Close()

	var wishes []lib.Wish2
	for rows.Next() {
		var wish lib.Wish2
		err = rows.Scan(&wish.Id, &wish.CvmfsRepo, &wish.InputUri, &wish.OutputUri, &wish.Source, &wish.CreateLayers, &wish.CreateThinImage, &wish.CreatePodman, &wish.CreateFlat, &wish.WebhookEnabled, &wish.FullSyncIntervalSec, &wish.LastConfigUpdate, &wish.LastFullSync)
		if err != nil {
			fmt.Println("Failed to scan wish:", err)
			return nil, err
		}
		wishes = append(wishes, wish)
	}
	err = rows.Err()
	if err != nil {
		fmt.Println("Failed to get wishes from the DB:", err)
		return nil, err
	}

	fmt.Printf("%+v", wishes)
	return wishes, nil
}

func getDanglingImages() ([]lib.Image2, error) {
	stmnt := "SELECT id, scheme, registry, repository, tag, digest FROM images WHERE id NOT IN (SELECT imageId FROM wish_image)"
	rows, err := db.Query(stmnt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var images []lib.Image2
	for rows.Next() {
		var image lib.Image2
		err = rows.Scan(&image.Id, &image.Scheme, &image.Registry, &image.Repository, &image.Tag, &image.Digest)
		if err != nil {
			return nil, err
		}
		images = append(images, image)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	return images, nil
}

func GetImagesByWishId(wishId int64) ([]lib.Image2, error) {
	stmnt := "SELECT id, scheme, registry, repository, tag, digest FROM images WHERE id IN (SELECT imageId FROM wish_image WHERE wishId = ?)"
	rows, err := db.Query(stmnt, wishId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var images []lib.Image2
	for rows.Next() {
		var image lib.Image2
		err = rows.Scan(&image.Id, &image.Scheme, &image.Registry, &image.Repository, &image.Tag, &image.Digest)
		if err != nil {
			return nil, err
		}
		images = append(images, image)
	}
	err = rows.Err()
	if err != nil {
		return nil, err
	}
	fmt.Println("Got", len(images), "images for wish", wishId)
	return images, nil
}

func UpdateImagesForWish(images []lib.Image2, wishId int64) error {

	tx, err := db.Begin()
	defer tx.Rollback() // The rollback will be ignored if the tx has been committed later in the function
	// Remove all existing images from the wish
	fmt.Println("Got list of images, updating DB...")
	stmnt := "DELETE FROM wish_image WHERE wishId = ?"
	_, err = tx.Exec(stmnt, wishId)
	if err != nil {
		fmt.Println("Failed to delete existing images from the wish:", err)
		return err
	}
	// Add the new images
	for _, dbImage := range images {
		_, err := addImageForWish(tx, dbImage, wishId)
		if err != nil {
			fmt.Println("Failed to add image to the wish:", err)
			return err
		}
	}
	fmt.Println("Added", len(images), "images to the wish")

	// Success! Commit the transaction
	err = tx.Commit()
	if err != nil {
		log.Println("Failed to commit transaction:", err)
		return err
	}
	return nil
}

func addImageForWish(tx *sql.Tx, image lib.Image2, wishId int64) (int64, error) {
	alreadyExists, existing_id, err := imageExists(tx, image)
	if err != nil {
		return lib.INVALID_ID, err
	}
	var image_id int64 = existing_id
	if alreadyExists {
		// Image already exists, we just need to add the link to the wish
		stmnt := "INSERT INTO wish_image (wishId, imageId) VALUES (?, ?)"
		_, err := tx.Exec(stmnt, wishId, existing_id)
		if err != nil {
			return lib.INVALID_ID, err
		}
	} else {
		// Image does not exist, we need to add it and then add the link to the wish
		stmnt := "INSERT INTO images (scheme, registry, repository, tag, digest) VALUES (?, ?, ?, ?, ?)"
		res, err := tx.Exec(stmnt, image.Scheme, image.Registry, image.Repository, image.Tag, image.Digest)
		if err != nil {
			return lib.INVALID_ID, err
		}
		image_id, err := res.LastInsertId()
		if err != nil {
			return lib.INVALID_ID, err
		}
		stmnt = "INSERT INTO wish_image (wishId, imageId) VALUES (?, ?)"
		_, err = tx.Exec(stmnt, wishId, image_id)
		if err != nil {
			return lib.INVALID_ID, err
		}

	}
	return image_id, nil
}

func imageExists(tx *sql.Tx, image lib.Image2) (bool, int64, error) {
	// We say that an image already exists if there is already one in the DB with the same scheme, registry, repository, tag and digest
	stmnt := "SELECT id FROM images WHERE scheme = ? AND registry = ? AND repository = ? AND tag = ? AND digest = ?"
	rows, err := tx.Query(stmnt, image.Scheme, image.Registry, image.Repository, image.Tag, image.Digest)
	if err != nil {
		return false, lib.INVALID_ID, err
	}
	defer rows.Close()

	var id int64
	for rows.Next() {
		err = rows.Scan(&id)
		if err != nil {
			return false, lib.INVALID_ID, err
		}
	}
	err = rows.Err()
	if err != nil {
		return false, lib.INVALID_ID, err
	}
	if id == 0 {
		return false, lib.INVALID_ID, nil
	}
	return true, id, nil
}

func GetImageById(id int64) (lib.Image2, error) {
	stmnt := "SELECT scheme, registry, repository, tag, digest FROM images WHERE id = ?"
	rows, err := db.Query(stmnt, id)
	if err != nil {
		return lib.Image2{}, err
	}
	defer rows.Close()

	var image lib.Image2
	for rows.Next() {
		err = rows.Scan(&image.Scheme, &image.Registry, &image.Repository, &image.Tag, &image.Digest)
		if err != nil {
			return lib.Image2{}, err
		}
	}
	err = rows.Err()
	if err != nil {
		return lib.Image2{}, err
	}
	return image, nil
}

func UpdateManifestForImage(manifest dockerutil.Manifest, imageId int64) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback() // The rollback will be ignored if the tx has been committed later in the function

	// TODO: Find a better way of doing this DB operation, deleting all layers and then adding them back is not necessary.
	// Can also add all new layers in a single query instead of one by one.

	// Remove all existing linked layers for this image
	stmnt := "DELETE FROM image_layer WHERE imageId = ?"
	_, err = tx.Exec(stmnt, imageId)
	if err != nil {
		return err
	}

	if err != nil {

		return err
	}

	for _, layer := range manifest.Layers {
		print("Adding layer ", layer.Digest, " to the DB\n")
		rawDigest := strings.Split(layer.Digest, ":")[1]
		stmnt := "INSERT OR IGNORE into layers (digest) VALUES (?)"
		_, err = tx.Exec(stmnt, rawDigest)
		if err != nil {
			fmt.Println("Failed to add layer to the image:", err)
			return err
		}
		stmnt = "INSERT INTO image_layer (imageId, layerDigest) VALUES (?, ?)"
		_, err = tx.Exec(stmnt, imageId, rawDigest)
		if err != nil {
			fmt.Println("Failed to add layer to the image:", err)
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		log.Println("Failed to commit transaction:", err)
		return err
	}
	return nil
}
