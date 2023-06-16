package localdb

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	dockerutil "github.com/cvmfs/ducc/docker-api"
	_ "github.com/mattn/go-sqlite3"
)

func addOrUpdateWish(db *sql.DB, wish DbWish) (int64, error) {
	tx, err := db.Begin()
	defer tx.Rollback() // The rollback will be ignored if the tx has been committed later in the function

	alreadyExists, id, err := wishExists(tx, wish)
	if err != nil {
		return db_invalid_id, err
	}
	if alreadyExists {
		// The wish already exists, but should be updated with potential new values
		log.Println("Wish already exists, updating it...")
		stmnt := "UPDATE wishes SET createLayers = ?, createThinImage = ?, createPodman = ?, createFlat = ? WHERE id = ?"
		_, err := tx.Exec(stmnt, wish.CreateLayers, wish.CreateThinImage, wish.CreatePodman, wish.CreateFlat, id)
		if err != nil {
			return db_invalid_id, err
		}
	} else {
		log.Println("Wish does not exist, inserting it...")
		stmnt := "INSERT INTO wishes (cvmfsRepo, inputUri, outputUri, source, createLayers, createThinImage, createPodman, createFlat) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
		res, err := tx.Exec(stmnt, wish.CvmfsRepo, wish.InputUri, wish.OutputUri, wish.Source, wish.CreateLayers, wish.CreateThinImage, wish.CreatePodman, wish.CreateFlat)
		if err != nil {
			return db_invalid_id, err
		}
		id, err = res.LastInsertId()
		if err != nil {
			return db_invalid_id, err
		}
	}

	// We have updated or inserted the wish, and ready to commit
	err = tx.Commit()
	if err != nil {
		log.Println("Failed to commit transaction:", err)
		return db_invalid_id, err
	}
	return id, nil

}

func wishExists(tx *sql.Tx, wish DbWish) (bool, int64, error) {
	// We say that a wish already exists if there is already one in the DB with the same cvmfsRepo, inputUri, outputUri and source
	stmnt := "SELECT id FROM wishes WHERE cvmfsRepo = ? AND inputUri = ? AND outputUri = ? AND source = ?"
	rows, err := tx.Query(stmnt, wish.CvmfsRepo, wish.InputUri, wish.OutputUri, wish.Source)
	if err != nil {
		return false, db_invalid_id, err
	}
	defer rows.Close()

	var id int64
	for rows.Next() {
		err = rows.Scan(&id)
		if err != nil {
			return false, db_invalid_id, err
		}
	}
	err = rows.Err()
	if err != nil {
		return false, db_invalid_id, err
	}
	if id == 0 {
		return false, db_invalid_id, nil
	}
	return true, id, nil
}

func GetWishById(db *sql.DB, id int64) (DbWish, error) {
	stmnt := "SELECT id, cvmfsRepo, inputUri, outputUri, source, createLayers, createThinImage, createPodman, createFlat, webhookEnabled, fullSyncIntervalSec, lastConfigUpdate, lastFullSync FROM wishes WHERE id = ?"
	rows, err := db.Query(stmnt, id)
	if err != nil {
		return DbWish{}, err
	}
	defer rows.Close()

	var wish DbWish
	for rows.Next() {
		err = rows.Scan(&wish.Id, &wish.CvmfsRepo, &wish.InputUri, &wish.OutputUri, &wish.Source, &wish.CreateLayers, &wish.CreateThinImage, &wish.CreatePodman, &wish.CreateFlat, &wish.WebhookEnabled, &wish.FullSyncIntervalSec, &wish.LastConfigUpdate, &wish.LastFullSync)
		if err != nil {
			return DbWish{}, err
		}
	}
	err = rows.Err()
	if err != nil {
		return DbWish{}, err
	}
	return wish, nil
}

func GetAllWishes(db *sql.DB) ([]DbWish, error) {
	stmnt := "SELECT * FROM wishes"
	rows, err := db.Query(stmnt)
	if err != nil {
		fmt.Println("Failed to get wishes from the DB:", err)
		return nil, err
	}
	defer rows.Close()

	var wishes []DbWish
	for rows.Next() {
		var wish DbWish
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

func getDanglingImages(db *sql.DB) ([]DbImage, error) {
	stmnt := "SELECT id, scheme, registry, repository, tag, digest FROM images WHERE id NOT IN (SELECT imageId FROM wish_image)"
	rows, err := db.Query(stmnt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var images []DbImage
	for rows.Next() {
		var image DbImage
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

func GetImagesByWishId(db *sql.DB, wishId int64) ([]DbImage, error) {
	stmnt := "SELECT id, scheme, registry, repository, tag, digest FROM images WHERE id IN (SELECT imageId FROM wish_image WHERE wishId = ?)"
	rows, err := db.Query(stmnt, wishId)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var images []DbImage
	for rows.Next() {
		var image DbImage
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

func UpdateImagesForWish(db *sql.DB, images []DbImage, wishId int64) error {

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

func addImageForWish(tx *sql.Tx, image DbImage, wishId int64) (int64, error) {
	alreadyExists, existing_id, err := imageExists(tx, image)
	if err != nil {
		return db_invalid_id, err
	}
	var image_id int64 = existing_id
	if alreadyExists {
		// Image already exists, we just need to add the link to the wish
		stmnt := "INSERT INTO wish_image (wishId, imageId) VALUES (?, ?)"
		_, err := tx.Exec(stmnt, wishId, existing_id)
		if err != nil {
			return db_invalid_id, err
		}
	} else {
		// Image does not exist, we need to add it and then add the link to the wish
		stmnt := "INSERT INTO images (scheme, registry, repository, tag, digest) VALUES (?, ?, ?, ?, ?)"
		res, err := tx.Exec(stmnt, image.Scheme, image.Registry, image.Repository, image.Tag, image.Digest)
		if err != nil {
			return db_invalid_id, err
		}
		image_id, err := res.LastInsertId()
		if err != nil {
			return db_invalid_id, err
		}
		stmnt = "INSERT INTO wish_image (wishId, imageId) VALUES (?, ?)"
		_, err = tx.Exec(stmnt, wishId, image_id)
		if err != nil {
			return db_invalid_id, err
		}

	}
	return image_id, nil
}

func imageExists(tx *sql.Tx, image DbImage) (bool, int64, error) {
	// We say that an image already exists if there is already one in the DB with the same scheme, registry, repository, tag and digest
	stmnt := "SELECT id FROM images WHERE scheme = ? AND registry = ? AND repository = ? AND tag = ? AND digest = ?"
	rows, err := tx.Query(stmnt, image.Scheme, image.Registry, image.Repository, image.Tag, image.Digest)
	if err != nil {
		return false, db_invalid_id, err
	}
	defer rows.Close()

	var id int64
	for rows.Next() {
		err = rows.Scan(&id)
		if err != nil {
			return false, db_invalid_id, err
		}
	}
	err = rows.Err()
	if err != nil {
		return false, db_invalid_id, err
	}
	if id == 0 {
		return false, db_invalid_id, nil
	}
	return true, id, nil
}

func GetImageById(db *sql.DB, id int64) (DbImage, error) {
	stmnt := "SELECT scheme, registry, repository, tag, digest FROM images WHERE id = ?"
	rows, err := db.Query(stmnt, id)
	if err != nil {
		return DbImage{}, err
	}
	defer rows.Close()

	var image DbImage
	for rows.Next() {
		err = rows.Scan(&image.Scheme, &image.Registry, &image.Repository, &image.Tag, &image.Digest)
		if err != nil {
			return DbImage{}, err
		}
	}
	err = rows.Err()
	if err != nil {
		return DbImage{}, err
	}
	return image, nil
}

func UpdateManifestForImage(db *sql.DB, manifest dockerutil.Manifest, imageId int64) error {
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
