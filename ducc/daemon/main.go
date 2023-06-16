package daemon

import (
	"database/sql"
	"fmt"

	"github.com/cvmfs/ducc/constants"
	cvmfs "github.com/cvmfs/ducc/cvmfs"
	"github.com/cvmfs/ducc/lib"
	"github.com/cvmfs/ducc/localdb"
)

func Main() {
	db, err := sql.Open("sqlite3", "./ducc.db")
	if err != nil {
		fmt.Println("Failed to open DB:", err)
		return
	}
	defer db.Close()

	fullUpdateAllWishes(db)
}

func fullUpdateAllWishes(db *sql.DB) error {
	wishes, err := localdb.GetAllWishes(db)
	if err != nil {
		return err
	}

	// For each wish, retrieve all wanted images
	fmt.Println("Expanding wildcards and fetching list of wanted images...")
	for _, wish := range wishes {
		err := fetchImageListForWish(db, wish.Id)
		if err != nil {
			fmt.Println("Failed to retrieve images for wish", wish.Id, ":", err)
			return err
		}
	}

	fmt.Println("Fetching manifests for all images...")
	// For each wish, get the manifest for each image
	for _, wish := range wishes {
		images, err := localdb.GetImagesByWishId(db, wish.Id)
		if err != nil {
			return err
		}
		fmt.Println("Downloading manifests for the ", len(images), " images referenced by wish", wish.InputUri, ".")
		for _, image := range images {
			err := fetchAndStoreImageManifest(db, image.Id)
			if err != nil {
				return err
			}
		}
		fmt.Println("Done downloading manifests!")
	}

	// Download all images

	fmt.Println("Converting all images...")
	for _, wish := range wishes {
		images, err := localdb.GetImagesByWishId(db, wish.Id)
		if err != nil {
			return err
		}
		err = cvmfs.CreateCatalogIntoDir(wish.CvmfsRepo, constants.SubDirInsideRepo)
		if err != nil {
			print("Impossible to create subcatalog in the directory.")
		}
		var firstError error
		for _, img := range images {
			compat_image := lib.Image{
				Scheme:     img.Scheme,
				Registry:   img.Registry,
				Repository: img.Repository,
				Tag:        img.Tag,
				Digest:     img.Digest,
			}

			err = lib.ConvertInputOutput(&compat_image, wish.CvmfsRepo, false, false)
			if err != nil && firstError == nil {
				firstError = err
			}
		}
	}
	fmt.Println("Full update complete!")
	return nil
}

func fetchImageListForWish(db *sql.DB, wishId int64) error {
	wish, err := localdb.GetWishById(db, wishId)
	if err != nil {
		return err
	}

	input, _ := lib.ParseImage(wish.InputUri)

	// Expand wildcards to get all applicable images
	var images <-chan *lib.Image
	fmt.Println("Expanding wildcard...")
	images, _, _ = input.ExpandWildcard()

	fmt.Println("Fetching images...")
	// We wait for all images before interacting with the DB
	var dbImages []localdb.DbImage
	for image := range images {
		db_image := localdb.DbImage{
			Scheme:     image.Scheme,
			Registry:   image.Registry,
			Repository: image.Repository,
			Tag:        image.Tag,
			Digest:     image.Digest,
		}
		dbImages = append(dbImages, db_image)
		fmt.Println("Got image: ", db_image.Repository, "/", db_image.Tag)
	}
	err = localdb.UpdateImagesForWish(db, dbImages, wishId)
	if err != nil {
		return err
	}
	return nil
}

func fetchAndStoreImageManifest(db *sql.DB, imageId int64) error {
	imagePair, err := localdb.GetImageById(db, imageId)
	if err != nil {
		return err
	}

	image := lib.Image{
		Scheme:     imagePair.Scheme,
		Registry:   imagePair.Registry,
		Repository: imagePair.Repository,
		Tag:        imagePair.Tag,
		Digest:     imagePair.Digest,
	}

	// Get the manifest
	manifest, err := image.GetManifest()
	if err != nil {
		fmt.Println("Failed to get manifest:", err)
		return err
	}

	err = localdb.UpdateManifestForImage(db, manifest, imageId)
	if err != nil {
		return err
	}
	return nil
}
