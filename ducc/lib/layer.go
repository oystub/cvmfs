package lib

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

type Layer struct {
	Tag       *Tag
	MediaType string
	Size      int
	Digest    string
}

func (l *Layer) Fetch() error {
	trimmedDigest := strings.TrimPrefix(l.Digest, "sha256:")

	// Create subdirectory based on first two characters of digest
	subDir := fmt.Sprintf("/tmp/ducc/layers/%s/", trimmedDigest[:2])
	err := os.MkdirAll(subDir, 0755) // Creates the directory if it doesn't exist
	if err != nil {
		fmt.Printf("error in creating directory: %s\n", err)
		return err
	}

	url := fmt.Sprintf("%s/blobs/%s", l.Tag.Repository.BaseUrl(), l.Digest)
	fmt.Printf("Fetching layer from %s\n", url)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		fmt.Printf("error in creating request: %s\n", err)
		return err
	}
	res, err := l.Tag.Repository.Registry.PerformRequest(req)
	if err != nil {
		fmt.Printf("error in fetching layer: %s\n", err)
		return err
	}
	defer res.Body.Close()
	if 200 > res.StatusCode || res.StatusCode >= 300 {
		fmt.Printf("error in fetching layer: %s\n", res.Status)
		return fmt.Errorf("error in fetching layer: %s", res.Status)
	}
	file, err := os.Create(subDir + trimmedDigest)
	if err != nil {
		fmt.Printf("error in creating file: %s\n", err)
		return err
	}
	defer file.Close()

	// Create a gzip reader for the response body
	gzipReader, err := gzip.NewReader(res.Body)
	if err != nil {
		fmt.Printf("error in creating gzip reader: %s\n", err)
		return err
	}
	defer gzipReader.Close()

	// write the decompressed layer to the file
	_, err = io.Copy(file, gzipReader)
	if err != nil {
		fmt.Printf("error in writing to file: %s\n", err)
		return err
	}
	return nil
}
