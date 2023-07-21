package concurrency

import (
	"compress/gzip"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"

	"github.com/cvmfs/ducc/lib"
	"github.com/opencontainers/go-digest"
)

var localTarFilePath string

var layerTarFileCacheCv sync.Cond
var layerTarFileCache map[string]layerTarFileInternal

type TarDownloadJob struct {
	// The compressed digest of the gzipped tar file
	CompressedDigest digest.Digest
	// The size in bytes of the compressed tar file
	CompressedSize int64
	// Pointer to the container registry from which to download the tar file
	Registry *lib.ContainerRegistry
	// Status of the download job
}

type TarFileDownloader struct {
	// The path to the local tar file cache
	CachePath string

	// Jobs to be processed
	JobsMutex sync.Mutex
	Jobs      []*TarDownloadJob

	// The number of concurrent downloads
}

func (tfd *TarFileDownloader) Work() {
	// Should feature a way to stop the entire downloader, canceling all downloads

	// Organize the jobs in some smart way

	// Start downloads according to the number of concurrent downloads

	// Wait for all downloads to finish

	// Should be able to cancel a download
}

type layerTarFileInternal struct {
	Ready              bool
	Path               string
	CompressedDigest   string
	UncompressedDigest string
	LeaseCount         int
}

type LayerTarFileLease struct {
	Path               string
	CompressedDigest   string
	UncompressedDigest string
}

func (l LayerTarFileLease) Release() {
	ReleaseLayerTar(l)
}

func initLayerTarFileCache(cachePath string) {
	// Create the local tar file cache directory if it doesn't exist
	err := os.MkdirAll(cachePath, 0755)
	if err != nil {
		// No point in continuing if we can't create the directory. Just crash the program.
		panic(fmt.Errorf("error in creating directory for layer tar file storage: %s", err))
	}
	localTarFilePath = cachePath
	layerTarFileCacheCv = sync.Cond{L: &sync.Mutex{}}
	layerTarFileCache = make(map[string]layerTarFileInternal)
}

func RequestLayerTar(layer lib.Layer) (LayerTarFileLease, error) {
	// Check if the layer is already in the local cache
	layerTarFileCacheCv.L.Lock()

	cachedFile, ok := layerTarFileCache[layer.Digest]
	if ok {
		// Increment the lease count
		cachedFile.LeaseCount += 1
		layerTarFileCache[layer.Digest] = cachedFile

		for !cachedFile.Ready {
			// File is not ready yet, wait for it to be ready
			layerTarFileCacheCv.Wait()
			cachedFile, ok = layerTarFileCache[layer.Digest]
			if !ok {
				// the tar file was removed from the cache while waiting for it to be ready
				layerTarFileCacheCv.L.Unlock()
				return LayerTarFileLease{}, fmt.Errorf("error downloading layer tar file")
			}
		}

		// File is ready, return the lease
		layerTarFileCacheCv.L.Unlock()
		return LayerTarFileLease{cachedFile.Path, cachedFile.CompressedDigest, cachedFile.UncompressedDigest}, nil
	}
	// File is not in the cache, create a new entry
	layerTarFileCache[layer.Digest] = layerTarFileInternal{false, "", layer.Digest, "", 1}
	layerTarFileCacheCv.L.Unlock()

	// Perform the download
	path, uncompressedDigest, err := fetchLayerTar(layer, localTarFilePath)
	if err != nil {
		layerTarFileCacheCv.L.Lock()
		delete(layerTarFileCache, layer.Digest)
		layerTarFileCacheCv.L.Unlock()
		return LayerTarFileLease{}, fmt.Errorf("error in fetching layer tar: %s", err)
	}

	// Update the cache entry (other threads might have increased the lease count in the meantime)
	layerTarFileCacheCv.L.Lock()
	cachedFile = layerTarFileCache[layer.Digest]
	cachedFile.Path = path
	cachedFile.UncompressedDigest = uncompressedDigest
	cachedFile.Ready = true
	layerTarFileCache[layer.Digest] = cachedFile

	// Notify all waiting threads
	layerTarFileCacheCv.Broadcast()
	layerTarFileCacheCv.L.Unlock()

	return LayerTarFileLease{cachedFile.Path, cachedFile.CompressedDigest, cachedFile.UncompressedDigest}, nil
}

func ReleaseLayerTar(lease LayerTarFileLease) {
	layerTarFileCacheCv.L.Lock()
	defer layerTarFileCacheCv.L.Unlock()

	cachedFile, ok := layerTarFileCache[lease.CompressedDigest]
	if !ok {
		fmt.Println("Could not release layer tar file: Not found in cache. This should never happen.")
		return
	}
	// Decrement the lease count
	cachedFile.LeaseCount -= 1
	if cachedFile.LeaseCount <= 0 {
		// No more leases, delete the file and remove it from the cache
		err := os.Remove(cachedFile.Path)
		if err != nil {
			// Nothing to do here, we just have to live with the file being there
			fmt.Printf("error in removing file: %s\n", err)
		}
		delete(layerTarFileCache, lease.CompressedDigest)
		return
	}
	// Save the updated cache entry
	layerTarFileCache[lease.CompressedDigest] = cachedFile
}

func fetchLayerTar(repository *lib.ContainerRepository, compressedDigest digest.Digest, tempdir string) (path string, uncompressedDigest string, errorOut error) {
	url := fmt.Sprintf("%s/blobs/%s", repository.BaseUrl(), compressedDigest.String())

	fmt.Printf("Fetching layer tar file from %s\n", url)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		errorOut = fmt.Errorf("error in creating request: %s", err)
		return
	}
	res, err := repository.Registry.PerformRequest(req)
	if err != nil {
		return "", "", fmt.Errorf("error in fetching layer: %s", err)
	}
	defer res.Body.Close()

	if 200 > res.StatusCode || res.StatusCode >= 300 {
		errorOut = fmt.Errorf("error in fetching layer: %s", res.Status)
		return
	}

	//  Create a temporary directory for the layer
	file, err := os.CreateTemp(tempdir, "")
	if err != nil {
		errorOut = fmt.Errorf("error in creating temporary file: %s", err)
		return
	}

	// Create a gzip reader for the response body
	gzipReader, err := gzip.NewReader(res.Body)
	if err != nil {
		errorOut = fmt.Errorf("error in creating gzip reader: %s", err)
		return
	}
	defer gzipReader.Close()

	checksum := sha256.New()
	tee := io.TeeReader(gzipReader, checksum)

	// write the decompressed layer to the file
	_, err = io.Copy(file, tee)
	if err != nil {
		file.Close()
		os.Remove(file.Name())
		errorOut = fmt.Errorf("error in writing to file: %s", err)
		return
	}

	file.Sync()
	path = file.Name()
	uncompressedDigest = fmt.Sprintf("sha256:%x", checksum.Sum(nil))
	errorOut = nil
	file.Close()
	return
}
