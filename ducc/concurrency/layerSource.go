package concurrency

import (
	"fmt"
	"os"
	"os/exec"
	"sync"

	"github.com/cvmfs/ducc/lib"
)

var localLayerPath string

var layerCacheCv sync.Cond
var layerCache map[string]localLayerInternal

type localLayerInternal struct {
	Ready              bool
	Path               string
	CompressedDigest   string
	UncompressedDigest string
	LeaseCount         int
}

type LayerLeaseType int

const (
	LL_CVMFS LayerLeaseType = iota
	LL_LOCAL LayerLeaseType = iota
)

type LayerLease struct {
	Path               string
	CompressedDigest   string
	UncompressedDigest string
	Type               LayerLeaseType
}

func initLayerCache(cachePath string, cvmfsRepo string) {
	// Create the local tar file cache directory if it doesn't exist
	err := os.MkdirAll(cachePath, 0755)
	if err != nil {
		// No point in continuing if we can't create the directory. Just crash the program.
		panic(fmt.Errorf("error in creating directory for layer storage: %s", err))
	}
	localLayerPath = cachePath
	layerCacheCv = sync.Cond{L: &sync.Mutex{}}
	layerCache = make(map[string]localLayerInternal)
}

func RequestLayer(layer lib.Layer) (LayerLease, error) {
	// Check if the layer is in CVMFS. If it is, return a CVMFS lease.

	// Check if the layer is already in the local cache
	layerCacheCv.L.Lock()
	localLayer, ok := layerCache[layer.Digest]
	if ok {
		// Increment the lease count
		localLayer.LeaseCount += 1
		layerCache[layer.Digest] = localLayer

		for !localLayer.Ready {
			// Layer is not ready yet, wait for it to be ready
			layerCacheCv.Wait()
			localLayer, ok = layerCache[layer.Digest]
			if !ok {
				// The layer was removed from the cache while waiting for it to be ready
				layerCacheCv.L.Unlock()
				return LayerLease{}, fmt.Errorf("error downloading layer")
			}
		}
		// Layer is ready, return the lease
		layerCacheCv.L.Unlock()
		return LayerLease{
			Path:               localLayer.Path,
			CompressedDigest:   localLayer.CompressedDigest,
			UncompressedDigest: localLayer.UncompressedDigest,
			Type:               LL_LOCAL,
		}, nil
	}

	// Layer is not in the cache, download it

	// Let others know that we are downloading the layer
	layerCache[layer.Digest] = localLayerInternal{
		Ready:              false,
		Path:               "",
		CompressedDigest:   layer.Digest,
		UncompressedDigest: "",
		LeaseCount:         1,
	}
	layerCacheCv.L.Unlock()

	// Download the layer
	tarFileLease, err := RequestLayerTar(layer)
	if err != nil {
		layerCacheCv.L.Lock()
		delete(layerCache, layer.Digest)
		layerCacheCv.L.Unlock()
		return LayerLease{}, fmt.Errorf("error downloading layer tar file")
	}
	// We don't need the tar file when we are done with it
	defer ReleaseLayerTar(tarFileLease)

	// Create a directory for the layer
	path, err := os.MkdirTemp(localLayerPath, "")
	if err != nil {
		layerCacheCv.L.Lock()
		delete(layerCache, layer.Digest)
		layerCacheCv.L.Unlock()
		return LayerLease{}, fmt.Errorf("error creating directory for layer: %s", err)
	}

	// Use tar to decompress and unpack the tarball
	fmt.Printf("Extracting layer %s\n", layer.Digest)
	cmd := exec.Command("tar", "-xvf", tarFileLease.Path, "-C", path)
	if err := cmd.Run(); err != nil {
		os.RemoveAll(path)
		layerCacheCv.L.Lock()
		delete(layerCache, layer.Digest)
		layerCacheCv.L.Unlock()
		return LayerLease{}, fmt.Errorf("error extracting tar file: %s", err)
	}

	// Update the cache
	layerCacheCv.L.Lock()
	localLayer = layerCache[layer.Digest]
	localLayer.Ready = true
	localLayer.Path = path
	localLayer.UncompressedDigest = tarFileLease.UncompressedDigest
	layerCache[layer.Digest] = localLayer
	layerCacheCv.Broadcast()
	layerCacheCv.L.Unlock()

	return LayerLease{
		Path:               path,
		CompressedDigest:   layer.Digest,
		UncompressedDigest: tarFileLease.UncompressedDigest,
		Type:               LL_LOCAL,
	}, nil
}

func ReleaseLayer(lease LayerLease) {
	layerCacheCv.L.Lock()
	defer layerCacheCv.L.Unlock()

	// Decrement the lease count
	localLayer, ok := layerCache[lease.CompressedDigest]
	if !ok {
		// The layer is not in the cache. This should never happen.
		panic(fmt.Errorf("error releasing layer: layer not in cache. This should never happen"))
	}
	localLayer.LeaseCount -= 1
	if localLayer.LeaseCount == 0 {
		// No one is using the layer anymore, delete the file and remove it from the cache
		err := os.RemoveAll(localLayer.Path)
		if err != nil {
			// Nothing to do here, we just have to live with the layer being there
			fmt.Printf("error removing layer: %s\n", err)
		}
		delete(layerCache, lease.CompressedDigest)
		return
	}
	// Someone is still using the layer, update the cache
	layerCache[lease.CompressedDigest] = localLayer
}
