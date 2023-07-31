package concurrency

/*
import (
	"errors"
	"fmt"
	"os"
	"sync"

	cvmfs "github.com/cvmfs/ducc/cvmfs"
	"github.com/cvmfs/ducc/lib"
	"github.com/opencontainers/go-digest"
)

var layerIngestCv sync.Cond
var layerIngestCount map[string]layerIngestInternal

type layerIngestInternal struct {
	Ready     bool
	Success   bool
	WaitCount int
}

func initLayerIngest() {
	layerIngestCv = sync.Cond{L: &sync.Mutex{}}
	layerIngestCount = make(map[string]layerIngestInternal)
}

func RequestIngestedLayer(cvmfsRepo string, layer lib.Layer) error {
	layerIngestCv.L.Lock()
	defer layerIngestCv.L.Unlock()

	mapKey := cvmfsRepo + layer.Digest.String()

	status, ok := layerIngestCount[mapKey]
	if ok {
		// Layer is already being ingested, wait for it to complete
		if !status.Ready {
			// Not ready yet, wait for it to be ready
			status.WaitCount++
			layerIngestCount[mapKey] = status
			layerIngestCv.Wait()
		}
		// Layer is ready, check if it was ingested successfully
		status, ok = layerIngestCount[mapKey]
		if !ok {
			return fmt.Errorf("the layer was removed from the ingest queue while waiting for it to be ready. This should never happen")
		}
		if !status.Success {
			return fmt.Errorf("the layer was not ingested successfully")
		}

		status.WaitCount--
		if status.WaitCount == 0 {
			delete(layerIngestCount, layer.Digest.String())
		} else {
			layerIngestCount[mapKey] = status
		}
		return nil
	}

	// Let others know that we are ingesting the layer
	layerIngestCount[mapKey] = layerIngestInternal{
		Ready:     false,
		Success:   false,
		WaitCount: 1,
	}
	layerIngestCv.L.Unlock()

	// Ingest the layer. If the layer is already in CVMFS, this will be a no-op
	err := IngestLayer(cvmfsRepo, layer)
	if err != nil {
		// Ingestion failed, mark it as such
		layerIngestCv.L.Lock()
		status, ok = layerIngestCount[mapKey]
		if !ok {
			return fmt.Errorf("the layer was removed from the ingest queue while waiting for it to be ready. This should never happen")
		}
		status.Success = false
		status.Ready = true
		status.WaitCount--
		if status.WaitCount == 0 {
			delete(layerIngestCount, layer.Digest.String())
		} else {
			layerIngestCount[mapKey] = status
		}
		return fmt.Errorf("error ingesting layer: %s", err)
	}

	// Ingestion was successful, mark it as such
	layerIngestCv.L.Lock()
	status, ok = layerIngestCount[mapKey]
	if !ok {
		return fmt.Errorf("the layer was removed from the ingest queue while waiting for it to be ready. This should never happen")
	}
	status.Success = true
	status.Ready = true
	status.WaitCount--
	if status.WaitCount == 0 {
		delete(layerIngestCount, layer.Digest.String())
	} else {
		layerIngestCount[mapKey] = status
	}
	layerIngestCv.Broadcast()

	return nil
}

func IngestLayer(cvmfsRepo string, layer lib.Layer) error {
	// First, check if the layer exists in CVMFS already
	// TODO: Should we check that the repository exists here? Or is it already checked? Or should we just let the ingestion fail?
	_, err := os.Stat(fmt.Sprintf("/cvmfs/%s%s", cvmfsRepo, layerPath(layer.Digest)))
	if err == nil {
		fmt.Printf("Layer %s already exists in CVMFS\n", layer.Digest)
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		// In case of any other error than the file not existing, return it
		return fmt.Errorf("error checking if layer is in CVMFS: %s", err)
	}

	// First, get the tarball for the layer
	tarLayerLease, err := RequestLayerTar(layer)
	if err != nil {
		return fmt.Errorf("error requesting layer tar: %s", err)
	}
	defer tarLayerLease.Release()

	// Now, ingest the layer
	tarReader, err := os.Open(tarLayerLease.Path)
	if err != nil {
		return fmt.Errorf("error opening layer tar: %s", err)
	}

	readHashCloseSizer := lib.NewReadAndHash(tarReader)

	// Now, ingest the layer.
	// In order for a "transaction" to have meaning, the system has to be left in a consistent state.
	// Ideally we want to both ingest the layer and update the manifest in the same transaction.
	// However, this is currently not possible, as the tar file ingestion is done in a single transaction

	// BEGIN METATRANSACTION
	// TODO: Lock CVMFS repo
	// TODO: Improve error handling. If we do this in a proper transaction, we should be able to just rollback
	err = cvmfs.CreateCatalogIntoDir(cvmfsRepo, layerPath(layer.Digest))
	// TODO: Should the catalog be created in the parent directory instead?
	if err != nil {
		cvmfs.RemoveDirectory(cvmfsRepo, layerPath(layer.Digest))
		return fmt.Errorf("error creating catalog: %s", err)
	}
	fmt.Printf("Created catalog at %s\n", layerPath(layer.Digest))
	err = cvmfs.Ingest(cvmfsRepo, readHashCloseSizer,
		"--catalog", "-t", "-",
		"-b", layerPath(layer.Digest))
	if err != nil {
		// Ingestion has failed, try to delete the remaining files
		fmt.Printf("Ingestion failed: %s\n", err)
		cvmfs.IngestDelete(cvmfsRepo, layerPath(layer.Digest))
		cvmfs.RemoveDirectory(cvmfsRepo, layerPath(layer.Digest))
		return fmt.Errorf("error ingesting layer: %s", err)
	}
	// Save layer metadata
	err = lib.StoreLayerInfo(cvmfsRepo, layer.Digest.Encoded(), readHashCloseSizer)
	if err != nil {
		fmt.Printf("Error storing layer metadata: %s\n", err)
		cvmfs.IngestDelete(cvmfsRepo, layerPath(layer.Digest))
		cvmfs.RemoveDirectory(cvmfsRepo, layerPath(layer.Digest))
		return fmt.Errorf("error storing layer metadata: %s", err)
	}
	fmt.Println("Ingestion successful")
	// TODO: Unlock CVMFS repo
	// END METATRANSACTION

	return nil

}

func layerPath(layerDigest digest.Digest) string {
	// If the digest begins with an algorithm and : (e.g. sha256:), remove them
	return fmt.Sprintf("/.layers/%s/%s", layerDigest.Encoded()[0:2], layerDigest.Encoded())
}
*/
