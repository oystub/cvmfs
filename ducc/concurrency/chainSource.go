package concurrency

import (
	"archive/tar"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cvmfs/ducc/cvmfs"
	"github.com/cvmfs/ducc/lib"
)

type ChainStep struct {
	layer       lib.Layer
	chainDigest string
}

type Chain []ChainStep

type chainInternal struct {
	Ready     bool
	Success   bool
	WaitCount int
}

var ingestChainCv sync.Cond
var ingestChainCount map[string]chainInternal

func initIngestChain() {
	ingestChainCv = sync.Cond{L: &sync.Mutex{}}
	ingestChainCount = make(map[string]chainInternal)
}

func RequestIngestedChain(cvmfsRepo string, chain Chain) error {
	mapKey := cvmfsRepo + chain[len(chain)-1].chainDigest

	ingestChainCv.L.Lock()
	defer ingestChainCv.L.Unlock()
	fmt.Println("Requesting ingest chain", chain[len(chain)-1].chainDigest)

	status, ok := ingestChainCount[mapKey]
	if ok {
		// Chain is already being ingested
		if !status.Ready {
			// Not ready yet, wait for it to be ready
			status.WaitCount++
			ingestChainCount[mapKey] = status
			ingestChainCv.Wait()
		}
		// Chain is ready, check if it was ingested successfully
		status, ok = ingestChainCount[mapKey]
		if !ok {
			return fmt.Errorf("the chain was removed from the ingest queue while waiting for it to be ready. This should never happen")
		}
		if !status.Success {
			status.WaitCount--
			if status.WaitCount == 0 {
				delete(ingestChainCount, mapKey)
			} else {
				ingestChainCount[mapKey] = status
			}
			return fmt.Errorf("the chain was not ingested successfully")
		}
		return nil
	}

	// Chain is not being ingested, start ingesting it
	ingestChainCount[mapKey] = chainInternal{Ready: false, Success: false, WaitCount: 0}
	ingestChainCv.L.Unlock()
	err := IngestChain(cvmfsRepo, chain)
	time.Sleep(time.Second)
	ingestChainCv.L.Lock()
	status, ok = ingestChainCount[mapKey]
	if !ok {
		return fmt.Errorf("the chain was removed from the ingest queue while ingesting it. This should never happen")
	}
	if err != nil {
		err = fmt.Errorf("error ingesting chain: %s", err)
	}
	status.Ready = true
	status.Success = err == nil
	status.WaitCount--
	ingestChainCount[mapKey] = status
	ingestChainCv.Broadcast()

	fmt.Printf("Chain %s ingested\n", chain[len(chain)-1].chainDigest)

	return err
}

func IngestChain(cvmfsRepo string, chain Chain) error {
	currentChainStep := chain[len(chain)-1]
	skipThisStep := false
	if currentChainStep.layer.MediaType == "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip" {
		skipThisStep = true
	} else {
		// First, check if the step exists in CVMFS already
		// TODO: Should we check that the repository exists here? Or is it already checked? Or should we just let the ingestion fail?
		_, err := os.Stat(fmt.Sprintf("/cvmfs/%s%s", cvmfsRepo, chainPath(&currentChainStep)))
		if err == nil {
			fmt.Printf("Chain %s already exists in CVMFS\n", currentChainStep.chainDigest)
			return nil
		} else if !errors.Is(err, os.ErrNotExist) {
			// In case of any other error than the file not existing, return it
			return fmt.Errorf("error checking if chain is in CVMFS: %s", err)
		}
	}

	previousDigest := ""
	if len(chain) > 1 {
		// We need to first ingest the previous step of the chain
		previousDigest = chain[len(chain)-2].chainDigest
		err := RequestIngestedChain(cvmfsRepo, chain[:len(chain)-1])
		fmt.Println("Ingested previous chain step")
		if err != nil {
			return fmt.Errorf("error requesting previous chain step: %s", err)
		}
	} else {
		fmt.Println("Ingesting base layer")
	}

	if skipThisStep {
		fmt.Println("Skipping this step, as it is a foreign layer")
		return nil
	}

	// We need the tar for the layer
	tarLease, err := RequestLayerTar(currentChainStep.layer)
	if err != nil {
		return fmt.Errorf("error requesting layer tar: %s", err)
	}
	defer tarLease.Release()

	fileReader, err := os.Open(tarLease.Path)
	if err != nil {
		return fmt.Errorf("error opening layer tar: %s", err)
	}
	readHashCloseSizer := lib.NewReadAndHash(fileReader)
	tarReader := *tar.NewReader(readHashCloseSizer)

	fmt.Println("Creating chain")
	err = cvmfs.CreateSneakyChain(cvmfsRepo, currentChainStep.chainDigest, previousDigest, tarReader)
	if err != nil {
		return fmt.Errorf("error ingesting chain: %s", err)
	}
	fmt.Println("Chain created")
	return nil
}

func chainPath(step *ChainStep) string {
	// If the digest begins with an algorithm and : (e.g. sha256:), remove them
	if strings.Contains(step.chainDigest, ":") {
		parts := strings.Split(step.chainDigest, ":")
		step.chainDigest = parts[1]
	}
	out := fmt.Sprintf("/.chains/%s/%s", step.chainDigest[0:2], step.chainDigest)
	return out
}

func createChain(image lib.Tag) (Chain, error) {
	manifest, err := image.FetchManifest()
	if err != nil {
		fmt.Printf("Error fetching manifest: %s\n", err)
		return nil, fmt.Errorf("error creating chain. Could not fetch manifest: %s", err)
	}
	chainIDs := manifest.GetChainIDs()
	chain := make(Chain, len(manifest.Layers))

	// Create the layer objects
	for i, layer := range manifest.Layers {
		chain[i] = ChainStep{
			layer:       lib.Layer{Tag: &image, MediaType: layer.MediaType, Size: layer.Size, Digest: layer.Digest},
			chainDigest: chainIDs[i].String(),
		}
	}
	return chain, nil
}

func IngestChainForTag(cvmfsRepo string, image lib.Tag) error {
	chain, err := createChain(image)
	if err != nil {
		return fmt.Errorf("error ingesting chain: %s", err)
	}

	return RequestIngestedChain(cvmfsRepo, chain)
}
