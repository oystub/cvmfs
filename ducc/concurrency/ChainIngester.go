package concurrency

import (
	"archive/tar"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/cvmfs/ducc/cvmfs"
	"github.com/cvmfs/ducc/lib"
	"github.com/opencontainers/go-digest"
)

type ChainStep struct {
	layer       lib.Layer
	chainDigest digest.Digest
}

type Chain []ChainStep

type ChainIngestJob struct {
	Status *StatusHandle
	// The repository from which to download the tar file
	ContainerRepository *lib.ContainerRepository
	// The repository to ingest the chain into
	CvmfsRepo string
	// Uncompressed digest of the layer to be ingested
	UncompressedDigest digest.Digest
	// Digest of the chain (calculated by hashing consecutive chain digests)
	ChainDigest       digest.Digest
	ParentChainDigest digest.Digest
	// This step of the chain can require the previous step to be ingested first
	DependsOn *ChainIngestJob

	Context       context.Context
	cancelContext context.CancelFunc
	Done          bool
	WaitCount     int
	StatusCv      sync.Cond
	Result        error
}

type ChainIngester struct {
	// For downloading the tar files
	TarDownloader *TarFileDownloader

	// Jobs to be processed
	JobsMutex  sync.Mutex
	Jobs       map[string]*ChainIngestJob
	queuedJobs []*ChainIngestJob
	newJobs    chan *ChainIngestJob
}

func NewChainIngester(tarDownloader *TarFileDownloader) *ChainIngester {
	ci := &ChainIngester{
		TarDownloader: tarDownloader,
		Jobs:          make(map[string]*ChainIngestJob),
		newJobs:       make(chan *ChainIngestJob, 100),
	}
	return ci
}

func (ci *ChainIngester) RequestIngestedChain(ctx context.Context, parentStatusHandle *StatusHandle, cvmfsRepo string, containerRepository *lib.ContainerRepository, chain Chain) error {
	// Iterate over the chain, requesting ingest of each step
	var prevJob *ChainIngestJob
	for _, step := range chain {
		// TODO: No need to check for foreign layers, as there is no reason to create a flat chain for them

		if step.layer.MediaType == "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip" {
			fmt.Println("Skipping foreign layer")
			continue
		} else {
			fmt.Printf("Requested chain ingestion of layer %s\n", step.layer.Digest)
		}

		var job *ChainIngestJob
		jobKey := cvmfsRepo + step.chainDigest.String()

		// Check if the step is in CVMFS already
		// TODO: Should we check that the repository exists here? Or is it already checked? Or should we just let the ingestion fail?
		_, err := os.Stat(fmt.Sprintf("/cvmfs/%s%s", cvmfsRepo, chainPath(step.layer.Digest)))
		if err == nil {
			fmt.Printf("Chain %s already exists in CVMFS\n", step.chainDigest)
			continue
		} else if !errors.Is(err, os.ErrNotExist) {
			// In case of any other error than the file not existing, return it
			return fmt.Errorf("error checking if chain is in CVMFS: %s", err)
		}

		// Check if the step is already being ingested
		ci.JobsMutex.Lock()
		job, ok := ci.Jobs[jobKey]
		if ok {
			// Step is already being ingested
			job.WaitCount++
			ci.JobsMutex.Unlock()
		} else {
			// Step is not being ingested, create a new job for it

			ctx, cancel := context.WithCancel(ctx)
			parentChainDigest := digest.Digest("")
			if prevJob != nil {
				parentChainDigest = prevJob.ChainDigest
			}
			status := NewStatusHandle(TS_NotStarted)
			parentStatusHandle.addChildHandle(status)
			job = &ChainIngestJob{
				Status:              status,
				ContainerRepository: containerRepository,
				CvmfsRepo:           cvmfsRepo,
				UncompressedDigest:  step.layer.Digest,
				ChainDigest:         step.chainDigest,
				ParentChainDigest:   parentChainDigest,
				DependsOn:           prevJob,
				Context:             ctx,
				cancelContext:       cancel,
				Done:                false,
				WaitCount:           0,
				StatusCv:            sync.Cond{L: &sync.Mutex{}},
			}
			ci.Jobs[jobKey] = job
			ci.JobsMutex.Unlock()
			// Register the prerequisite download job
			go func() {
				lease, err := ci.TarDownloader.RequestTarFile(ctx, step.layer.Digest, step.layer.Tag.Repository)
				if err != nil {
					defer lease.Release()
				}
			}()
			ci.newJobs <- job // Notify the ingester that there is a new job

		}
		prevJob = job
	}

	// Wait for the last step to be ingested
	if prevJob == nil {
		// There is no last step, the entire chain is already in CVMFS
		return nil
	}

	lastStep := prevJob
	lastStepDone := make(chan bool)
	go func() {
		lastStep.StatusCv.L.Lock()
		for !lastStep.Done {
			lastStep.StatusCv.Wait()
		}
		lastStep.StatusCv.L.Unlock()
		lastStepDone <- true
	}()

	select {
	case <-ctx.Done():
		// The context was cancelled, we should decrement the wait count
		ci.JobsMutex.Lock()
		lastStep.StatusCv.L.Lock()
		lastStep.WaitCount--
		if lastStep.WaitCount == 0 {
			// No more waiters, cancel the job
			lastStep.Done = true
			lastStep.cancelContext()
			delete(ci.Jobs, lastStep.ChainDigest.String())
		}
		lastStep.StatusCv.L.Unlock()
		ci.JobsMutex.Unlock()
		return fmt.Errorf("context was cancelled")
	case <-lastStepDone:
		// The job is done, return the result
		return lastStep.Result
	}
}

func (ci *ChainIngester) Work(ctx context.Context) {
	workerReady := make(chan bool)

	go func() {
		workerReady <- true
	}()

	for {
		select {
		case <-ctx.Done():
			// Worker context was cancelled, stop all jobs
			return
		case <-workerReady:
			// Worker is ready
			// TODO: Reorganize the queue in an efficient way

			if len(ci.queuedJobs) == 0 {
				// Our worker is ready, but there are no jobs in the queue
				// We need either a new job or a cancellation
				select {
				case <-ctx.Done():
					// Worker context was cancelled, stop all jobs
					return
				case job := <-ci.newJobs:
					// TODO: Scedule jobs to account for dependencies
					ci.queuedJobs = append(ci.queuedJobs, job)
				}
			}
			job := ci.queuedJobs[0]
			ci.queuedJobs = ci.queuedJobs[1:]

			go func() {
				err := ci.ingestChainStep(ctx, job)
				if err != nil {
					job.Status.Logger.GetLogger().Printf("[error] Unable to ingest chain step: %s\n", err)
					job.Status.SetStatus(TS_Failure)
				} else {
					job.Status.Logger.GetLogger().Printf("[info] Chain step ingested successfully\n")
					job.Status.SetStatus(TS_Success)
				}
				ci.JobsMutex.Lock()
				job.StatusCv.L.Lock()
				job.Done = true
				job.Result = err
				job.StatusCv.Broadcast()
				job.StatusCv.L.Unlock()
				ci.JobsMutex.Unlock()
				// TODO: Remove job from map
				workerReady <- true
			}()

		case job := <-ci.newJobs:
			// There is a new job, but the worker is not ready
			// Queue the job
			ci.queuedJobs = append(ci.queuedJobs, job)
		}
	}
}

func (ci *ChainIngester) ingestChainStep(ctx context.Context, job *ChainIngestJob) error {
	// Get the tar file for the layer
	tarLease, err := ci.TarDownloader.RequestTarFile(ctx, job.UncompressedDigest, job.ContainerRepository)
	if err != nil {
		return fmt.Errorf("error requesting layer tar: %s", err)
	}
	defer tarLease.Release()

	// Open the tar file
	fileReader, err := os.Open(tarLease.Path)
	if err != nil {
		return fmt.Errorf("error opening layer tar: %s", err)
	}
	readHashCloseSizer := lib.NewReadAndHash(fileReader)
	tarReader := *tar.NewReader(readHashCloseSizer)

	previousChainId := ""
	if job.DependsOn != nil {
		previousChainId = job.ParentChainDigest.Encoded()
	}

	err = cvmfs.CreateSneakyChain(job.CvmfsRepo, job.ChainDigest.Encoded(), previousChainId, tarReader)

	if err != nil {
		return fmt.Errorf("error ingesting chain: %s", err)
	}
	fmt.Println("Chain created")
	return nil
}

func chainPath(chainDigest digest.Digest) string {
	// If the digest begins with an algorithm and : (e.g. sha256:), remove them
	return fmt.Sprintf("/.chains/%s/%s", chainDigest.Encoded()[0:2], chainDigest.Encoded())
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
		splitDigest := strings.Split(layer.Digest, ":")
		chain[i] = ChainStep{
			layer:       lib.Layer{Tag: &image, MediaType: layer.MediaType, Size: layer.Size, Digest: digest.NewDigestFromEncoded(digest.Algorithm(splitDigest[0]), splitDigest[1])},
			chainDigest: chainIDs[i],
		}
	}
	return chain, nil
}

func (ci *ChainIngester) IngestChainForTag(ctx context.Context, parentStatusHandle *StatusHandle, cvmfsRepo string, image lib.Tag) error {
	status := NewStatusHandle(TS_NotStarted)
	status.Logger.GetLogger().Printf("Ingesting chain for tag %s\n", image.Name)
	parentStatusHandle.addChildHandle(status)

	chain, err := createChain(image)
	if err != nil {
		status.Logger.GetLogger().Printf("[error] Unable to create chain: %s\n", err)
		return fmt.Errorf("error creating chain: %s", err)
	}
	fmt.Printf("Length of chain: %d\n", len(chain))
	err = ci.RequestIngestedChain(ctx, status, cvmfsRepo, image.Repository, chain)
	if err != nil {
		status.Logger.GetLogger().Printf("[error] Unable to ingest chain: %s\n", err)
		status.SetStatus(TS_Failure)
	}

	return err
}
