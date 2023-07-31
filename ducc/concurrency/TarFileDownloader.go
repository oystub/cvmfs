package concurrency

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"

	"github.com/cvmfs/ducc/lib"
	"github.com/opencontainers/go-digest"
)

type TarDownloadJob struct {
	// The compressed digest of the gzipped tar file to download
	CompressedDigest digest.Digest
	// Pointer to the container repository from which to download the tar file
	Repository *lib.ContainerRepository
	// Context for canceling the job when no longer needed
	Context       context.Context
	cancelContext context.CancelFunc
	//
	Done bool
	// Number of times this job has been requested. If it is 0, the job should be cancelled.
	WaitCount int
	// The path to the local tar file
	Lease LayerTarFileLease
	// Condition variable for notifying waiters when the job is done
	statusCv sync.Cond
}

type TarFileDownloader struct {
	// The path to the local tar file cache
	CachePath string

	// Jobs to be processed
	JobsMutex  sync.Mutex
	Jobs       map[digest.Digest]*TarDownloadJob
	queuedJobs []digest.Digest
	newJobs    chan *TarDownloadJob

	// TODO: More than one concurrent download
}

func NewTarFileDownloader(cachePath string) *TarFileDownloader {
	tfd := &TarFileDownloader{
		CachePath: cachePath,
		Jobs:      make(map[digest.Digest]*TarDownloadJob),
		newJobs:   make(chan *TarDownloadJob, 100),
	}
	return tfd
}

// RequestTarFile will request a tar file from the downloader. The function will block until the tar file is ready or the context is cancelled.
func (tfd *TarFileDownloader) RequestTarFile(ctx context.Context, compressedDigest digest.Digest, repository *lib.ContainerRepository) (LayerTarFileLease, error) {
	// Is there already a job for this exact tar file?
	tfd.JobsMutex.Lock()
	job, ok := tfd.Jobs[compressedDigest]
	if ok {
		// There is already a job for this tar file
		job.WaitCount++
		tfd.JobsMutex.Unlock()
	} else {
		// There is no job for this tar file, create a new one
		context, cancel := context.WithCancel(ctx)
		job = &TarDownloadJob{
			CompressedDigest: compressedDigest,
			Repository:       repository,
			Context:          context,
			cancelContext:    cancel,
			Done:             false,
			WaitCount:        1,
			statusCv:         sync.Cond{L: &sync.Mutex{}},
		}
		tfd.Jobs[compressedDigest] = job
		tfd.JobsMutex.Unlock()
		tfd.newJobs <- job // Notify the downloader thread that there is a new job
	}

	done := make(chan bool)
	go func() {
		job.statusCv.L.Lock()
		for !job.Done {
			job.statusCv.Wait()
		}
		job.statusCv.L.Unlock()
		done <- true
	}()

	select {
	case <-ctx.Done():
		// The context was cancelled, we should decrement the wait count
		tfd.JobsMutex.Lock()
		job.statusCv.L.Lock()
		job.WaitCount--
		if job.WaitCount == 0 {
			// No more waiters, cancel the job
			job.Done = true
			job.cancelContext()
			delete(tfd.Jobs, compressedDigest)
		}
		job.statusCv.L.Unlock()
		tfd.JobsMutex.Unlock()
		return LayerTarFileLease{}, fmt.Errorf("context was cancelled")
	case <-done:
		// The job is done, return the lease
		return job.Lease, nil
	}
}

// Work will start processing jobs. This function will block until the context is cancelled.
// Any remaining jobs will be cancelled when the context is cancelled.
func (tfd *TarFileDownloader) Work(ctx context.Context) {
	// TODO: More than one concurrent download
	// Create the local tar file cache directory if it doesn't exist
	err := os.MkdirAll(tfd.CachePath, 0755)
	if err != nil {
		// No point in continuing if we can't create the directory. Just crash the program.
		panic(fmt.Errorf("error in creating directory for layer tar file storage: %s", err))
	}
	for {
		tfd.JobsMutex.Lock()
		var job *TarDownloadJob
		if len(tfd.queuedJobs) > 0 {
			// Start processing the first job in the queue
			job = tfd.Jobs[tfd.queuedJobs[0]]
			tfd.queuedJobs = tfd.queuedJobs[1:]
			tfd.JobsMutex.Unlock()
		} else {
			tfd.JobsMutex.Unlock()
			// Wait for a new job
			select {
			case <-ctx.Done():
				// TODO: Context was cancelled, cancel all remaining jobs
				return
			case job = <-tfd.newJobs:
				// Got a new job
			}
		}
		path, uncompressedDigest, err := fetchLayerTar(job.Context, job.Repository, job.CompressedDigest, tfd.CachePath)
		if err != nil {
			fmt.Printf("error in fetching layer tar: %s\n", err)
		} else {
			job.Lease = LayerTarFileLease{
				Path:               path,
				CompressedDigest:   job.CompressedDigest,
				UncompressedDigest: digest.NewDigestFromEncoded(digest.SHA256, uncompressedDigest),
				useCount:           &job.WaitCount,
				useCountMutex:      &sync.Mutex{},
			}
			fmt.Printf("Successfully downloaded layer tar file %s\n", job.CompressedDigest)
		}
		tfd.JobsMutex.Lock()
		job.statusCv.L.Lock()
		job.Done = true
		job.statusCv.Broadcast()
		delete(tfd.Jobs, job.CompressedDigest)
		job.statusCv.L.Unlock()
		tfd.JobsMutex.Unlock()

		continue
	}

}

type LayerTarFileLease struct {
	Path               string
	CompressedDigest   digest.Digest
	UncompressedDigest digest.Digest
	useCount           *int
	useCountMutex      *sync.Mutex
}

func (l LayerTarFileLease) Release() {
	l.useCountMutex.Lock()
	defer l.useCountMutex.Unlock()
	// No more leases, delete the file and remove it from the cache
	err := os.Remove(l.Path)
	*l.useCount--
	if *l.useCount <= 0 {
		if err != nil {
			// Nothing to do here, we just have to live with the file being there
			fmt.Printf("error in removing file: %s\n", err)
		}
	}
}

func fetchLayerTar(ctx context.Context, repository *lib.ContainerRepository, compressedDigest digest.Digest, tempdir string) (path string, uncompressedDigest string, errorOut error) {
	url := fmt.Sprintf("%s/blobs/%s", repository.BaseUrl(), compressedDigest.String())

	fmt.Printf("Fetching layer tar file from %s\n", url)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
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
