package main

import (
	"context"
	"fmt"
	"net/http"
	"sync"

	"github.com/cvmfs/ducc/concurrency"
	"github.com/cvmfs/ducc/lib"
	"github.com/cvmfs/ducc/rest"
)

func main() {

	fmt.Println("Initializing DUCC")
	lib.SetupNotification()
	lib.SetupRegistries()

	fmt.Println("Starting REST API")
	rest.Init()
	go rest.RunRawRestApi()

	testRegistry := lib.ContainerRegistry{

		Identifier: lib.ContainerRegistryIdentifier{
			Scheme:   "https",
			Hostname: "registry.cern.ch",
		},
		Credentials: lib.ContainerRegistryCredentials{
			Username: "",
			Password: "",
		},
		TokenCv: sync.NewCond(&sync.Mutex{}),
		Client:  &http.Client{},
	}

	testRepository := lib.ContainerRepository{
		Registry: &testRegistry,
		Name:     "docker.io/cmssw/cms",
	}

	testTag := lib.Tag{
		Repository: &testRepository,
		Name:       "sha256:6f8923d5bc650882e8a43edc38df4d1566be652b2e92e90dfb26404792770035",
	}

	wg := sync.WaitGroup{}
	wg.Add(2)

	tfd := concurrency.NewTarFileDownloader("/tmp/ducc/tarfiles")
	tfdCtx, cancelTfd := context.WithCancel(context.Background())
	fmt.Printf("Starting TAR file downloader\n")
	go func() {
		tfd.Work(tfdCtx)
		fmt.Println("TFD done")
		wg.Done()
	}()
	ci := concurrency.NewChainIngester(tfd)
	ciCtx, cancelCi := context.WithCancel(context.Background())
	fmt.Printf("Starting chain ingester\n")
	go func() {
		ci.Work(ciCtx)
		fmt.Println("CI done")
		wg.Done()
	}()

	ingestCtx, _ := context.WithCancel(context.Background())

	wg2 := sync.WaitGroup{}
	wg2.Add(1)
	go func() {
		fmt.Println("Starting chain ingester")
		status := concurrency.NewStatusHandle(concurrency.TS_NotStarted)
		ci.IngestChainForTag(ingestCtx, status, "local.test.repo", testTag)
		fmt.Println("Ingest done")
		wg2.Done()
	}()

	wg2.Wait()

	fmt.Println("Cancelling workers")

	cancelTfd()
	cancelCi()

	wg.Wait()

	//concurrency.IngestChainForTag("local.test.repo", testTag)
	fmt.Println("Done")
}
