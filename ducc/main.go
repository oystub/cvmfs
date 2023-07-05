package main

import (
	"context"
	"fmt"
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

	testImage := lib.Image{
		Scheme:     "https",
		Registry:   "gitlab-registry.cern.ch",
		Repository: "atlas/athena/athanalysis",
		Tag:        "24.2.0",
	}

	logger := concurrency.NewLoggerWithBuffer()
	ctx, _ := context.WithCancel(context.WithValue(context.Background(), "logger", &logger))

	pipeline := concurrency.NewUpdateImage("local.test.repo")

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		pipeline.Process()
		wg.Done()
	}()
	start := concurrency.NewRefCountedChan[concurrency.TaggedValueWithCtx[lib.Image]]()
	concurrency.Connect(start, pipeline.In)
	start.Chan() <- concurrency.NewTaggedValueWithCtx[lib.Image](testImage, ctx)
	start.Close()
	//daemon.Main()

	//cmd.EntryPoint()
	wg.Wait()

	fmt.Print(logger.GetText())
}

/*Id          int
User        string
Scheme      string
Registry    string
Repository  string
Tag         string
Digest      string
IsThin      bool
TagWildcard bool
Manifest    *da.Manifest
OCIImage    *image.Image
*/
