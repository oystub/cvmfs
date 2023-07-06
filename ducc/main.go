package main

import (
	"bytes"
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

	ctx, _ := context.WithCancel(context.Background())

	pipeline := concurrency.NewUpdateImage("local.test.repo")

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		pipeline.Process()
		wg.Done()
	}()
	start := concurrency.NewRefCountedChan[concurrency.InformationPacket[lib.Image]]()
	concurrency.Connect(start, pipeline.In)
	firstFrame := concurrency.NewInformationPacket[lib.Image](testImage, ctx, "Root")
	firstFrame.Handle.Log.GetLogger().Println("Starting pipeline")
	start.Chan() <- firstFrame
	start.Close()

	wg.Wait()
	buf := bytes.NewBuffer([]byte{})
	firstFrame.Handle.GetAllLogs(buf, 0)
	fmt.Print(buf.String())
}
