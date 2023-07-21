package concurrency

import (
	"fmt"
	"sync"
	"time"

	"github.com/cvmfs/ducc/lib"
)

type UpdateImage struct {
	In        RefCountedChan[InformationPacket[lib.Tag]]
	Out       RefCountedChan[InformationPacket[interface{}]]
	cvmfsRepo string
}

func NewUpdateImage(cvmfsRepo string) UpdateImage {
	return UpdateImage{
		In:        NewRefCountedChan[InformationPacket[lib.Tag]](),
		Out:       NewRefCountedChan[InformationPacket[interface{}]](),
		cvmfsRepo: cvmfsRepo,
	}
}

func (component UpdateImage) Process() {
	tempdir := "/tmp"

	// Fetch the current remote manifest
	fetchManifest := NewFetchManifest()
	Connect(component.In, fetchManifest.In)
	imageInCvmfs := NewImageInCvmfs(component.cvmfsRepo)
	Connect(fetchManifest.Out, imageInCvmfs.In)

	// Download the layers that are not in CVMFS
	downloadLayers := NewDownloadLayers(tempdir)
	Connect(imageInCvmfs.OutNotInCvmfs, downloadLayers.In)

	// Perform additional operations on the images, both the ones in CVMFS and the ones that were downloaded
	broadcastToAdditionalOperations := NewBroadcast[lib.Tag](1)
	Connect(imageInCvmfs.OutInCvmfs, broadcastToAdditionalOperations.In)
	Connect(downloadLayers.Out, broadcastToAdditionalOperations.In)

	createLayer := NewCreateLayers()
	ConnectWithTypeAssert(broadcastToAdditionalOperations.Out[0], createLayer.In)

	ConnectWithTypeAssert(createLayer.Out, component.Out)

	wg := sync.WaitGroup{}

	wg.Add(5)
	go func() { fetchManifest.Process(); wg.Done() }()
	go func() { imageInCvmfs.Process(); wg.Done() }()
	go func() { downloadLayers.Process(); wg.Done() }()
	go func() { broadcastToAdditionalOperations.Process(); wg.Done() }()
	go func() { createLayer.Process(); wg.Done() }()

	wg.Wait()
	fmt.Printf("UpdateImage shutting down\n")
}

type DownloadLayers struct {
	In      RefCountedChan[InformationPacket[lib.Tag]]
	Out     RefCountedChan[InformationPacket[lib.Tag]]
	tempdir string
}

func NewDownloadLayers(tempdir string) DownloadLayers {
	return DownloadLayers{
		In:      NewRefCountedChan[InformationPacket[lib.Tag]](),
		Out:     NewRefCountedChan[InformationPacket[lib.Tag]](),
		tempdir: tempdir,
	}
}

func (d DownloadLayers) Process() {
	broadcastImage := NewBroadcast[lib.Tag](2) // out[1] will be sent to the next component when all layers are downloaded
	Connect(d.In, broadcastImage.In)

	scatterIntoLayers := NewScatterIntoLayers(1)
	Connect(broadcastImage.Out[0], scatterIntoLayers.In)

	alreadyInCvmfs := NewLayerAlreadyInCvmfs()
	ConnectWithTypeAssert(scatterIntoLayers.Out[0], alreadyInCvmfs.In)

	downloadLayer := NewDownloadLayer(d.tempdir)
	Connect(alreadyInCvmfs.OutNotInCvmfs, downloadLayer.In)

	convertLayer := NewConvertLayer(d.tempdir)
	Connect(downloadLayer.OutLayer, convertLayer.In)

	gatherLayers := NewGather[lib.Layer]()
	ConnectWithTypeAssert(convertLayer.Out, gatherLayers.In)

	syncImageAndLayers := NewSync[any](2)
	ConnectWithTypeAssert(broadcastImage.Out[1], syncImageAndLayers.In[0])
	ConnectWithTypeAssert(gatherLayers.Out, syncImageAndLayers.In[1])

	layerSink := NewSink[[]lib.Layer]()
	ConnectWithTypeAssert(syncImageAndLayers.Out[1], layerSink.In) // We don't care about the individual layers anymore

	ConnectWithTypeAssert(syncImageAndLayers.Out[0], d.Out)

	wg := sync.WaitGroup{}
	wg.Add(8)

	go func() { broadcastImage.Process(); wg.Done() }()
	go func() { scatterIntoLayers.Process(); wg.Done() }()
	go func() { alreadyInCvmfs.Process(); wg.Done() }()
	go func() { downloadLayer.Process(); wg.Done() }()
	go func() { convertLayer.Process(); wg.Done() }()
	go func() { gatherLayers.Process(); wg.Done() }()
	go func() { syncImageAndLayers.Process(); wg.Done() }()
	go func() { layerSink.Process(); wg.Done() }()

	wg.Wait()
}

type FetchManifest struct {
	In  RefCountedChan[InformationPacket[lib.Tag]]
	Out RefCountedChan[InformationPacket[lib.Tag]]
}

func NewFetchManifest() FetchManifest {
	return FetchManifest{
		In:  NewRefCountedChan[InformationPacket[lib.Tag]](),
		Out: NewRefCountedChan[InformationPacket[lib.Tag]](),
	}
}

func (f FetchManifest) Process() {
	defer f.Out.Close()
	for input := range f.In.ch {
		handle := input.Handle.NewChildHandle("Fetch Manifest")
		logger := handle.Log.GetLogger()

		tag := input.Value

		logger.Printf("[INFO] Attempting to fetch manifest for image %s\n", tag.GetSimpleName())
		manifest, err := tag.FetchManifest()
		if err != nil {
			logger.Printf("[ERROR] Failed to fetch manifest for image %s: %s\n", tag.GetSimpleName(), err)
			continue
		}
		logger.Printf("[INFO] Successfully fetched manifest for image %s\n", tag.GetSimpleName())
		fmt.Printf("Manifest: %+v\n", manifest)
		f.Out.ch <- input
	}
}

type LayerAlreadyInCvmfs struct {
	In            RefCountedChan[InformationPacket[lib.Layer]]
	OutNotInCvmfs RefCountedChan[InformationPacket[lib.Layer]]
	OutInCvmfs    RefCountedChan[InformationPacket[lib.Layer]]
	cvmfsRepo     string
}

func NewLayerAlreadyInCvmfs() LayerAlreadyInCvmfs {
	return LayerAlreadyInCvmfs{
		In:            NewRefCountedChan[InformationPacket[lib.Layer]](),
		OutNotInCvmfs: NewRefCountedChan[InformationPacket[lib.Layer]](),
		OutInCvmfs:    NewRefCountedChan[InformationPacket[lib.Layer]](),
	}
}

func (a LayerAlreadyInCvmfs) Process() {
	defer a.OutNotInCvmfs.Close()
	defer a.OutInCvmfs.Close()
	for input := range a.In.ch {
		handle := input.Handle.NewChildHandle("Check if layer is in CVMFS")
		logger := handle.Log.GetLogger()
		logger.Printf("[INFO] Layer %s is not present in CVMFS\n", input.Value.Digest)
		// TODO: Check if the layer is already in CVMFS
		a.OutNotInCvmfs.ch <- input
	}
}

type ScatterIntoLayers struct {
	In  RefCountedChan[InformationPacket[lib.Tag]]
	Out []RefCountedChan[InformationPacket[lib.Layer]]
}

func NewScatterIntoLayers(numOutputs int) ScatterIntoLayers {
	out := make([]RefCountedChan[InformationPacket[lib.Layer]], numOutputs)
	for i := range out {
		out[i] = NewRefCountedChan[InformationPacket[lib.Layer]]()
	}
	return ScatterIntoLayers{
		In:  NewRefCountedChan[InformationPacket[lib.Tag]](),
		Out: out,
	}
}

func (s ScatterIntoLayers) Process() {
	toSend := make(chan InformationPacket[lib.Layer])

	wg := sync.WaitGroup{}

	for _, outChan := range s.Out {
		wg.Add(1)
		go func(outChan *RefCountedChan[InformationPacket[lib.Layer]]) {
			defer outChan.Close()
			for input := range toSend {
				outChan.ch <- input
			}
			wg.Done()
		}(&outChan)
	}

	for input := range s.In.ch {
		manifest := input.Value.Manifest
		handle := input.Handle.NewChildHandle("Process layers individually")
		logger := handle.Log.GetLogger()
		logger.Printf("[INFO] Proccessing download and conversion individually for the %d layer(s) of image %s\n", len(manifest.Layers), input.Value.GetSimpleName())
		siblingsRemaining := sync.WaitGroup{}
		for i, layer := range manifest.Layers {
			subHandle := handle.NewChildHandle(fmt.Sprintf("Layer %d/%d", i+1, len(manifest.Layers)))
			subHandle.name = fmt.Sprintf("Layer %d/%d", i+1, len(manifest.Layers))
			subHandle.seqNum = int64(i)
			subHandle.siblingsRemaining = &siblingsRemaining
			siblingsRemaining.Add(1)
			subHandle.Log.GetLogger().Printf("[INFO] Processing layer %s\n", layer.Digest)

			toSend <- InformationPacket[lib.Layer]{
				Handle: subHandle,
				Value:  lib.Layer{Digest: layer.Digest, Size: layer.Size},
			}

		}
	}
	close(toSend)
	wg.Wait()
}

type DownloadLayer struct {
	In           RefCountedChan[InformationPacket[lib.Layer]]
	OutLayer     RefCountedChan[InformationPacket[lib.Layer]]
	OutTempFiles RefCountedChan[InformationPacket[[]string]]
	tempdir      string
}

func NewDownloadLayer(tempdir string) DownloadLayer {
	return DownloadLayer{
		In:           NewRefCountedChan[InformationPacket[lib.Layer]](),
		OutLayer:     NewRefCountedChan[InformationPacket[lib.Layer]](),
		OutTempFiles: NewRefCountedChan[InformationPacket[[]string]](),
		tempdir:      tempdir,
	}
}

func (d DownloadLayer) Process() {
	defer d.OutLayer.Close()
	defer d.OutTempFiles.Close()
	// TODO: Download the layer to a temporary directory
	for input := range d.In.ch {
		handle := input.Handle.NewChildHandle("Download Layer")
		logger := handle.Log.GetLogger()

		//token := registry.GetToken()

		logger.Printf("[INFO] Downloading layer %s\n", input.Value.Digest)
		time.Sleep(10 * time.Millisecond)
		logger.Printf("[INFO] Successfully downloaded layer %s\n", input.Value.Digest)
		d.OutLayer.ch <- input
	}
	// Append the path to the downloaded layer to the output
	// This is for later cleanup purposes
}

type ConvertLayer struct {
	In      RefCountedChan[InformationPacket[lib.Layer]]
	Out     RefCountedChan[InformationPacket[lib.Layer]]
	tempdir string
}

func NewConvertLayer(tempdir string) ConvertLayer {
	return ConvertLayer{
		In:      NewRefCountedChan[InformationPacket[lib.Layer]](),
		Out:     NewRefCountedChan[InformationPacket[lib.Layer]](),
		tempdir: tempdir,
	}
}

func (c ConvertLayer) Process() {
	defer c.Out.Close()

	// TODO: Conver the layer to cvmfs format

	for input := range c.In.ch {
		handle := input.Handle.NewChildHandle("Convert Layer")
		logger := handle.Log.GetLogger()
		logger.Printf("[INFO] Converting layer %s\n", input.Value.Digest)
		time.Sleep(10 * time.Millisecond)
		logger.Printf("[INFO] Successfully converted layer %s\n", input.Value.Digest)
		c.Out.ch <- input
	}
}

type CreateLayers struct {
	In  RefCountedChan[InformationPacket[lib.Tag]]
	Out RefCountedChan[InformationPacket[any]]
}

func NewCreateLayers() CreateLayers {
	return CreateLayers{
		In:  NewRefCountedChan[InformationPacket[lib.Tag]](),
		Out: NewRefCountedChan[InformationPacket[any]](),
	}
}

func (c CreateLayers) Process() {
	defer c.Out.Close()
	for input := range c.In.ch {
		handle := input.Handle.NewChildHandle("Create Layers")
		logger := handle.Log.GetLogger()
		logger.Printf("[INFO] Creating layers for %s\n", input.Value.GetSimpleName())
		logger.Printf("[INFO] Successfully created layers for %s\n", input.Value.GetSimpleName())

		c.Out.ch <- InformationPacket[any]{
			Handle: input.Handle,
			Value:  any(input.Value),
		}
	}
}

type ImageInCvmfs struct {
	In            RefCountedChan[InformationPacket[lib.Tag]]
	OutInCvmfs    RefCountedChan[InformationPacket[lib.Tag]]
	OutNotInCvmfs RefCountedChan[InformationPacket[lib.Tag]]
	cvmfsRepo     string
}

func NewImageInCvmfs(cvmfsRepo string) ImageInCvmfs {
	return ImageInCvmfs{
		In:            NewRefCountedChan[InformationPacket[lib.Tag]](),
		OutInCvmfs:    NewRefCountedChan[InformationPacket[lib.Tag]](),
		OutNotInCvmfs: NewRefCountedChan[InformationPacket[lib.Tag]](),
		cvmfsRepo:     cvmfsRepo,
	}
}

func (m ImageInCvmfs) Process() {
	defer m.OutInCvmfs.Close()
	defer m.OutNotInCvmfs.Close()

	for input := range m.In.ch {
		handle := input.Handle.NewChildHandle("Check if image is in CVMFS")
		logger := handle.Log.GetLogger()

		//manifestPath := filepath.Join("/", "cvmfs", m.cvmfsRepo, ".metadata", input.Value.GetSimpleName(), "manifest.json")
		if true {
			logger.Printf("[INFO] Image %s is already in CVMFS\n", input.Value.GetSimpleName())
			m.OutInCvmfs.ch <- input
		} else {
			logger.Printf("[INFO] Image %s is NOT already in CVMFS\n", input.Value.GetSimpleName())
			m.OutNotInCvmfs.ch <- input
		}
	}
	fmt.Println("ImageInCvmfs done")
}
