package concurrency

import (
	"fmt"
	"path/filepath"
	"sync"
	"time"

	dockerutil "github.com/cvmfs/ducc/docker-api"
	"github.com/cvmfs/ducc/lib"
	"github.com/cvmfs/ducc/scheduler"
	"github.com/google/uuid"
)

type UpdateImage struct {
	In        RefCountedChan[TaggedValueWithCtx[lib.Image]]
	Out       RefCountedChan[TaggedValueWithCtx[interface{}]]
	cvmfsRepo string
}

func NewUpdateImage(cvmfsRepo string) UpdateImage {
	return UpdateImage{
		In:        NewRefCountedChan[TaggedValueWithCtx[lib.Image]](),
		Out:       NewRefCountedChan[TaggedValueWithCtx[interface{}]](),
		cvmfsRepo: cvmfsRepo,
	}
}

func (component UpdateImage) Process() {
	tempdir := "/tmp"

	////// Fetch the current remote manifest
	fetchManifest := NewFetchManifest()
	Connect(component.In, fetchManifest.In)
	imageInCvmfs := NewImageInCvmfs(component.cvmfsRepo)
	Connect(fetchManifest.Out, imageInCvmfs.In)

	////// Download the layers that are not in CVMFS
	downloadLayers := NewDownloadLayers(tempdir)
	Connect(imageInCvmfs.OutNotInCvmfs, downloadLayers.In)

	////// Perform additional operations on the images, both the ones in CVMFS and the ones that were downloaded
	broadcastToAdditionalOperations := NewBroadcast[lib.Image](1)
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
	In      RefCountedChan[TaggedValueWithCtx[lib.Image]]
	Out     RefCountedChan[TaggedValueWithCtx[lib.Image]]
	tempdir string
}

func NewDownloadLayers(tempdir string) DownloadLayers {
	return DownloadLayers{
		In:      NewRefCountedChan[TaggedValueWithCtx[lib.Image]](),
		Out:     NewRefCountedChan[TaggedValueWithCtx[lib.Image]](),
		tempdir: tempdir,
	}
}

func (d DownloadLayers) Process() {
	broadcastImage := NewBroadcast[lib.Image](2) // out[1] will be sent to the next component when all layers are downloaded
	Connect(d.In, broadcastImage.In)

	scatterIntoLayers := NewScatterIntoLayers(1)
	Connect(broadcastImage.Out[0], scatterIntoLayers.In)

	alreadyInCvmfs := NewLayerAlreadyInCvmfs()
	ConnectWithTypeAssert(scatterIntoLayers.Out[0], alreadyInCvmfs.In)

	downloadLayer := NewDownloadLayer(d.tempdir)
	Connect(alreadyInCvmfs.OutNotInCvmfs, downloadLayer.In)

	convertLayer := NewConvertLayer(d.tempdir)
	Connect(downloadLayer.OutLayer, convertLayer.In)

	gatherLayers := NewGather[dockerutil.Layer]()
	ConnectWithTypeAssert(convertLayer.Out, gatherLayers.In)

	syncImageAndLayers := NewSync[any](2)
	ConnectWithTypeAssert(broadcastImage.Out[1], syncImageAndLayers.In[0])
	ConnectWithTypeAssert(gatherLayers.Out, syncImageAndLayers.In[1])

	layerSink := NewSink[[]dockerutil.Layer]()
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
	In  RefCountedChan[TaggedValueWithCtx[lib.Image]]
	Out RefCountedChan[TaggedValueWithCtx[lib.Image]]
}

func NewFetchManifest() FetchManifest {
	return FetchManifest{
		In:  NewRefCountedChan[TaggedValueWithCtx[lib.Image]](),
		Out: NewRefCountedChan[TaggedValueWithCtx[lib.Image]](),
	}
}

func (f FetchManifest) Process() {
	defer f.Out.Close()
	for input := range f.In.ch {
		logger := input.Ctx.Value("logger").(*LoggerWithBuffer).GetLogger()

		img := input.Value

		logger.Printf("[INFO] Attempting to fetch manifest for image %s\n", img.GetSimpleName())
		manifest, err := img.GetManifest()
		if err != nil {
			logger.Printf("[ERROR] Failed to fetch manifest for image %s\n", img.GetSimpleName())
			continue
		}
		logger.Printf("[INFO] Successfully fetched manifest for image %s\n", img.GetSimpleName())

		input.Value.Manifest = &manifest
		f.Out.ch <- input
	}
}

type LayerAlreadyInCvmfs struct {
	In            RefCountedChan[TaggedValueWithCtx[dockerutil.Layer]]
	OutNotInCvmfs RefCountedChan[TaggedValueWithCtx[dockerutil.Layer]]
	OutInCvmfs    RefCountedChan[TaggedValueWithCtx[dockerutil.Layer]]
	cvmfsRepo     string
}

func NewLayerAlreadyInCvmfs() LayerAlreadyInCvmfs {
	return LayerAlreadyInCvmfs{
		In:            NewRefCountedChan[TaggedValueWithCtx[dockerutil.Layer]](),
		OutNotInCvmfs: NewRefCountedChan[TaggedValueWithCtx[dockerutil.Layer]](),
		OutInCvmfs:    NewRefCountedChan[TaggedValueWithCtx[dockerutil.Layer]](),
	}
}

func (a LayerAlreadyInCvmfs) Process() {
	defer a.OutNotInCvmfs.Close()
	defer a.OutInCvmfs.Close()
	for input := range a.In.ch {
		logger := input.Ctx.Value("logger").(*LoggerWithBuffer).GetLogger()

		logger.Printf("[INFO] Layer %s (%d/%d) is not present in CVMFS\n", input.Value.Digest, input.TagStack[0].seqNum, input.TagStack[0].count)
		// TODO: Check if the layer is already in CVMFS
		a.OutNotInCvmfs.ch <- input
	}
}

type ScatterIntoLayers struct {
	In  RefCountedChan[TaggedValueWithCtx[lib.Image]]
	Out []RefCountedChan[TaggedValueWithCtx[dockerutil.Layer]]
}

func NewScatterIntoLayers(numOutputs int) ScatterIntoLayers {
	out := make([]RefCountedChan[TaggedValueWithCtx[dockerutil.Layer]], numOutputs)
	for i := range out {
		out[i] = NewRefCountedChan[TaggedValueWithCtx[dockerutil.Layer]]()
	}
	return ScatterIntoLayers{
		In:  NewRefCountedChan[TaggedValueWithCtx[lib.Image]](),
		Out: out,
	}
}

func (s ScatterIntoLayers) Process() {
	toSend := make(chan TaggedValueWithCtx[dockerutil.Layer])

	wg := sync.WaitGroup{}

	for _, outChan := range s.Out {
		wg.Add(1)
		go func(outChan *RefCountedChan[TaggedValueWithCtx[dockerutil.Layer]]) {
			defer outChan.Close()
			for input := range toSend {
				outChan.ch <- input
			}
			wg.Done()
		}(&outChan)
	}

	for input := range s.In.ch {
		logger := input.Ctx.Value("logger").(*LoggerWithBuffer).GetLogger()
		manifest := input.Value.Manifest
		newTagStack := append([]Tag{{
			id:     uuid.New(),
			seqNum: 0, // To be updated for each layer
			count:  len(manifest.Layers),
		}}, input.TagStack...)

		logger.Printf("[INFO] Proccessing download and conversion individually for the %d layer(s) of image %s\n", len(manifest.Layers), input.Value.GetSimpleName())
		for i, layer := range manifest.Layers {
			copiedTagStack := make([]Tag, len(newTagStack))
			copy(copiedTagStack, newTagStack)
			singleLayer := TaggedValueWithCtx[dockerutil.Layer]{
				TagStack: copiedTagStack,
				Ctx:      input.Ctx,
				Value:    layer,
			}
			//  Update the sqeuence number of the tag
			singleLayer.TagStack[0].seqNum = i
			toSend <- singleLayer

		}
	}
	close(toSend)
	wg.Wait()
}

type DownloadLayer struct {
	In           RefCountedChan[TaggedValueWithCtx[dockerutil.Layer]]
	OutLayer     RefCountedChan[TaggedValueWithCtx[dockerutil.Layer]]
	OutTempFiles RefCountedChan[TaggedValueWithCtx[[]string]]
	tempdir      string
}

func NewDownloadLayer(tempdir string) DownloadLayer {
	return DownloadLayer{
		In:           NewRefCountedChan[TaggedValueWithCtx[dockerutil.Layer]](),
		OutLayer:     NewRefCountedChan[TaggedValueWithCtx[dockerutil.Layer]](),
		OutTempFiles: NewRefCountedChan[TaggedValueWithCtx[[]string]](),
		tempdir:      tempdir,
	}
}

func (d DownloadLayer) Process() {
	defer d.OutLayer.Close()
	defer d.OutTempFiles.Close()
	// TODO: Download the layer to a temporary directory
	for input := range d.In.ch {
		logger := input.Ctx.Value("logger").(*LoggerWithBuffer).GetLogger()
		logger.Printf("[INFO] Downloading layer %s, (%d/%d)\n", input.Value.Digest, input.TagStack[0].seqNum, input.TagStack[0].count)
		time.Sleep(10 * time.Millisecond)
		logger.Printf("[INFO] Successfully downloaded layer %s, (%d/%d)\n", input.Value.Digest, input.TagStack[0].seqNum, input.TagStack[0].count)
		d.OutLayer.ch <- input
	}
	// Append the path to the downloaded layer to the output
	// This is for later cleanup purposes
}

type ConvertLayer struct {
	In      RefCountedChan[TaggedValueWithCtx[dockerutil.Layer]]
	Out     RefCountedChan[TaggedValueWithCtx[dockerutil.Layer]]
	tempdir string
}

func NewConvertLayer(tempdir string) ConvertLayer {
	return ConvertLayer{
		In:      NewRefCountedChan[TaggedValueWithCtx[dockerutil.Layer]](),
		Out:     NewRefCountedChan[TaggedValueWithCtx[dockerutil.Layer]](),
		tempdir: tempdir,
	}
}

func (c ConvertLayer) Process() {
	defer c.Out.Close()

	// TODO: Conver the layer to cvmfs format

	for input := range c.In.ch {
		logger := input.Ctx.Value("logger").(*LoggerWithBuffer).GetLogger()
		logger.Printf("[INFO] Converting layer %s (%d/%d)\n", input.Value.Digest, input.TagStack[0].seqNum, input.TagStack[0].count)
		time.Sleep(10 * time.Millisecond)
		logger.Printf("[INFO] Successfully converted layer %s (%d/%d)\n", input.Value.Digest, input.TagStack[0].seqNum, input.TagStack[0].count)
		c.Out.ch <- input
	}
}

type CreateLayers struct {
	In  RefCountedChan[TaggedValueWithCtx[lib.Image]]
	Out RefCountedChan[TaggedValueWithCtx[any]]
}

func NewCreateLayers() CreateLayers {
	return CreateLayers{
		In:  NewRefCountedChan[TaggedValueWithCtx[lib.Image]](),
		Out: NewRefCountedChan[TaggedValueWithCtx[any]](),
	}
}

func (c CreateLayers) Process() {
	defer c.Out.Close()
	for input := range c.In.ch {
		logger := input.Ctx.Value("logger").(*LoggerWithBuffer).GetLogger()
		logger.Printf("[INFO] Creating layers for %s\n", input.Value.GetSimpleName())
		// Copy the tag stack from the input
		newTagStack := make([]Tag, len(input.TagStack))
		copy(newTagStack, input.TagStack)
		logger.Printf("[INFO] Successfully created layers for %s\n", input.Value.GetSimpleName())
		c.Out.ch <- TaggedValueWithCtx[any]{
			TagStack: newTagStack,
			Ctx:      input.Ctx,
			Value:    1,
		}
	}
}

type AcquireResource[T any] struct {
	InValue RefCountedChan[TaggedValueWithCtx[T]]
	InRes   RefCountedChan[TaggedValueWithCtx[*scheduler.Resource]]
	OutVal  RefCountedChan[TaggedValueWithCtx[T]]
	OutRes  RefCountedChan[TaggedValueWithCtx[*scheduler.Resource]]
}

func NewAcquireResource[T any]() AcquireResource[T] {
	return AcquireResource[T]{
		InValue: NewRefCountedChan[TaggedValueWithCtx[T]](),
		InRes:   NewRefCountedChan[TaggedValueWithCtx[*scheduler.Resource]](),
		OutVal:  NewRefCountedChan[TaggedValueWithCtx[T]](),
		OutRes:  NewRefCountedChan[TaggedValueWithCtx[*scheduler.Resource]](),
	}
}

type ImageInCvmfs struct {
	In            RefCountedChan[TaggedValueWithCtx[lib.Image]]
	OutInCvmfs    RefCountedChan[TaggedValueWithCtx[lib.Image]]
	OutNotInCvmfs RefCountedChan[TaggedValueWithCtx[lib.Image]]
	cvmfsRepo     string
}

func NewImageInCvmfs(cvmfsRepo string) ImageInCvmfs {
	return ImageInCvmfs{
		In:            NewRefCountedChan[TaggedValueWithCtx[lib.Image]](),
		OutInCvmfs:    NewRefCountedChan[TaggedValueWithCtx[lib.Image]](),
		OutNotInCvmfs: NewRefCountedChan[TaggedValueWithCtx[lib.Image]](),
		cvmfsRepo:     cvmfsRepo,
	}
}

func (m ImageInCvmfs) Process() {
	defer m.OutInCvmfs.Close()
	defer m.OutNotInCvmfs.Close()

	for input := range m.In.ch {
		logger := input.Ctx.Value("logger").(*LoggerWithBuffer).GetLogger()

		manifestPath := filepath.Join("/", "cvmfs", m.cvmfsRepo, ".metadata", input.Value.GetSimpleName(), "manifest.json")
		if lib.AlreadyConverted(manifestPath, input.Value.Manifest.Config.Digest) == lib.ConversionMatch {
			logger.Printf("[INFO] Image %s is already in CVMFS\n", input.Value.GetSimpleName())
			m.OutInCvmfs.ch <- input
		} else {
			logger.Printf("[INFO] Image %s is NOT already in CVMFS\n", input.Value.GetSimpleName())
			m.OutNotInCvmfs.ch <- input
		}
	}
	fmt.Println("ImageInCvmfs done")
}
