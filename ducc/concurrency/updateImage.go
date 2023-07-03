package concurrency

import (
	"fmt"
	"path/filepath"
	"sync"

	dockerutil "github.com/cvmfs/ducc/docker-api"
	"github.com/cvmfs/ducc/lib"
	"github.com/cvmfs/ducc/scheduler"
	"github.com/google/uuid"
)

type UpdateImage struct {
	In        chan TaggedValueWithCtx[lib.Image]
	Out       chan TaggedValueWithCtx[interface{}]
	cvmfsRepo string
}

func NewUpdateImage(cvmfsRepo string) UpdateImage {
	return UpdateImage{
		In:        make(chan TaggedValueWithCtx[lib.Image]),
		Out:       make(chan TaggedValueWithCtx[interface{}]),
		cvmfsRepo: cvmfsRepo,
	}
}

func (component UpdateImage) Process() {
	defer close(component.Out)
	tempdir := "/tmp"

	////// Fetch the current remote manifest
	fetchManifest := NewFetchManifest()
	Connect(component.In, fetchManifest.in, "Input->FetchManifest")
	imageInCvmfs := NewImageInCvmfs(component.cvmfsRepo)
	Connect(fetchManifest.out, imageInCvmfs.In, "FetchManifest->ImageInCvmfs")

	////// Download the layers that are not in CVMFS
	downloadLayers := NewDownloadLayers(tempdir)
	Connect(imageInCvmfs.OutNotInCvmfs, downloadLayers.In, "ImageInCvmfs->DownloadLayers")

	////// Perform additional operations on the images, both the ones in CVMFS and the ones that were downloaded
	broadcastToAdditionalOperations := NewBroadcast[lib.Image](1)
	Connect(imageInCvmfs.OutInCvmfs, broadcastToAdditionalOperations.In, "ImageInCvmfs->BroadcastToAdditionalOperations")
	Connect(downloadLayers.Out, broadcastToAdditionalOperations.In, "DownloadLayers->BroadcastToAdditionalOperations")

	createLayer := NewCreateLayers()
	ConnectWithTypeAssert(broadcastToAdditionalOperations.Out[0], createLayer.In)

	wg := sync.WaitGroup{}

	wg.Add(5)
	go func() { fetchManifest.Process(); wg.Done() }()
	go func() { imageInCvmfs.Process(); wg.Done() }()
	go func() { downloadLayers.Process(); wg.Done() }()
	go func() { broadcastToAdditionalOperations.Process(); wg.Done() }()
	go func() { createLayer.Process(); wg.Done() }()

	wg.Wait()
	fmt.Println("UpdateImage done")
}

type DownloadLayers struct {
	In      chan TaggedValueWithCtx[lib.Image]
	Out     chan TaggedValueWithCtx[lib.Image]
	tempdir string
}

func NewDownloadLayers(tempdir string) DownloadLayers {
	return DownloadLayers{
		In:      make(chan TaggedValueWithCtx[lib.Image]),
		Out:     make(chan TaggedValueWithCtx[lib.Image]),
		tempdir: tempdir,
	}
}

func (d DownloadLayers) Process() {
	defer close(d.Out)
	broadcastImage := NewBroadcast[lib.Image](2) // out[1] will be sent to the next component when all layers are downloaded
	Connect(d.In, broadcastImage.In, "Input->BroadcastImage")

	scatterIntoLayers := NewScatterIntoLayers(1)
	Connect(broadcastImage.Out[0], scatterIntoLayers.In, "BroadcastImage->ScatterIntoLayers")

	alreadyInCvmfs := NewLayerAlreadyInCvmfs()
	ConnectWithTypeAssert(scatterIntoLayers.Out[0], alreadyInCvmfs.In)

	downloadLayer := NewDownloadLayer(d.tempdir)
	Connect(alreadyInCvmfs.OutNotInCvmfs, downloadLayer.In, "LayerAlreadyInCvmfs->DownloadLayer")

	convertLayer := NewConvertLayer(d.tempdir)
	Connect(downloadLayer.OutLayer, convertLayer.In, "DownloadLayer->ConvertLayer")

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
	fmt.Println("DownloadLayers done")
}

type FetchManifest struct {
	in  chan TaggedValueWithCtx[lib.Image]
	out chan TaggedValueWithCtx[lib.Image]
}

func NewFetchManifest() FetchManifest {
	return FetchManifest{
		in:  make(chan TaggedValueWithCtx[lib.Image]),
		out: make(chan TaggedValueWithCtx[lib.Image]),
	}
}

func (f FetchManifest) Process() {
	defer close(f.out)
	for input := range f.in {
		img := input.Value

		fmt.Printf("Fetching manifest for %s\n", img.GetSimpleName())

		manifest, _ := img.GetManifest()
		input.Value.Manifest = &manifest

		fmt.Printf("Got manifest for %s: %v\n", img.GetSimpleName(), manifest)

		f.out <- input
	}
	fmt.Println("FetchManifest done")
}

type LayerAlreadyInCvmfs struct {
	In            chan TaggedValueWithCtx[dockerutil.Layer]
	OutNotInCvmfs chan TaggedValueWithCtx[dockerutil.Layer]
	OutInCvmfs    chan TaggedValueWithCtx[dockerutil.Layer]
	cvmfsRepo     string
}

func NewLayerAlreadyInCvmfs() LayerAlreadyInCvmfs {
	return LayerAlreadyInCvmfs{
		In:            make(chan TaggedValueWithCtx[dockerutil.Layer]),
		OutNotInCvmfs: make(chan TaggedValueWithCtx[dockerutil.Layer]),
		OutInCvmfs:    make(chan TaggedValueWithCtx[dockerutil.Layer]),
	}
}

func (a LayerAlreadyInCvmfs) Process() {
	defer close(a.OutNotInCvmfs)
	defer close(a.OutInCvmfs)
	for input := range a.In {
		// TODO: Check if the layer is already in CVMFS
		a.OutNotInCvmfs <- input
	}
}

type ScatterIntoLayers struct {
	In  chan TaggedValueWithCtx[lib.Image]
	Out []chan TaggedValueWithCtx[dockerutil.Layer]
}

func NewScatterIntoLayers(numOutputs int) ScatterIntoLayers {
	out := make([]chan TaggedValueWithCtx[dockerutil.Layer], numOutputs)
	for i := range out {
		out[i] = make(chan TaggedValueWithCtx[dockerutil.Layer])
	}
	return ScatterIntoLayers{
		In:  make(chan TaggedValueWithCtx[lib.Image]),
		Out: out,
	}
}

func (s ScatterIntoLayers) Process() {

	fmt.Println("Begin ScatterIntoLayers")
	toSend := make(chan TaggedValueWithCtx[dockerutil.Layer])
	defer close(toSend)

	for _, outChan := range s.Out {
		go func(outChan chan TaggedValueWithCtx[dockerutil.Layer]) {
			defer close(outChan)
			for input := range toSend {
				outChan <- input
			}
		}(outChan)
	}

	for input := range s.In {
		fmt.Printf("Scattering image %s into layers\n", input.Value.GetSimpleName())
		manifest := input.Value.Manifest
		newTagStack := append([]Tag{{
			id:     uuid.New(),
			seqNum: 0, // To be updated for each layer
			count:  len(manifest.Layers),
		}}, input.TagStack...)

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
}

type DownloadLayer struct {
	In           chan TaggedValueWithCtx[dockerutil.Layer]
	OutLayer     chan TaggedValueWithCtx[dockerutil.Layer]
	OutTempFiles chan TaggedValueWithCtx[[]string]
	tempdir      string
}

func NewDownloadLayer(tempdir string) DownloadLayer {
	return DownloadLayer{
		In:           make(chan TaggedValueWithCtx[dockerutil.Layer]),
		OutLayer:     make(chan TaggedValueWithCtx[dockerutil.Layer]),
		OutTempFiles: make(chan TaggedValueWithCtx[[]string]),
		tempdir:      tempdir,
	}
}

func (d DownloadLayer) Process() {
	// TODO: Download the layer to a temporary directory
	for input := range d.In {
		fmt.Printf("Downloading layer %s, which is fragment (%d/%d)\n", input.Value.Digest, input.TagStack[0].seqNum, input.TagStack[0].count)
		d.OutLayer <- input
	}
	// Append the path to the downloaded layer to the output
	// This is for later cleanup purposes
}

type ConvertLayer struct {
	In      chan TaggedValueWithCtx[dockerutil.Layer]
	Out     chan TaggedValueWithCtx[dockerutil.Layer]
	tempdir string
}

func NewConvertLayer(tempdir string) ConvertLayer {
	return ConvertLayer{
		In:      make(chan TaggedValueWithCtx[dockerutil.Layer]),
		Out:     make(chan TaggedValueWithCtx[dockerutil.Layer]),
		tempdir: tempdir,
	}
}

func (c ConvertLayer) Process() {
	// TODO: Conver the layer to cvmfs format

	for input := range c.In {
		fmt.Printf("Converting layer %s, which is fragment (%d/%d)\n", input.Value.Digest, input.TagStack[0].seqNum, input.TagStack[0].count)
		c.Out <- input
	}
}

type CreateLayers struct {
	In  chan TaggedValueWithCtx[lib.Image]
	Out chan any
}

func NewCreateLayers() CreateLayers {
	return CreateLayers{
		In:  make(chan TaggedValueWithCtx[lib.Image]),
		Out: make(chan any),
	}
}

func (c CreateLayers) Process() {
	defer close(c.Out)
	for input := range c.In {
		fmt.Printf("Creating layers for %s\n", input.Value.GetSimpleName())
		c.Out <- any(input)
	}
}

type AcquireResource[T any] struct {
	InValue chan TaggedValueWithCtx[T]
	InRes   chan TaggedValueWithCtx[*scheduler.Resource]
	OutVal  chan TaggedValueWithCtx[T]
	OutRes  chan TaggedValueWithCtx[*scheduler.Resource]
}

func NewAcquireResource[T any]() AcquireResource[T] {
	return AcquireResource[T]{
		InValue: make(chan TaggedValueWithCtx[T]),
		InRes:   make(chan TaggedValueWithCtx[*scheduler.Resource]),
		OutVal:  make(chan TaggedValueWithCtx[T]),
		OutRes:  make(chan TaggedValueWithCtx[*scheduler.Resource]),
	}
}

type ImageInCvmfs struct {
	In            chan TaggedValueWithCtx[lib.Image]
	OutInCvmfs    chan TaggedValueWithCtx[lib.Image]
	OutNotInCvmfs chan TaggedValueWithCtx[lib.Image]
	cvmfsRepo     string
}

func NewImageInCvmfs(cvmfsRepo string) ImageInCvmfs {
	return ImageInCvmfs{
		In:            make(chan TaggedValueWithCtx[lib.Image]),
		OutInCvmfs:    make(chan TaggedValueWithCtx[lib.Image]),
		OutNotInCvmfs: make(chan TaggedValueWithCtx[lib.Image]),
		cvmfsRepo:     cvmfsRepo,
	}
}

func (m ImageInCvmfs) Process() {
	defer close(m.OutInCvmfs)
	defer close(m.OutNotInCvmfs)
	for input := range m.In {
		manifestPath := filepath.Join("/", "cvmfs", m.cvmfsRepo, ".metadata", input.Value.GetSimpleName(), "manifest.json")
		if lib.AlreadyConverted(manifestPath, input.Value.Manifest.Config.Digest) == lib.ConversionMatch {
			fmt.Printf("Image %s is already in CVMFS\n", input.Value.GetSimpleName())
			m.OutInCvmfs <- input
		} else {
			fmt.Printf("Image %s is not in CVMFS\n", input.Value.GetSimpleName())
			m.OutNotInCvmfs <- input
		}
	}
	fmt.Println("ImageInCvmfs done")
}
