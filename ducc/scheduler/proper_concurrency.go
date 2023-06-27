package scheduler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	dockerutil "github.com/cvmfs/ducc/docker-api"
	"github.com/cvmfs/ducc/lib"
	"github.com/cvmfs/ducc/temp"
)

type FetchManifestAction struct {
	ImageId  lib.ObjectId
	Manifest dockerutil.Manifest
}

func (a *FetchManifestAction) Work(ctx context.Context) error {
	var err error
	a.Manifest, err = fetchAndStoreImageManifest(a.ImageId)
	if err != nil {
		fmt.Println("Failed to fetch manifest for image", a.ImageId, ":", err)
		return err
	}
	return nil
}

func NewFetchManifestTask(image lib.Image2, pool *ResourcePool) *Task {
	return NewTask(
		1,
		&FetchManifestAction{ImageId: image.Id},
		[]*Task{},
		[]*Resource{pool.CreateOrGetResource(CONTAINER_REGISTRY_RESOURCE_PREFIX+image.Repository, 20)},
	)
}

type SyncImageAction struct {
	imageID lib.ObjectId
}

func UpdateImageFail(image lib.Image, pool *ResourcePool, repo string) error {
	// TODO:
	convertAgain := false
	///////////

	repositoryResource := pool.CreateOrGetResource(CONTAINER_REGISTRY_RESOURCE_PREFIX+image.Repository, 20)
	// 1. Fetch manifest
	fetchManifest := Step{
		Name:          "Fetch manifest",
		Description:   "Fetch manifest for image",
		Prerequisites: []*Step{},
		Resources:     []*Resource{repositoryResource},
	}
	fetchManifest.Start()
	manifest, err := image.GetManifest()
	if err != nil {
		fmt.Println("Failed to fetch manifest for image", image.Id, ":", err)
		fetchManifest.Complete(TS_FAILED)
	}
	fetchManifest.Complete(TS_SUCCESS)

	// 2. ConvertInputOutput
	ConvertInputOutput := Step{
		Name:          "Convert InputOutput",
		Description:   "Fetch layers and ingest them into CVMFS",
		Prerequisites: []*Step{&fetchManifest},
		Resources:     []*Resource{},
	}
	ConvertInputOutput.Start()
	manifestPath := filepath.Join("/", "cvmfs", repo, ".metadata", image.GetSimpleName(), "manifest.json")
	alreadyConverted := lib.AlreadyConverted(manifestPath, manifest.Config.Digest)

	if alreadyConverted == lib.ConversionMatch && !convertAgain {
		ConvertInputOutput.Logs = append(ConvertInputOutput.Logs, "Already converted the image, skipping.")
		ConvertInputOutput.Complete(TS_SUCCESS)
	} else {
		// Create a temporary directory
		tmpDir, err := temp.UserDefinedTempDir("", "conversion")
		if err != nil {
			ConvertInputOutput.Logs = append(ConvertInputOutput.Logs, "Error in creating a temporary directory for all the files")
			ConvertInputOutput.Complete(TS_FAILED)
		}
		defer os.RemoveAll(tmpDir)

	}
	return nil
}

func UpdateImageStep(step *Step, image lib.Image, pool *ResourcePool, repo string) error {
	// Define the step
	step.Name = "Update image"
	step.Description = "Update image " + image.GetSimpleName()
	// Define any needed resources
	step.Resources = []*Resource{
		pool.CreateOrGetResource("UpdateImage", 1),
	}

	fetchManifest := Step{}

	FetchManifestStep(&fetchManifest, image)

	step.Start()

	// TODO
	return nil
}

func FetchManifestStep(step *Step, image lib.Image) dockerutil.Manifest {
	// TODO: Fetch manifest
	return dockerutil.Manifest{}
}
