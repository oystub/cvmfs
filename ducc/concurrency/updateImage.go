package concurrency

import (
	"time"

	"github.com/cvmfs/ducc/scheduler"
)

type something struct {
}

func createUpdateImageTask(task *scheduler.Task2, image something, registry string) (error, something) {
	registryResource := task.ResourcePool.CreateOrGetResource(scheduler.CONTAINER_REGISTRY_RESOURCE_PREFIX+registry, scheduler.DEFAULT_CONCURRENT_REPOSITORY_ACTIONS)
	task.Name = "Update Image"
	downloadManifest := scheduler.NewTask2("Download Manifest", task.ResourcePool, 0)
	downloadManifest.AddRequiredResources(registryResource)

	return nil, something{}
}

func createDownloadLayersTask(task *scheduler.Task2, image something, registry string, manifest something) error {
	registryResource := task.ResourcePool.CreateOrGetResource(scheduler.CONTAINER_REGISTRY_RESOURCE_PREFIX+registry, scheduler.DEFAULT_CONCURRENT_REPOSITORY_ACTIONS)
	task.Name = "Download Layers"

	layerPaths := make(chan string)

	for _, layer := range manifest.Layers() {
		downloadLayer := scheduler.NewTask2("Download Layer", task.ResourcePool, 5)
		downloadLayer.AddRequiredResources(registryResource)
		task.AddChild(downloadLayer, true)

		go func(t *scheduler.Task2, layer something, layerQ chan<- string) {
			t.StartWhenReady()
			for {
				err, path := downloadIndividualLayer(layer)
				switch err {
				case nil:
					layerQ <- path
					t.CompleteWhenReady(scheduler.TS_SUCCESS)
					return
				default:
					// TODO: Return 429 in a suitable way
					if t.Retry() {
						// Set backoff for the resource
						registryResource.ProhibitUntil(time.Now().Add(5 * time.Second))
						continue
					}
					t.CompleteWhenReady(scheduler.TS_FAILED)
					return
				}
			}
		}(downloadLayer, layer, layerPaths)
	}
	task.StartWhenReady()
	task.CompleteWhenReady(scheduler.TS_SUCCESS)
	return nil
}

func getManifest() (error, something) {
	return nil, something{}
}

func (something) Layers() []something {
	return []something{something{}}
}

func downloadIndividualLayer(layer something) (error, string) {
	return nil, "a path"
}
