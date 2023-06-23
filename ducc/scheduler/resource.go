package scheduler

import (
	"sort"
	"sync"
)

// TODO: Improve resource management. The current soulution prevents deadlocks, but can be very inefficient.

const CONTAINER_REPOSITORY_RESOURCE_PREFIX = "container_repository_"

type Resource struct {
	order     int
	available int
	cv        sync.Cond
}

type ResourcePool struct {
	mutex     sync.Mutex
	nextOrder int
	resources map[string]*Resource
}

func NewResourcePool() *ResourcePool {
	return &ResourcePool{resources: make(map[string]*Resource)}
}

// Create a new resource with the given id and maxUsage.
// If a resource with the given id already exists, the existing resource is returned, and maxUsage is ignored.
func (p *ResourcePool) CreateOrGetResource(id string, maxUsage int) *Resource {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	if p.resources[id] != nil {
		return p.resources[id]
	}
	p.resources[id] = &Resource{order: p.nextOrder, available: maxUsage, cv: sync.Cond{L: &sync.Mutex{}}}
	p.nextOrder++
	return p.resources[id]
}

func (r *Resource) Release() {
	r.cv.L.Lock()
	r.available++
	r.cv.Signal()
	r.cv.L.Unlock()
}

func (r *Resource) Acquire() {
	r.cv.L.Lock()
	for r.available == 0 {
		r.cv.Wait()
	}
	r.available--
	r.cv.L.Unlock()
}

func (r *Resource) TryAcquire() bool {
	r.cv.L.Lock()
	if r.available == 0 {
		r.cv.L.Unlock()
		return false
	}
	r.available--
	r.cv.L.Unlock()
	return true
}

func AcquireMultiple(resources []*Resource) {
	// Sort resources by order
	sort.Slice(resources, func(i, j int) bool {
		return resources[i].order < resources[j].order
	})
	for _, r := range resources {
		r.Acquire()
	}

}

func ReleaseMultiple(resources []*Resource) {
	for _, r := range resources {
		r.Release()
	}
}
