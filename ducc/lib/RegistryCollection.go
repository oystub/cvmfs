package lib

import (
	"fmt"
	"sync"
)

type RegistryCollection struct {
	RegistriesMutex sync.Mutex
	Registries      map[ContainerRegistryIdentifier]*ContainerRegistry
}

func NewRegistryCollection() *RegistryCollection {
	return &RegistryCollection{
		Registries: make(map[ContainerRegistryIdentifier]*ContainerRegistry),
	}
}

func (rc *RegistryCollection) AddRegistry(registry *ContainerRegistry) error {
	rc.RegistriesMutex.Lock()
	defer rc.RegistriesMutex.Unlock()

	// Check if the registry already exists
	if _, ok := rc.Registries[registry.Identifier]; ok {
		return fmt.Errorf("registry already exists")
	}

	rc.Registries[registry.Identifier] = registry
	return nil
}

func (rc *RegistryCollection) RemoveRegistry(registry *ContainerRegistry) error {
	rc.RegistriesMutex.Lock()
	defer rc.RegistriesMutex.Unlock()

	// Check if the registry already exists
	if _, ok := rc.Registries[registry.Identifier]; !ok {
		return fmt.Errorf("registry does not exist")
	}

	delete(rc.Registries, registry.Identifier)
	return nil
}

func (rc *RegistryCollection) GetRegistry(identifier ContainerRegistryIdentifier) *ContainerRegistry {
	rc.RegistriesMutex.Lock()
	defer rc.RegistriesMutex.Unlock()

	// Return nil if the registry does not exist
	if _, ok := rc.Registries[identifier]; !ok {
		return nil
	}

	return rc.Registries[identifier]
}
