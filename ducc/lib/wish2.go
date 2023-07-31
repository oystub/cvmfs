package lib

import (
	"fmt"
	"time"

	"github.com/opencontainers/go-digest"
)

type Wish2 struct {
	Id ObjectId

	CvmfsRepo string
	InputUri  string
	OutputUri string
	Source    string

	CreateLayers    OptionalBool
	CreateThinImage OptionalBool
	CreatePodman    OptionalBool
	CreateFlat      OptionalBool

	WebhookEnabled      OptionalBool
	FullSyncIntervalSec int64

	LastConfigUpdate time.Time
	LastFullSync     time.Time
}

type WishOperations struct {
	CreateLayers    bool
	CreateThinImage bool
	CreatePodman    bool
	CreateFlat      bool
}

type WishIdentifier struct {
	InputRegistry ContainerRegistryIdentifier
	TagStr        string
	ImageDigest   digest.Digest
	CvmfsRepo     string
	Source        string
}

func (w WishIdentifier) String() string {
	printedTag := ""
	if w.TagStr != "" {
		printedTag = ":" + w.TagStr
	}
	printedDigest := ""
	if w.ImageDigest.String() != "" {
		printedDigest = "@" + w.ImageDigest.String()
	}

	return fmt.Sprintf("[%s] %s://%s/%s@%s -> %s", w.Source, w.InputRegistry.Scheme,
		w.InputRegistry.Hostname, printedTag, printedDigest, w.CvmfsRepo)

}

type WishInternal struct {
	WishIdentifier
	WishOperations
	Repository *ContainerRepository
}

type WishInternalWithTags struct {
	WishInternal
	Tags []*Tag
}

type WishInternalWithTagsAndManifests struct {
	WishInternal
	Tags []*TagWithManifest
}
