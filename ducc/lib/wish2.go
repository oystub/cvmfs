package lib

import "time"

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
