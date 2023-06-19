package lib

type Wish2 struct {
	Id int64

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

	LastConfigUpdate int64
	LastFullSync     int64
}
