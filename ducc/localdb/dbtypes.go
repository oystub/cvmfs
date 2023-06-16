package localdb

type OptionalBool int

const db_invalid_id int64 = -1

const (
	OB_NONE OptionalBool = iota
	OB_TRUE
	OB_FALSE
)

type DbWish struct {
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

type DbImage struct {
	Id int64

	Scheme     string
	Registry   string
	Repository string
	Tag        string
	Digest     string
}

type DbImageAndManifest struct {
	imagePair DbImage
	manifest  string
}
