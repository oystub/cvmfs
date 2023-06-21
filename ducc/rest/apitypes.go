package rest

import (
	"github.com/cvmfs/ducc/lib"
)

type CreateWish struct {
	CvmfsRepo string `json:"cvmfsRepo"`
	InputUri  string `json:"inputUri"`

	OutputUri string `json:"outputUri"`
	Source    string `json:"source"`

	CreateLayers    lib.OptionalBool `json:"createLayers"`
	CreateThinImage lib.OptionalBool `json:"createThinImage"`
	CreatePodman    lib.OptionalBool `json:"createPodman"`
	CreateFlat      lib.OptionalBool `json:"createFlat"`

	WebhookEnabled      lib.OptionalBool `json:"webhookEnabled"`
	FullSyncIntervalSec int64            `json:"fullSyncIntervalSec"`
}

type Wish struct {
	Id        int64  `json:"id"`
	CvmfsRepo string `json:"cvmfsRepo"`
	InputUri  string `json:"inputUri"`
	OutputUri string `json:"outputUri"`
	Source    string `json:"source"`

	CreateLayers    lib.OptionalBool `json:"createLayers"`
	CreateThinImage lib.OptionalBool `json:"createThinImage"`
	CreatePodman    lib.OptionalBool `json:"createPodman"`
	CreateFlat      lib.OptionalBool `json:"createFlat"`

	WebhookEnabled      lib.OptionalBool `json:"webhookEnabled"`
	FullSyncIntervalSec int64            `json:"fullSyncIntervalSec"`

	LastConfigUpdate string `json:"lastConfigUpdate"`
	LastFullSync     string `json:"lastFullSync"`
}

func (this CreateWish) ToWish2() lib.Wish2 {
	return lib.Wish2{
		Id:                  lib.INVALID_OBJECT_ID,
		CvmfsRepo:           this.CvmfsRepo,
		InputUri:            this.InputUri,
		OutputUri:           this.OutputUri,
		Source:              this.Source,
		CreateLayers:        this.CreateLayers,
		CreateThinImage:     this.CreateThinImage,
		CreatePodman:        this.CreatePodman,
		CreateFlat:          this.CreateFlat,
		WebhookEnabled:      this.WebhookEnabled,
		FullSyncIntervalSec: this.FullSyncIntervalSec,
	}
}

// Cast from Wish2 to Wish
func WishFromWish2(wish2 lib.Wish2) Wish {
	return Wish{
		Id:                  int64(wish2.Id),
		CvmfsRepo:           wish2.CvmfsRepo,
		InputUri:            wish2.InputUri,
		OutputUri:           wish2.OutputUri,
		Source:              wish2.Source,
		CreateLayers:        wish2.CreateLayers,
		CreateThinImage:     wish2.CreateThinImage,
		CreatePodman:        wish2.CreatePodman,
		CreateFlat:          wish2.CreateFlat,
		WebhookEnabled:      wish2.WebhookEnabled,
		FullSyncIntervalSec: wish2.FullSyncIntervalSec,
		LastConfigUpdate:    wish2.LastConfigUpdate.Format(lib.DATE_FORMAT),
		LastFullSync:        wish2.LastFullSync.Format(lib.DATE_FORMAT),
	}
}
