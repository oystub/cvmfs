package lib

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"

	dockerutil "github.com/cvmfs/ducc/docker-api"
)

type Tag struct {
	Repository *ContainerRepository
	Name       string
	Digest     string
	Manifest   dockerutil.Manifest // TODO: Determine what type this should be
}

type TagWithManifest struct {
	Tag
	Manifest *dockerutil.Manifest
}

func (t *Tag) BaseUrl() string {
	if t.Digest != "" {
		return fmt.Sprintf("%s/%s", t.Repository.BaseUrl(), t.Name)
	}
	return fmt.Sprintf("%s/%s", t.Repository.BaseUrl(), t.Name)
}

func (t *Tag) FetchManifest() (*dockerutil.Manifest, error) {
	url := fmt.Sprintf("%s/manifests/%s", t.Repository.BaseUrl(), t.Name)
	fmt.Printf("Fetching manifest from %s\n", url)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Add("Accept", "application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.manifest.v1+json")
	res, err := t.Repository.Registry.PerformRequest(req)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(body, &t.Manifest)
	if err != nil {
		return nil, err
	}
	if reflect.DeepEqual(dockerutil.Manifest{}, t.Manifest) {
		return nil, fmt.Errorf("got empty manifest")
	}
	t.Digest = t.Manifest.Config.Digest
	return &t.Manifest, nil
}

func (t *Tag) GetManifestUrl(reference string) string {
	url := t.Repository.BaseUrl() + "manifests/"
	if reference != "" {
		url = fmt.Sprintf("%s%s", url, reference)
	} else if t.Digest != "" {
		url = fmt.Sprintf("%s%s", url, t.Digest)
	} else {
		url = fmt.Sprintf("%s%s", url, t.Name)
	}
	return url
}

func (t *Tag) GetSimpleName() string {
	return fmt.Sprintf("%s:%s", t.Repository.Name, t.Name)
}
