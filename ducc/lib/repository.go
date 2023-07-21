package lib

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
)

type ContainerRepository struct {
	Registry *ContainerRegistry
	Name     string
}

func (cr ContainerRepository) BaseUrl() string {
	return fmt.Sprintf("%s/%s", cr.Registry.baseUrl(), cr.Name)
}

func (cr *ContainerRepository) FetchTags(filter string) ([]*Tag, error) {
	url := fmt.Sprintf("%s/tags/list", cr.BaseUrl())
	fmt.Printf("Fetching tags from %s\n", url)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Add("Accept", "application/json")
	res, err := cr.Registry.PerformRequest(req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()
	var tagList struct{ Tags []string }
	if err = json.NewDecoder(res.Body).Decode(&tagList); err != nil {
		return nil, fmt.Errorf("error in decoding the tags from the server: %s", err)
	}

	if filter != "" {
		filteredTags, err := filterUsingGlob(filter, tagList.Tags)
		if err != nil {
			return nil, err
		}
		tagList.Tags = filteredTags
	}
	tags := make([]*Tag, len(tagList.Tags))
	for i, tag := range tagList.Tags {
		tags[i] = &Tag{
			Repository: cr,
			Name:       tag,
		}
	}
	return tags, nil
}

func filterUsingGlob(pattern string, toFilter []string) ([]string, error) {
	result := make([]string, 0)
	regexPattern := strings.ReplaceAll(pattern, "*", ".*")
	regex, err := regexp.Compile(regexPattern)
	if err != nil {
		return result, err
	}
	regex.Longest()
	for _, toCheck := range toFilter {
		s := regex.FindString(toCheck)
		if s == "" {
			continue
		}
		if s == toCheck {
			result = append(result, s)
		}
	}
	return result, nil
}
