package db

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/opencontainers/go-digest"
)

type ParsedWishInputURL struct {
	Scheme      string
	Registry    string
	Repository  string
	Tag         string
	TagWildcard bool
	Digest      digest.Digest
}

func ParseWishInputURL(imageUrl string) (ParsedWishInputURL, error) {
	url, err := url.Parse(imageUrl)
	if err != nil {
		return ParsedWishInputURL{}, err
	}
	if url.Host == "" {
		// TODO: Do we really want to support this?
		// likely the protocol `https://` is missing in the image string.
		// worth to try to append it, and re-parse the image
		image2 := "https://" + imageUrl
		img2, err2 := ParseWishInputURL(image2)
		if err2 == nil {
			return img2, err2
		}

		// some other error, let's return the first error
		return ParsedWishInputURL{}, fmt.Errorf("Impossible to identify the registry of the image: %s", imageUrl)
	}
	if url.Scheme != "http" && url.Scheme != "https" {
		return ParsedWishInputURL{}, fmt.Errorf("Unsupported protocol: %s", url.Scheme)
	}

	if url.Path == "" {
		return ParsedWishInputURL{}, fmt.Errorf("Impossible to identify the repository of the image: %s", imageUrl)
	}
	colonPathSplitted := strings.Split(url.Path, ":")
	if len(colonPathSplitted) == 0 {
		return ParsedWishInputURL{}, fmt.Errorf("Impossible to identify the path of the image: %s", imageUrl)
	}
	// no split happened, hence we don't have neither a tag nor a digest, but only a path
	if len(colonPathSplitted) == 1 {

		// we remove the first  and the trailing `/`
		repository := strings.TrimLeft(colonPathSplitted[0], "/")
		repository = strings.TrimRight(repository, "/")
		if repository == "" {
			return ParsedWishInputURL{}, fmt.Errorf("Impossible to find the repository for: %s", imageUrl)
		}
		return ParsedWishInputURL{
			Scheme:     url.Scheme,
			Registry:   url.Host,
			Repository: repository,
			Tag:        "latest",
		}, nil

	}
	if len(colonPathSplitted) > 3 {
		fmt.Println(colonPathSplitted)
		return ParsedWishInputURL{}, fmt.Errorf("Impossible to parse the string into an image, too many `:` in : %s", imageUrl)
	}
	// the colon `:` is used also as separator in the digest between sha256
	// and the actuall digest, a len(pathSplitted) == 2 could either means
	// a repository and a tag or a repository and an hash, in the case of
	// the hash however the split will be more complex.  Now we split for
	// the at `@` which separate the digest from everything else. If this
	// split produce only one result we have a repository and maybe a tag,
	// if it produce two we have a repository, maybe a tag and definitely a
	// digest, if it produce more than two we have an error.
	atPathSplitted := strings.Split(url.Path, "@")
	if len(atPathSplitted) > 2 {
		return ParsedWishInputURL{}, fmt.Errorf("To many `@` in the image name: %s", imageUrl)
	}
	var repoTag string
	var parsedDigest digest.Digest
	if len(atPathSplitted) == 2 {
		parsedDigest, err = digest.Parse(atPathSplitted[1])
		if err != nil {
			return ParsedWishInputURL{}, fmt.Errorf("Impossible to parse the digest: %s", atPathSplitted[1])
		}
		repoTag = atPathSplitted[0]
	}
	if len(atPathSplitted) == 1 {
		repoTag = atPathSplitted[0]
	}
	// finally we break up also the repoTag to find out if we have also a
	// tag or just a repository name
	colonRepoTagSplitted := strings.Split(repoTag, ":")

	// only the repository, without the tag
	if len(colonRepoTagSplitted) == 1 {
		repository := strings.TrimLeft(colonRepoTagSplitted[0], "/")
		repository = strings.TrimRight(repository, "/")
		if repository == "" {
			return ParsedWishInputURL{}, fmt.Errorf("Impossible to find the repository for: %s", imageUrl)
		}
		tag := ""
		if parsedDigest == "" {
			// TODO: Warn the user that we are defauting to "latest"
			tag = "latest"
		}
		return ParsedWishInputURL{
			Scheme:     url.Scheme,
			Registry:   url.Host,
			Repository: repository,
			Digest:     parsedDigest,
			Tag:        tag,
		}, nil
	}

	// both repository and tag
	if len(colonRepoTagSplitted) == 2 {
		repository := strings.TrimLeft(colonRepoTagSplitted[0], "/")
		repository = strings.TrimRight(repository, "/")
		if repository == "" {
			return ParsedWishInputURL{}, fmt.Errorf("Impossible to find the repository for: %s", imageUrl)
		}
		tag := colonRepoTagSplitted[1]
		if parsedDigest != "" {
			// Digest takes precedence over the tag
			tag = ""
		}
		tagWildcard := strings.Contains(tag, "*")

		return ParsedWishInputURL{
			Scheme:      url.Scheme,
			Registry:    url.Host,
			Repository:  repository,
			Digest:      parsedDigest,
			Tag:         tag,
			TagWildcard: tagWildcard,
		}, nil
	}
	return ParsedWishInputURL{}, fmt.Errorf("Impossible to parse the wish: %s", imageUrl)
}

func parseRecipe(recipe string) (RecipeV1, error) {
	return RecipeV1{}, errors.New("not implemented")
}
