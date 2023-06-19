package lib

type Image2 struct {
	Id int64

	Scheme     string
	Registry   string
	Repository string
	Tag        string
	Digest     string
}
