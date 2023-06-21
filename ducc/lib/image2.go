package lib

type Image2 struct {
	Id ObjectId

	Scheme     string
	Registry   string
	Repository string
	Tag        string
	Digest     string
}
