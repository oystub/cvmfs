package lib

type Job interface {
	GetId() int64
	//GetWishId() int
	GetPrerequisiteIds() []int64
	

