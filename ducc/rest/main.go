package rest

import (
	"errors"
	"net/http"

	"github.com/cvmfs/ducc/lib"
	"github.com/cvmfs/ducc/localdb"
	"github.com/cvmfs/ducc/scheduler"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

// Handler
func hello(c echo.Context) error {
	return c.String(http.StatusOK, "Hello, World!")
}

type DuccRestImpl struct {
}

func Setup() {
	var myApi DuccRestImpl
	var err error
	if err != nil {
		panic(err)
	}
	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	RegisterHandlers(e, &myApi)
	e.Logger.Fatal(e.Start(":1323"))
}

// Returns all wishes
// (GET /wish)
func (this *DuccRestImpl) GetWishes(ctx echo.Context) error {
	wishes, err := scheduler.GetAllWishes()
	if err != nil {
		return err
	}
	var out []Wish
	for _, cur_wish := range wishes {
		out = append(out, Wish{
			Id:              cur_wish.Id,
			InputUri:        cur_wish.InputUri,
			CvmfsRepo:       cur_wish.CvmfsRepo,
			Source:          cur_wish.Source,
			CreateLayers:    &cur_wish.CreateLayers,
			CreatePodman:    &cur_wish.CreatePodman,
			CreateThinImage: &cur_wish.CreateThinImage,
			CreateFlat:      &cur_wish.CreateFlat,
		})
	}
	return ctx.JSON(http.StatusOK, out)

}

// Creates a new wish
// (POST /wish)
func (*DuccRestImpl) AddWish(ctx echo.Context) error {
	var wish NewWish
	if err := ctx.Bind(&wish); err != nil {
		return err
	}
	createdWish, err := scheduler.AddOrUpdateWish(lib.Wish2{InputUri: wish.Uri, CvmfsRepo: wish.CvmfsRepo})
	if err != nil {
		return err
	}
	return ctx.JSON(http.StatusOK, newWish(createdWish))
}

// Returns a wish by ID
// (GET /wishes/{id})
func (*DuccRestImpl) GetWishById(ctx echo.Context, id int64) error {
	wish, err := localdb.GetWishById(id)
	if err != nil {
		return err
	}
	return ctx.JSON(http.StatusOK, newWish(wish))
}

// Synchronizes a wish by ID
// (POST /wishes/{id}/sync)
func (*DuccRestImpl) SyncWishById(ctx echo.Context, id int64) error {
	return errors.New("Not implemented")
}

func newWish2(wish Wish) lib.Wish2 {
	return lib.Wish2{
		InputUri:        wish.InputUri,
		CvmfsRepo:       wish.CvmfsRepo,
		Source:          wish.Source,
		CreateLayers:    *wish.CreateLayers,
		CreatePodman:    *wish.CreatePodman,
		CreateThinImage: *wish.CreateThinImage,
		CreateFlat:      *wish.CreateFlat,
	}
}

func newWish(wish2 lib.Wish2) Wish {
	return Wish{
		Id:              wish2.Id,
		InputUri:        wish2.InputUri,
		CvmfsRepo:       wish2.CvmfsRepo,
		Source:          wish2.Source,
		CreateLayers:    &wish2.CreateLayers,
		CreatePodman:    &wish2.CreatePodman,
		CreateThinImage: &wish2.CreateThinImage,
		CreateFlat:      &wish2.CreateFlat,
	}
}
