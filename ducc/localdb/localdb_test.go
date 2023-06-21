package localdb

import (
	"database/sql"
	"testing"
	"time"

	_ "embed"

	"github.com/cvmfs/ducc/lib"
	"github.com/cvmfs/ducc/test"
)

func withDbFixture(dbFixture string) LocalDb {
	var db LocalDb
	err := db.Init(":memory:")
	_, err = db.db.Exec(dbFixture)
	if err != nil {
		panic(err)
	}
	return db
}

var (
	dbFixture0 = ""
	//go:embed fixtures/fixture1.sql
	dbFixture1 string
)

func TestGetWishById(t *testing.T) {
	// Getting a nonexistent wish should return sql.ErrNoRows
	t.Run("GetWishFromEmptyDb", func(t *testing.T) {
		db := withDbFixture(dbFixture0)
		_, err := db.GetWishById(1)
		if err != sql.ErrNoRows {
			t.Fatal("Expected sql.ErrNoRows, got", err)
		}
	})

	// Getting a nonexistent wish should return sql.ErrNoRows
	t.Run("GetNonexistentWish", func(t *testing.T) {
		db := withDbFixture(dbFixture1)
		_, err := db.GetWishById(10)
		if err != sql.ErrNoRows {
			t.Fatal("Expected sql.ErrNoRows, got", err)
		}
	})

	// Getting an existing wish should return the wish
	t.Run("GetExistingWish", func(t *testing.T) {
		db := withDbFixture(dbFixture1)
		wish, err := db.GetWishById(1)
		if err != nil {
			t.Fatal(err)
		}

		expectedWish := lib.Wish2{
			Id:                  1,
			CvmfsRepo:           "repo.invalid",
			InputUri:            "https://registry.invalid/repo:tag",
			OutputUri:           "https://another.registry.invalid/repo:tag",
			Source:              "testfixture",
			CreateLayers:        lib.OB_TRUE,
			CreateThinImage:     lib.OB_TRUE,
			CreatePodman:        lib.OB_TRUE,
			CreateFlat:          lib.OB_TRUE,
			WebhookEnabled:      lib.OB_TRUE,
			FullSyncIntervalSec: 1234,
			LastConfigUpdate:    time.Unix(42, 42),
			LastFullSync:        time.Unix(42, 42),
		}
		if !test.EqualsExceptForFields(wish, expectedWish, []string{"LastConfigUpdate", "LastFullSync"}) {
			t.Fatal("Did not get expected wish")
		}
	})
}

func TestGetAllWishes(t *testing.T) {
	// Getting all wishes from an empty database should return an empty list
	t.Run("GetAllWishesFromEmptyDb", func(t *testing.T) {
		db := withDbFixture(dbFixture0)
		wishes, err := db.GetAllWishes()
		if err != nil {
			t.Fatal(err)
		}
		if len(wishes) != 0 {
			t.Fatal("Expected empty list, got", wishes)
		}
	})

	// Getting all wishes from fixture1
	t.Run("GetAllWishesFromNonEmptyDb", func(t *testing.T) {
		db := withDbFixture(dbFixture1)
		wishes, err := db.GetAllWishes()
		if err != nil {
			t.Fatal(err)
		}
		if len(wishes) != 2 {
			t.Fatal("Expected list with two wishes, got", wishes)
		}
	})
}

func TestCreateOrUpdateWish(t *testing.T) {
	// Create a new wish in an empty database
	t.Run("CreateWishInEmptyDb", func(t *testing.T) {
		db := withDbFixture(dbFixture0)
		wishToInsert := lib.Wish2{
			Id:                  123,
			CvmfsRepo:           "repo.invalid",
			InputUri:            "https://registry.invalid/repo:tag",
			OutputUri:           "https://another.registry.invalid/repo:tag",
			Source:              "testfixture",
			CreateLayers:        lib.OB_TRUE,
			CreateThinImage:     lib.OB_TRUE,
			CreatePodman:        lib.OB_TRUE,
			CreateFlat:          lib.OB_TRUE,
			WebhookEnabled:      lib.OB_TRUE,
			FullSyncIntervalSec: 1234,
			LastConfigUpdate:    time.Unix(42, 42),
			LastFullSync:        time.Unix(42, 42),
		}
		// What ID we pass to the function should be irrelevant
		id, err := db.CreateOrUpdateWish(wishToInsert)
		if err != nil {
			t.Fatal(err)
		}
		if id != 1 {
			t.Fatal("Expected ID 1, got", id)
		}
		// Check that the wish was created
		wish, err := db.GetWishById(1)
		if err != nil {
			t.Fatal(err)
		}
		if !test.EqualsExceptForFields(wish, wishToInsert, []string{"Id", "LastConfigUpdate", "LastFullSync"}) {
			t.Fatal("Did not get expected wish")
		}
	})

	// Creating a wish with different values in `CvmfsRepo`, `InputUri` and `Source` should create a new wish.
	// If the values are the same, the existing wish should be updated.
	t.Run("CreateOrUpdate", func(t *testing.T) {
		db := withDbFixture(dbFixture0)
		var wish1 = lib.Wish2{
			Id:                  1,
			CvmfsRepo:           "repo.invalid",
			InputUri:            "https://registry.invalid/repo:tag",
			OutputUri:           "https://another.registry.invalid/repo:tag",
			Source:              "testfixture",
			CreateLayers:        lib.OB_FALSE,
			CreateThinImage:     lib.OB_FALSE,
			CreatePodman:        lib.OB_FALSE,
			CreateFlat:          lib.OB_FALSE,
			WebhookEnabled:      lib.OB_FALSE,
			FullSyncIntervalSec: 0,
			LastConfigUpdate:    time.Unix(42, 42),
			LastFullSync:        time.Unix(42, 42),
		}
		var wish2 = lib.Wish2{
			Id:                  1,
			CvmfsRepo:           "otherrepo.invalid",
			InputUri:            "https://otherregistry.invalid/repo:tag",
			OutputUri:           "https://another.otherregistry.invalid/repo:tag",
			Source:              "othertestfixture",
			CreateLayers:        lib.OB_TRUE,
			CreateThinImage:     lib.OB_TRUE,
			CreatePodman:        lib.OB_TRUE,
			CreateFlat:          lib.OB_TRUE,
			WebhookEnabled:      lib.OB_TRUE,
			FullSyncIntervalSec: 1234,
			LastConfigUpdate:    time.Unix(43, 43),
			LastFullSync:        time.Unix(43, 43),
		}

		// Create a bitmask of cvmfsRepo, inputUri and source. Test all combinations of these.
		for i := 0; i < 8; i++ {
			id1, err := db.CreateOrUpdateWish(wish1)
			if err != nil {
				t.Fatal(err)
			}
			wishToInsert := wish2
			if i&1 == 0 {
				wishToInsert.CvmfsRepo = wish1.CvmfsRepo
			}
			if i&2 == 0 {
				wishToInsert.InputUri = wish1.InputUri
			}
			if i&4 == 0 {
				wishToInsert.Source = wish1.Source
			}
			id2, err := db.CreateOrUpdateWish(wishToInsert)
			if err != nil {
				t.Fatal(err)
			}
			// Same values, should simply update the existing wish
			if i == 0 {
				if id1 != id2 {
					t.Fatal("Expected same ID, got", id1, id2)
				}
				wish, err := db.GetWishById(id1)
				if err != nil {
					t.Fatal(err)
				}
				if !test.EqualsExceptForFields(wish, wishToInsert, []string{"LastConfigUpdate", "LastFullSync"}) {
					t.Fatal("Expected", wishToInsert, "got", wish)
				}
			} else {
				// Different values, should create a new wish
				if id1 == id2 {
					t.Fatal("Expected different IDs, got", id1, id2)
				}
				// Check that the wish was created correctly
				wish, err := db.GetWishById(id2)
				if err != nil {
					t.Fatal(err)
				}
				if !test.EqualsExceptForFields(wish, wishToInsert, []string{"Id", "LastConfigUpdate", "LastFullSync"}) {
					t.Fatal("Expected", wishToInsert, "got", wish)
				}

			}
		}
	})
}
