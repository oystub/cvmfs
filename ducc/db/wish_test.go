package db

import (
	"database/sql"
	"testing"

	"github.com/cvmfs/ducc/test"
	"github.com/google/uuid"
	"github.com/opencontainers/go-digest"
)

func TestCreateWish(t *testing.T) {
	db := createInMemDBForTesting()
	Init(db)
	defer db.Close()

	t.Run("CreateWishByIdentifier", func(t *testing.T) {
		identifier := WishIdentifier{
			Source:                "source",
			CvmfsRepository:       "cvmfs",
			InputTag:              "tag",
			InputTagWildcard:      false,
			InputRepository:       "repository",
			InputRegistryScheme:   "https",
			InputRegistryHostname: "registry",
		}
		wish, err := CreateWishByIdentifier(nil, identifier)
		if err != nil {
			t.Fatal(err)
		}
		if !test.EqualsExceptForFields(identifier, wish.Identifier, []string{}) {
			t.Fatalf("Expected %v, got %v", identifier, wish.Identifier)
		}
		if wish.ID == (WishID{}) {
			t.Fatal("Expected ID to be set")
		}
	})

	t.Run("CreateWishesByIdentifier", func(t *testing.T) {
		identifiers := []WishIdentifier{
			{
				Source:                "source",
				CvmfsRepository:       "cvmfs",
				InputTag:              "tag",
				InputTagWildcard:      false,
				InputRepository:       "repository",
				InputRegistryScheme:   "https",
				InputRegistryHostname: "registry",
			},
			{
				Source:                "source",
				CvmfsRepository:       "cvmfs",
				InputTag:              "tag2",
				InputTagWildcard:      false,
				InputRepository:       "repository",
				InputRegistryScheme:   "https",
				InputRegistryHostname: "registry",
			},
		}
		wishes, err := CreateWishesByIdentifier(nil, identifiers)
		if err != nil {
			t.Fatal(err)
		}
		if len(wishes) != len(identifiers) {
			t.Fatalf("Expected %d wishes, got %d", len(identifiers), len(wishes))
		}
		for i, wish := range wishes {
			if !test.EqualsExceptForFields(identifiers[i], wish.Identifier, []string{}) {
				t.Fatalf("Expected %v, got %v", identifiers[i], wish.Identifier)
			}
			if wish.ID == (WishID{}) {
				t.Fatal("Expected ID to be set")
			}
		}
	})
}

func TestGetWish(t *testing.T) {
	db := createInMemDBForTesting()
	Init(db)
	defer db.Close()

	dbWishIdentifiers := []WishIdentifier{
		{
			Source:                "source",
			CvmfsRepository:       "cvmfs",
			InputTag:              "tag*",
			InputTagWildcard:      true,
			InputRepository:       "repository",
			InputRegistryScheme:   "https",
			InputRegistryHostname: "registry",
		},
		{
			Source:                "source",
			CvmfsRepository:       "cvmfs",
			InputTag:              "tag",
			InputTagWildcard:      false,
			InputRepository:       "repository",
			InputRegistryScheme:   "https",
			InputRegistryHostname: "registry",
		},
		{
			Source:                "source",
			CvmfsRepository:       "cvmfs",
			InputDigest:           digest.FromString("digest"),
			InputRepository:       "repository2",
			InputRegistryScheme:   "https",
			InputRegistryHostname: "registry2",
		},
	}

	dbWishes, err := CreateWishesByIdentifier(nil, dbWishIdentifiers)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("GetWishesByValue", func(t *testing.T) {
		input := []WishIdentifier{
			dbWishIdentifiers[0],
			dbWishIdentifiers[1],
		}
		wishes, err := GetWishesByValue(nil, input)
		if err != nil {
			t.Fatal(err)
		}
		if len(wishes) != len(input) {
			t.Fatalf("Expected %d wishes, got %d", len(input), len(wishes))
		}
		for i, wish := range wishes {
			if !test.EqualsExceptForFields(dbWishes[i], wish, []string{"ID"}) {
				t.Fatalf("Expected %v, got %v", dbWishes[i], wish)
			}
			if wish.ID != dbWishes[i].ID {
				t.Fatal("Expected ID to be set")
			}
		}
	})

	t.Run("GetWishByValue", func(t *testing.T) {
		input := dbWishIdentifiers[0]
		wish, err := GetWishByValue(nil, input)
		if err != nil {
			t.Fatal(err)
		}
		if !test.EqualsExceptForFields(dbWishes[0], wish, []string{"ID"}) {
			t.Fatalf("Expected %v, got %v", dbWishes[0], wish)
		}
		if wish.ID != dbWishes[0].ID {
			t.Fatal("Expected ID to be set")
		}
	})

	t.Run("GetWishByValueNotFound", func(t *testing.T) {
		input := dbWishIdentifiers[0]
		input.InputTag = "notfound"

		_, err := GetWishByValue(nil, input)
		if err != sql.ErrNoRows {
			t.Fatal("Expected sql.ErrNoRows, got", err)
		}
	})

	t.Run("GetWisheByValueNotFound", func(t *testing.T) {
		notfound := dbWishIdentifiers[0]
		notfound.InputTag = "notfound"

		input := []WishIdentifier{
			dbWishIdentifiers[0],
			notfound,
		}

		_, err := GetWishesByValue(nil, input)
		if err != sql.ErrNoRows {
			t.Fatal("Expected sql.ErrNoRows, got", err)
		}
	})

	t.Run("GetWishesByID", func(t *testing.T) {
		input := []WishID{
			dbWishes[0].ID,
			dbWishes[1].ID,
		}
		wishes, err := GetWishesByIDs(nil, input)
		if err != nil {
			t.Fatal(err)
		}
		if len(wishes) != len(input) {
			t.Fatalf("Expected %d wishes, got %d", len(input), len(wishes))
		}
		for i, wish := range wishes {
			if !test.EqualsExceptForFields(dbWishes[i], wish, []string{}) {
				t.Fatalf("Expected %v, got %v", dbWishes[i], wish)
			}
		}
	})

	t.Run("GetWishByID", func(t *testing.T) {
		input := dbWishes[0].ID
		wish, err := GetWishByID(nil, input)
		if err != nil {
			t.Fatal(err)
		}
		if !test.EqualsExceptForFields(dbWishes[0], wish, []string{}) {
			t.Fatalf("Expected %v, got %v", dbWishes[0], wish)
		}
	})

	t.Run("GetWishByIDNotFound", func(t *testing.T) {
		input := WishID(uuid.New())

		_, err := GetWishByID(nil, input)
		if err != sql.ErrNoRows {
			t.Fatal("Expected sql.ErrNoRows, got", err)
		}
	})

	t.Run("GetWishesByIDsNotFound", func(t *testing.T) {
		notfound := WishID(uuid.New())

		input := []WishID{
			dbWishes[0].ID,
			notfound,
		}

		_, err := GetWishesByIDs(nil, input)
		if err != sql.ErrNoRows {
			t.Fatal("Expected sql.ErrNoRows, got", err)
		}
	})
}

func TestGetWishesSource(t *testing.T) {
	db := createInMemDBForTesting()
	Init(db)
	defer db.Close()

	dbWishIdentifiers := []WishIdentifier{
		{
			Source:                "source",
			CvmfsRepository:       "cvmfs",
			InputTag:              "tag*",
			InputTagWildcard:      true,
			InputRepository:       "repository",
			InputRegistryScheme:   "https",
			InputRegistryHostname: "registry",
		},
		{
			Source:                "source",
			CvmfsRepository:       "cvmfs",
			InputTag:              "tag",
			InputTagWildcard:      false,
			InputRepository:       "repository",
			InputRegistryScheme:   "https",
			InputRegistryHostname: "registry",
		},
		{
			Source:                "source2",
			CvmfsRepository:       "cvmfs",
			InputDigest:           digest.FromString("digest"),
			InputRepository:       "repository2",
			InputRegistryScheme:   "https",
			InputRegistryHostname: "registry2",
		},
	}

	dbWishes, err := CreateWishesByIdentifier(nil, dbWishIdentifiers)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("GetWishesBySource", func(t *testing.T) {
		input := "source"
		wishes, err := GetWishesBySource(nil, input)
		if err != nil {
			t.Fatal(err)
		}
		if len(wishes) != 2 {
			t.Fatalf("Expected 2 wishes, got %d", len(wishes))
		}
		for i, wish := range wishes {
			if !test.EqualsExceptForFields(dbWishes[i], wish, []string{}) {
				t.Fatalf("Expected %v, got %v", dbWishes[i], wish)
			}
		}
	})

	t.Run("GetWishesBySourceNotFound", func(t *testing.T) {
		input := "notfound"

		wishes, err := GetWishesBySource(nil, input)
		if err != nil {
			t.Fatal(err)
		}
		if len(wishes) != 0 {
			t.Fatalf("Expected 0 wishes, got %d", len(wishes))
		}
	})
}
