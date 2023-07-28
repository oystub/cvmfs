package db

import (
	"database/sql"

	"github.com/cvmfs/ducc/config"
	"github.com/google/uuid"
	"github.com/opencontainers/go-digest"
)

const wishSqlFieldsOrdered string = "id, source, cvmfs_repository, input_tag, input_tag_wildcard, input_digest, input_repository, input_registry_scheme, input_registry_hostname, create_layers, create_flat, create_podman, create_thin, webhook_enabled"

// const wishSqlFieldsQs string = "?,?,?,?,?,?,?,?,?,?,?,?;"
const wishIdentifierSqlFieldsOrdered string = "id, source, cvmfs_repository, input_tag, input_tag_wildcard, input_digest, input_repository, input_registry_scheme, input_registry_hostname"
const wishIdentifierSqlFieldsQs string = "?,?,?,?,?,?,?,?,?"

type WishID uuid.UUID

type WishIdentifier struct {
	Source                string
	CvmfsRepository       string
	InputTag              string
	InputTagWildcard      bool
	InputDigest           digest.Digest
	InputRepository       string
	InputRegistryScheme   string
	InputRegistryHostname string
}

type WishOutputOptions struct {
	CreateLayers bool
	CreateFlat   bool
	CreatePodman bool
	CreateThin   bool
}

type WishScheduleOptions struct {
	WebhookEnabled bool
}

type Wish struct {
	ID              WishID `dbrow:"id"`
	Identifier      WishIdentifier
	OutputOptions   WishOutputOptions
	ScheduleOptions WishScheduleOptions
}

// CreateWishByIdentifier creates a wish in the database from a wish identifier.
// All options will be set to NULL, which means that the default values will be used.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func CreateWishByIdentifier(tx *sql.Tx, identifier WishIdentifier) (Wish, error) {
	ws, err := CreateWishesByIdentifier(tx, []WishIdentifier{identifier})
	if err != nil {
		return Wish{}, err
	}
	return ws[0], nil
}

// CreateWishesByIdentifier creates wishes in the database from a slice of wish identifiers.
// All options will be sett to NULL, which means that the default values will be used.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func CreateWishesByIdentifier(tx *sql.Tx, identifiers []WishIdentifier) ([]Wish, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		tx, err := GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	stmnt := "INSERT INTO wishes (id," + wishIdentifierSqlFieldsOrdered + ") VALUES (" + wishIdentifierSqlFieldsQs + ",?) RETURNING " + wishSqlFieldsOrdered

	// Prepare the statement
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	defer prepStmnt.Close()

	createdWishes := make([]Wish, 0, len(identifiers))
	for _, identifier := range identifiers {
		// Generate a new ID
		var wishId WishID
		if id, err := uuid.NewRandom(); err != nil {
			wishId = WishID(id)
		} else {
			return nil, err
		}
		// Handle nullable values
		input_tag := sql.NullString{String: identifier.InputTag, Valid: identifier.InputTag != ""}
		input_digest := sql.NullString{String: identifier.InputDigest.String(), Valid: identifier.InputDigest != digest.Digest("")}

		row := prepStmnt.QueryRow(wishId, identifier.Source, identifier.CvmfsRepository, input_tag, identifier.InputTagWildcard, input_digest, identifier.InputRepository, identifier.InputRegistryScheme, identifier.InputRegistryHostname)
		w, err := parseWishFromRow(row)
		if err != nil {
			return nil, err
		}
		createdWishes = append(createdWishes, w)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return createdWishes, nil
}

// GetWishByID returns a wish by its ID
// If the wish is not found, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetWishByID(tx *sql.Tx, id WishID) (Wish, error) {
	w, err := GetWishesByIds(tx, []WishID{id})
	if err != nil {
		return Wish{}, err
	}
	return w[0], nil
}

// GetWishesByIds takes in a slice of wish IDs and returns a slice of wishes from the database.
// Unless all wishes are found, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetWishesByIds(tx *sql.Tx, ids []WishID) ([]Wish, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		tx, err := GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	wishes := make([]Wish, 0, len(ids))
	stmnt := "SELECT " + wishSqlFieldsOrdered + " from wishes WHERE id = ?"
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	defer prepStmnt.Close()

	for _, id := range ids {
		row := prepStmnt.QueryRow(id)
		w, err := parseWishFromRow(row)
		if err != nil {
			return nil, err
		}
		wishes = append(wishes, w)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return wishes, nil
}

// GetWishByValue takes in a wish identifier and returns a wish from the database.
// If the wish is not found, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetWishByValue(tx *sql.Tx, identifier WishIdentifier) (Wish, error) {
	wish, err := GetWishesByValue(tx, []WishIdentifier{identifier})
	if err != nil {
		return Wish{}, err
	}
	return wish[0], nil
}

// GetWishesByValue takes in a slice of wish identifiers and returns a slice of wishes from the database.
// Unless all wishes are found, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetWishesByValue(tx *sql.Tx, identifiers []WishIdentifier) ([]Wish, error) {
	ownTx := false
	if tx == nil {
		ownTx = true
		tx, err := GetTransaction()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
	}

	stmnt := "SELECT " + wishSqlFieldsOrdered + " WHERE (" + wishIdentifierSqlFieldsOrdered + ") IN (()" + wishIdentifierSqlFieldsQs + "))"
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return nil, err
	}
	defer prepStmnt.Close()

	wishes := make([]Wish, 0, len(identifiers))
	for _, identifier := range identifiers {

		// Handle nullable values
		input_tag := sql.NullString{String: identifier.InputTag, Valid: identifier.InputTag != ""}
		input_digest := sql.NullString{String: identifier.InputDigest.String(), Valid: identifier.InputDigest != digest.Digest("")}

		row := tx.QueryRow(stmnt, identifier.Source, identifier.CvmfsRepository, input_tag, identifier.InputTagWildcard, input_digest, identifier.InputRepository, identifier.InputRegistryScheme, identifier.InputRegistryHostname)
		wish, err := parseWishFromRow(row)
		if err != nil {
			return nil, err
		}
		wishes = append(wishes, wish)
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return nil, err
		}
	}

	return wishes, nil
}

// GetWishesBySource takes in a source and returns a slice of all wishes from the database with that source.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func GetWishesBySource(tx *sql.Tx, source string) ([]Wish, error) {
	stmnt := "SELECT " + wishSqlFieldsOrdered + " from wishes WHERE source = $1"

	var res *sql.Rows
	var err error
	if tx == nil {
		res, err = g_db.Query(stmnt, source)
	} else {
		res, err = tx.Query(stmnt, source)
	}

	if err != nil {
		return nil, err
	}
	defer res.Close()

	wishes, err := parseWishesFromRows(res)
	if err != nil {
		return nil, err
	}
	return wishes, nil
}

// DeleteWishesBySource deletes all wishes from the database with the given source.
// Unless all wishes are deleted, an error is returned.
// If a tx is provided, it will be used to query the database. No commit or rollback will be performed.
func DeleteWishesByID(tx *sql.Tx, ids []WishID) error {
	ownTx := false
	if tx == nil {
		ownTx = true
		tx, err := GetTransaction()
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	stmnt := "DELETE FROM wishes WHERE id = ?"

	// Prepare the statement
	prepStmnt, err := tx.Prepare(stmnt)
	if err != nil {
		return err
	}
	defer prepStmnt.Close()

	for _, id := range ids {
		_, err := prepStmnt.Exec(id)
		if err != nil {
			return err
		}
	}

	if ownTx {
		err := tx.Commit()
		if err != nil {
			return err
		}
	}

	return nil
}

// parseWishesFromRows takes in a sql.Rows and Scans it into a wish.
// If any options contain NULL, the default values will be used.
// The row must contain the exact fields in the wishSqlFieldsOrdered constant, in the same order.
func parseWishFromRow(row scannableRow) (Wish, error) {
	w := Wish{}
	var input_tag, input_digest sql.NullString
	var create_layers, create_flat, create_podman, create_thin, webhook_enabled sql.NullBool
	err := row.Scan(w.ID, w.Identifier.Source, w.Identifier.CvmfsRepository, input_tag, w.Identifier.InputTagWildcard, input_digest, w.Identifier.InputRepository, w.Identifier.InputRegistryScheme, w.Identifier.InputRegistryHostname, create_layers, create_flat, create_podman, create_thin, webhook_enabled)
	if err != nil {
		return Wish{}, err
	}

	// Handle the null values
	if input_tag.Valid {
		w.Identifier.InputTag = input_tag.String
	}
	if input_digest.Valid {
		w.Identifier.InputDigest, _ = digest.Parse(input_digest.String)
		// TODO: Handle invalid database state
	}
	if create_layers.Valid {
		w.OutputOptions.CreateLayers = create_layers.Bool
	} else {
		w.OutputOptions.CreateLayers = config.DEFAULT_CREATELAYERS
	}
	if create_flat.Valid {
		w.OutputOptions.CreateFlat = create_flat.Bool
	} else {
		w.OutputOptions.CreateFlat = config.DEFAULT_CREATEFLAT
	}
	if create_podman.Valid {
		w.OutputOptions.CreatePodman = create_podman.Bool
	} else {
		w.OutputOptions.CreatePodman = config.DEFAULT_CREATEPODMAN
	}
	if create_thin.Valid {
		w.OutputOptions.CreateThin = create_thin.Bool
	} else {
		w.OutputOptions.CreateThin = config.DEFAULT_CREATETHINIMAGE
	}
	if webhook_enabled.Valid {
		w.ScheduleOptions.WebhookEnabled = webhook_enabled.Bool
	} else {
		w.ScheduleOptions.WebhookEnabled = config.DEFAULT_WEBHOOKENABLED
	}

	return w, nil
}

// parseWishesFromRows takes in a sql.Rows and Scans it into a slice of wishes.
// If any options contain NULL, the default values will be used.
// The rows must contain the exact fields in the wishSqlFieldsOrdered constant, in the same order.
// Unless all rows are successfully parsed, an error is returned.
func parseWishesFromRows(rows *sql.Rows) ([]Wish, error) {
	wishes := make([]Wish, 0)
	for rows.Next() {
		w, err := parseWishFromRow(rows)
		if err != nil {
			return nil, err
		}
		wishes = append(wishes, w)
	}

	return wishes, nil
}
