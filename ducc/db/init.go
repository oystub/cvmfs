package db

import (
	"database/sql"
	"errors"
)

type scannableRow interface {
	Scan(dest ...any) error
}

// Open opens a database transaction, which can be used for calling multiple operations atomically.
// Remember to call `tx.Commit()` or `tx.Rollback()` when done.
func GetTransaction() (*sql.Tx, error) {
	if g_db == nil {
		return nil, errors.New("database not initialized")
	}
	tx, err := g_db.Begin()
	if err != nil {
		panic(err)
	}
	return tx, nil
}

var g_db *sql.DB

func Init(db *sql.DB) {
	g_db = db
}
