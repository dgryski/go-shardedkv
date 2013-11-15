// Package sql is an SQL-backed key-value store
package sql

import (
	"database/sql"
	"fmt"
)

/*
TODO(dgryski): support multiple value columns
    perl has ShardedKV::MySQL->set("foo", ["bar", "baz", "qux"])
    We can define this as base behaviour and require users to add replacement bits for their tables
    This requires changing Get/Set to take and return interface{} instead of []byte
*/

// TODO(dgryski): include all other table creation logic
// TODO(dgryski): cache prepared queries

type Storage struct {
	db        *sql.DB
	config    *TableConfig
	connector func() (*sql.DB, error)
}

// TableConfig is the configuration for the table used for the key-value store
type TableConfig struct {
	Table       string
	KeyColumn   string
	ValueColumn string
}

// New returns a new sql key-value store.  Connector should be a function which
// returns an sql.DB object for the database where the table lives.
func New(connector func() (*sql.DB, error), config *TableConfig) (*Storage, error) {

	db, err := connector()
	if err != nil {
		return nil, err
	}

	return &Storage{
		db:        db,
		connector: connector,
		config:    config,
	}, nil
}

func (s *Storage) Get(key string) ([]byte, bool, error) {

	q := fmt.Sprintf("SELECT %s FROM %s where %s = ?", s.config.ValueColumn, s.config.Table, s.config.KeyColumn)

	stmt, err := s.db.Prepare(q)
	if err != nil {
		return nil, false, err
	}
	defer stmt.Close()

	var val []byte
	err = stmt.QueryRow(key).Scan(&val)

	switch err {
	case nil:
		return val, true, nil
	case sql.ErrNoRows:
		return nil, false, nil
	default:
		return nil, false, err
	}

	panic("not reached")
}

func (s *Storage) Set(key string, val []byte) error {

	// TODO(dgryski): sqlite doesn't have ODKU
	q := fmt.Sprint("INSERT OR REPLACE INTO ", s.config.Table, " (", s.config.KeyColumn, ",", s.config.ValueColumn, ") VALUES (?, ?)")

	stmt, err := s.db.Prepare(q)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.Exec(key, val)

	return err
}

func (s *Storage) Delete(key string) (bool, error) {

	q := fmt.Sprint("DELETE FROM ", s.config.Table, " WHERE ", s.config.KeyColumn, "=?")

	stmt, err := s.db.Prepare(q)
	if err != nil {
		return false, err
	}
	defer stmt.Close()

	result, err := stmt.Exec(key)
	if err != nil {
		return false, err
	}

	n, _ := result.RowsAffected()

	return n == 1, nil
}

func (s *Storage) ResetConnection(key string) error {

	s.db.Close()

	var err error
	s.db, err = s.connector()

	return err
}
