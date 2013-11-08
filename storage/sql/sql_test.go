package sql

import (
	"database/sql"
	"github.com/dgryski/go-shardedkv/storagetest"
	_ "github.com/mattn/go-sqlite3"
	"io/ioutil"
	"os"
	"testing"
)

func TestSQL(t *testing.T) {

	f, err := ioutil.TempFile(os.TempDir(), "shardedkv-sql-storagetest")

	if err != nil {
		t.Skipf("unable to create tempfile: %s", err)
	}

	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		t.Errorf("error creating sqlite temp file")
		return
	}

	_, err = db.Exec(`
CREATE TABLE Storage (
key VARCHAR(64) NOT NULL,
value VARCHAR(256) NOT NULL
);
        `)

	if err != nil {
		t.Errorf("error creating table: %s", err)
		return
	}
	db.Close()

	var tblConf = TableConfig{
		Table:       "Storage",
		KeyColumn:   "key",
		ValueColumn: "value",
	}

	connector := func() (*sql.DB, error) {
		return sql.Open("sqlite3", f.Name())
	}

	s, err := New(connector, &tblConf)

	if err != nil {
		t.Errorf("error creating sqlite")
		return
	}

	storagetest.StorageTest(t, s)

	os.Remove(f.Name())
}
