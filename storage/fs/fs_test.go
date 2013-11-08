package fs

import (
	"github.com/dgryski/go-shardedkv/storagetest"
	"io/ioutil"
	"os"
	"testing"
)

func TestFS(t *testing.T) {

	dir, err := ioutil.TempDir(os.TempDir(), "shardedkv-fs-storagetest")

	if err != nil {
		t.Skipf("Unable to create temporary directory: %s", err)
	}

	m := New(dir)
	storagetest.StorageTest(t, m)

	// cleanup
	os.RemoveAll(dir)
}
