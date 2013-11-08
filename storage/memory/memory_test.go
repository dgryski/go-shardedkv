package memory

import (
	"github.com/dgryski/go-shardedkv/storagetest"
	"testing"
)

func TestMemory(t *testing.T) {
	m := New()
	storagetest.StorageTest(t, m)
}
