package replica

import (
	"github.com/dgryski/go-shardedkv/storage/memory"
	"github.com/dgryski/go-shardedkv/storagetest"
	"testing"
)

// the dev-null of storage engines
type discard struct{}

func (d discard) Get(key string) ([]byte, bool, error) { return nil, false, nil }
func (d discard) Set(key string, val []byte) error     { return nil }
func (d discard) Delete(key string) (bool, error)      { return false, nil }
func (d discard) ResetConnection(key string) error     { return nil }

func TestReplica(t *testing.T) {
	for i := 0; i < 10; i++ {
		r := New(discard{}, memory.New())
		storagetest.StorageTest(t, r)
		r = New(memory.New(), discard{})
		storagetest.StorageTest(t, r)
	}
}
