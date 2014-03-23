package replica

import (
	"testing"

	"github.com/dgryski/go-shardedkv/storage/memory"
	"github.com/dgryski/go-shardedkv/storagetest"
)

// the dev-null of storage engines
type discard struct{}

func (d discard) Get(key string) ([]byte, bool, error) { return nil, false, nil }
func (d discard) Set(key string, val []byte) error     { return nil }
func (d discard) Delete(key string) (bool, error)      { return false, nil }
func (d discard) ResetConnection(key string) error     { return nil }

func checkMultiError(t *testing.T, err error, what string) {
	me, ok := err.(MultiError)
	if !ok {
		t.Errorf("error not a multierror from %s: %T", what, err)
	}

	if len(me) != 2 {
		t.Errorf("%s len(multierror)=%d, wanted 2", what, len(me))
	}
}

func TestMultiError(t *testing.T) {
	r := New(0, storagetest.Errstore{}, storagetest.Errstore{})

	v, ok, err := r.Get("hello")
	if v != nil || ok || err == nil {
		t.Errorf("got a key from an error store: v=%v ok=%v err=%v", v, ok, err)
	}
	checkMultiError(t, err, "get")

	err = r.Set("hello", []byte("world"))
	if err == nil {
		t.Errorf("set a key from an error store: err=%v", err)
	}
	checkMultiError(t, err, "set")

	ok, err = r.Delete("hello")
	if ok || err == nil {
		t.Errorf("deleted a key from an error store: ok=%v err=%v", ok, err)
	}
	checkMultiError(t, err, "delete")

	err = r.ResetConnection("hello")
	if err == nil {
		t.Errorf("reset connection on an error store: v=%v ok=%v err=%v", v, ok, err)
	}
	checkMultiError(t, err, "resetconnection")
}

func TestDiscardReplica(t *testing.T) {
	for i := 0; i < 10; i++ {
		r := New(0, discard{}, memory.New())
		storagetest.StorageTest(t, r)
		r = New(0, memory.New(), discard{})
		storagetest.StorageTest(t, r)
	}
}

func TestErrorReplica(t *testing.T) {

	for i := 0; i < 10; i++ {
		r := New(2, storagetest.Errstore{}, memory.New())
		storagetest.StorageTest(t, r)
		r = New(2, memory.New(), storagetest.Errstore{})
		storagetest.StorageTest(t, r)
	}
}
