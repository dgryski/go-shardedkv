package storagetest

import (
	"errors"
	"testing"

	"github.com/dgryski/go-shardedkv"
)

type Errstore struct{}

func (e Errstore) Get(key string) ([]byte, bool, error) {
	return nil, false, errors.New("error storage get")
}
func (e Errstore) Set(key string, val []byte) error { return errors.New("error storage Set") }
func (e Errstore) Delete(key string) (bool, error)  { return false, errors.New("error storage Delete") }
func (e Errstore) ResetConnection(key string) error {
	return errors.New("error storage ResetConnection")
}

// StorageTest is a simple sanity check for a shardedkv Storage backend
func StorageTest(t *testing.T, storage shardedkv.Storage) {

	v, ok, err := storage.Get("hello")
	if v != nil || ok || err != nil {
		t.Errorf("getting a non-existent key was 'ok': v=%v ok=%v err=%v\n", v, ok, err)
	}

	err = storage.Set("hello", []byte("wowza"))
	if err != nil {
		t.Errorf("error setting key: err=%v\n", err)
	}

	v, ok, err = storage.Get("hello")
	if v == nil || !ok || err != nil || string(v) != "wowza" {
		t.Errorf("failed getting a valid key: v=%v ok=%v err=%v\n", v, ok, err)
	}

	ok, err = storage.Delete("doesnotexist")
	if ok == true || err != nil {
		t.Errorf("failed deleting non-existant key: ok=%v err=%v\n", ok, err)
	}

	ok, err = storage.Delete("hello")
	if ok != true || err != nil {
		t.Errorf("failed deleting key: ok=%v err=%v\n", ok, err)
	}

	err = storage.ResetConnection("hello")
	if err != nil {
		t.Errorf("failed resetting connection for key: err=%v\n", err)
	}

	v, ok, err = storage.Get("hello")
	if v != nil || ok || err != nil {
		t.Errorf("getting a non-existent key post-delete was 'ok': v=%v ok=%v err=%v\n", v, ok, err)
	}
}
