package storagetest

import (
	"github.com/dgryski/go-shardedkv"
	"testing"
)

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
	if v == nil || !ok || err != nil {
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

	v, ok, err = storage.Get("hello")
	if v != nil || ok || err != nil {
		t.Errorf("getting a non-existent key post-delete was 'ok': v=%v ok=%v err=%v\n", v, ok, err)
	}
}
