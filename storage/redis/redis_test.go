package redis

import (
	"github.com/dgryski/go-shardedkv/storagetest"
	"testing"
)

func TestRedis(t *testing.T) {

	s, err := New("localhost:6379")
	if err != nil {
		t.Skip("error connecting to redis instance on localhost:6379 -- can't test")
		return
	}

	storagetest.StorageTest(t, s)
}
