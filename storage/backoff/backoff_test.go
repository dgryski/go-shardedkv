package backoff

import (
	"testing"
	"time"

	"github.com/dgryski/go-shardedkv"
	"github.com/dgryski/go-shardedkv/storage/memory"
	"github.com/dgryski/go-shardedkv/storagetest"
)

func TestBackoff(t *testing.T) {
	m := memory.New()
	b := &Storage{Store: m}
	storagetest.StorageTest(t, b)
}

func TestError(t *testing.T) {
	e := storagetest.Errstore{}
	b := &Storage{Store: e, MaxWarns: 5}

	for i := 0; i < 5; i++ {
		_, _, err := b.Get("foo")

		if err == ErrBackingOff {
			t.Errorf("bad error from errstore: got %v", err)
		}
	}
	_, _, err := b.Get("foo")
	if err != ErrBackingOff {
		t.Errorf("bad error from backoff: got %v", err)
	}

	timeNow = func() time.Time { return time.Now().Add(10 * time.Second) }

	_, _, err = b.Get("foo")
	if err == ErrBackingOff {
		t.Errorf("bad error from post-backoff time: got %v", err)
	}
}

var _ shardedkv.Storage = &Storage{}
