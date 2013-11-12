package shardedkv

import (
	kc "github.com/dgryski/go-shardedkv/choosers/ketama"
	st "github.com/dgryski/go-shardedkv/storage/memory"
	"strconv"
	"testing"
)

func TestShardedkv(t *testing.T) {
	var shards []Shard
	nElements := 1000
	nShards := 10

	for i := 0; i < nShards; i++ {
		label := "test_shard" + strconv.Itoa(i)
		shards = append(shards, Shard{Name: label, Backend: st.New()})
	}

	chooser := kc.New()

	kv := New(chooser, shards)

	for i := 0; i < nElements; i++ {
		kv.Set("test"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))
	}

	for i := 0; i < nElements; i++ {
		k := "test" + strconv.Itoa(i)

		v, ok, err := kv.Get(k)
		if ok != true {
			t.Errorf("failed  to get key: %s\n", err)
		}

		if string(v) != "value"+strconv.Itoa(i) {
			t.Errorf("failed to get a valid value: %s != \"value%d\"\n", v, i)
		}
	}

}
