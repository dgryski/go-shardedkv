package shardedkv

import (
	"strconv"
	"testing"

	kc "github.com/dgryski/go-shardedkv/choosers/ketama"
	st "github.com/dgryski/go-shardedkv/storage/memory"
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

	var migrationBuckets []string

	for i := nShards; i < nShards*2; i++ {
		label := "test_shard" + strconv.Itoa(i)
		migrationBuckets = append(migrationBuckets, label)
		backend := st.New()
		shards = append(shards, Shard{Name: label, Backend: backend})
		kv.AddShard(label, backend)
	}

	migration := kc.New()
	migration.SetBuckets(migrationBuckets)

	kv.BeginMigration(migration)

	// make sure requesting still works
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

	// make sure setting still works
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

	// and that deleting removes from both during a migration
	for i := 0; i < nElements; i++ {
		kv.Delete("test" + strconv.Itoa(i))
	}

	for i := 0; i < nElements; i++ {
		k := "test" + strconv.Itoa(i)

		_, ok, _ := kv.Get(k)
		if ok {
			t.Errorf("got a key that shouldn't have been there")
		}
	}

	// set the keys again
	for i := 0; i < nElements; i++ {
		kv.Set("test"+strconv.Itoa(i), []byte("value"+strconv.Itoa(i)))
	}

	// end the migration[
	kv.EndMigration()

	// delete the old shards
	for i := 0; i < nShards; i++ {
		label := "test_shard" + strconv.Itoa(i)
		kv.DeleteShard(label)
	}

	// and make sure we can still get to the keys
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
