package shardedkv

import (
	"sync"
)

type Storage interface {
	Get(key string) ([]byte, bool, error)
	Set(key string, value []byte) error
	Delete(key string) (bool, error)
	ResetConnection(key string) error
}

type KVStore struct {
	continuum Chooser
	storages  map[string]Storage
	migration Chooser

	// we avoid holding the lock during a call to a storage engine, which may block
	mu sync.Mutex
}

type Chooser interface {
	Choose(key string) string
}

func (kv *KVStore) Get(key string) ([]byte, bool, error) {

	var storage Storage
	var migStorage Storage

	kv.mu.Lock()

	if kv.migration != nil {
		shard := kv.migration.Choose(string(key))
		migStorage = kv.storages[shard]
	}
	shard := kv.continuum.Choose(string(key))
	storage = kv.storages[shard]

	kv.mu.Unlock()

	if migStorage != nil {
		val, ok, err := migStorage.Get(key)
		if err != nil {
			return nil, false, err
		}

		if ok {
			return val, ok, nil
		}
	}

	return storage.Get(key)
}

func (kv *KVStore) Set(key string, val []byte) error {

	var shard string

	kv.mu.Lock()

	if kv.migration != nil {
		shard = kv.migration.Choose(string(key))
	} else {
		shard = kv.continuum.Choose(string(key))
	}
	storage := kv.storages[shard]

	kv.mu.Unlock()

	return storage.Set(key, val)
}

func (kv *KVStore) Delete(key string) (bool, error) {

	var storage Storage
	var migStorage Storage

	kv.mu.Lock()

	if kv.migration != nil {
		shard := kv.migration.Choose(string(key))
		migStorage = kv.storages[shard]
	}
	shard := kv.continuum.Choose(string(key))
	storage = kv.storages[shard]

	kv.mu.Unlock()

	var migOk bool
	if migStorage != nil {
		var err error
		migOk, err = migStorage.Delete(key)
		if err != nil {
			return false, err
		}
	}

	ok, err := storage.Delete(key)
	// true if we deleted it from at least one of the shards
	return (ok || migOk), err
}

func (kv *KVStore) ResetConnection(key string) error {

	var storage Storage
	var migStorage Storage

	kv.mu.Lock()

	if kv.migration != nil {
		shard := kv.migration.Choose(string(key))
		migStorage = kv.storages[shard]
	}
	shard := kv.continuum.Choose(string(key))
	storage = kv.storages[shard]

	kv.mu.Unlock()

	if migStorage != nil {
		err := migStorage.ResetConnection(key)
		if err != nil {
			return err
		}
	}
	return storage.ResetConnection(key)
}

func (kv *KVStore) AddShard(shard string, storage Storage) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.storages[shard] = storage
}

func (kv *KVStore) DeleteShard(shard string, storage Storage) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.storages, shard)
}

func (kv *KVStore) BeginMigration(continuum Chooser) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.migration = continuum
}

func (kv *KVStore) EndMigration() {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.continuum = kv.migration
	kv.migration = nil
}
