package shardedkv

import (
	"sync"
)

// Storage is a key-value storage backend
type Storage interface {
	// Get returns the value for a given key and a bool indicating if the key was present
	Get(key string) ([]byte, bool, error)
	// Set sets the value for key
	Set(key string, value []byte) error
	// Delete removes a key from the storage, and returns a bool indicating if the key was found
	Delete(key string) (bool, error)
	// ResetConnection reinitializes the connection for the shard responsible for a key
	ResetConnection(key string) error
}

// KVStore is a sharded key-value store
type KVStore struct {
	continuum Chooser
	storages  map[string]Storage
	migration Chooser

	// we avoid holding the lock during a call to a storage engine, which may block
	mu sync.Mutex
}

// Chooser maps keys to shards
type Chooser interface {
	// SetBuckets sets the list of known buckets from which the chooser should select
	SetBuckets([]string) error
	// Choose returns a bucket for a given key
	Choose(key string) string
	// Buckets returns the list of known buckets
	Buckets() []string
}

// Shard is a named storage backend
type Shard struct {
	Name    string
	Backend Storage
}

// New returns a KVStore that uses chooser to shard the keys across the provided shards
func New(chooser Chooser, shards []Shard) *KVStore {
	var buckets []string
	kv := &KVStore{
		continuum: chooser,
		storages:  make(map[string]Storage),
		// what about migration?
	}
	for _, shard := range shards {
		buckets = append(buckets, shard.Name)
		kv.AddShard(shard.Name, shard.Backend)
	}
	chooser.SetBuckets(buckets)
	return kv
}

// Get implements Storage.Get()
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

// Set implements Storage.Set()
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

// Delete implements Storage.Delete()
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

// ResetConnection implements Storage.ResetConnection()
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

// AddShard adds a shard from the list of known shards
func (kv *KVStore) AddShard(shard string, storage Storage) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.storages[shard] = storage
}

// DeleteShard removes a shard from the list of known shards
func (kv *KVStore) DeleteShard(shard string, storage Storage) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	delete(kv.storages, shard)
}

// BeginMigration begins a continuum migration.  All the shards in the new
// continuum must already be known to the KVStore via AddShard().
func (kv *KVStore) BeginMigration(continuum Chooser) {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.migration = continuum
}

// EndMigration ends a continuum migration and marks the migration continuum
// as the new primary
func (kv *KVStore) EndMigration() {

	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.continuum = kv.migration
	kv.migration = nil
}
