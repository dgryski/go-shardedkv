package rendezvous

import "github.com/dgryski/go-shardedkv"

var _ shardedkv.Chooser = &Rendezvous{}
