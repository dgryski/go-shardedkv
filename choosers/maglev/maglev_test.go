package maglev

import "github.com/dgryski/go-shardedkv"

var _ shardedkv.Chooser = &Maglev{}
