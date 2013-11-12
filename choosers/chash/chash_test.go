package chash

import (
	"github.com/dgryski/go-shardedkv"
)

var _ shardedkv.Chooser = &CHash{}
