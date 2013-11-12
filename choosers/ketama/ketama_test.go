package ketama

import (
	"github.com/dgryski/go-shardedkv"
)

var _ shardedkv.Chooser = &Ketama{}
