package jump

import (
	"github.com/dgryski/go-shardedkv"
)

var _ shardedkv.Chooser = &Jump{}
