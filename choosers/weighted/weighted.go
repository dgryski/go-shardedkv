// Packaged weighted implements weighted shards for a SharedKV chooser
package weighted

import (
	"fmt"
	"strings"

	"github.com/dgryski/go-shardedkv"
)

type Weighted struct {
	chooser shardedkv.Chooser

	lookup func(string) int

	buckets []string
}

func New(chooser shardedkv.Chooser, lookup func(string) int) *Weighted {
	return &Weighted{
		lookup:  lookup,
		chooser: chooser,
	}
}

func (w *Weighted) SetBuckets(buckets []string) error {

	var mbuckets []string

	// created weighted shard array
	for _, b := range buckets {
		weight := w.lookup(b)
		for j := 0; j < weight; j++ {
			name := fmt.Sprintf("%s#%d", b, j)
			mbuckets = append(mbuckets, name)
		}
	}

	w.chooser.SetBuckets(mbuckets)
	w.buckets = buckets

	return nil
}

func (w *Weighted) Choose(key string) string {
	m := w.chooser.Choose(key)
	l := strings.LastIndex(m, "#")
	return m[:l]
}

func (w *Weighted) Buckets() []string {
	return w.buckets
}
