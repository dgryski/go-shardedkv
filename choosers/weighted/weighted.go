// Packaged weighted implements weighted shards for a SharedKV chooser
package weighted

import (
	"fmt"

	"github.com/dgryski/go-shardedkv"
)

type Weighted struct {
	chooser shardedkv.Chooser

	lookup func(string) int

	buckets []string
	smap    map[string]string
}

func New(chooser shardedkv.Chooser, lookup func(string) int) *Weighted {
	return &Weighted{
		lookup:  lookup,
		chooser: chooser,
	}
}

func (w *Weighted) SetBuckets(buckets []string) error {

	smap := make(map[string]string)

	var mbuckets []string

	// created weighted shard array
	for _, b := range buckets {
		weight := w.lookup(b)
		for j := 0; j < weight; j++ {
			name := fmt.Sprintf("%s#%d", b, j)
			mbuckets = append(mbuckets, name)
			smap[name] = b
		}
	}

	w.chooser.SetBuckets(mbuckets)
	w.buckets = buckets
	w.smap = smap

	return nil
}

func (w *Weighted) Choose(key string) string {
	m := w.chooser.Choose(key)
	return w.smap[m]

}

func (w *Weighted) Buckets() []string {
	return w.buckets
}
