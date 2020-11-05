package choosers

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/dgryski/go-metro"
	"github.com/dgryski/go-shardedkv"
	"github.com/dgryski/go-shardedkv/choosers/chash"
	"github.com/dgryski/go-shardedkv/choosers/jump"
	"github.com/dgryski/go-shardedkv/choosers/ketama"
	"github.com/dgryski/go-shardedkv/choosers/maglev"
	"github.com/dgryski/go-shardedkv/choosers/mpc"
	"github.com/dgryski/go-shardedkv/choosers/rendezvous"
)

func benchmarkChooser(b *testing.B, shards int, ch shardedkv.Chooser) {

	var buckets []string
	for i := 0; i < shards; i++ {
		buckets = append(buckets, fmt.Sprintf("shard-%d", i))
	}

	ch.SetBuckets(buckets)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		ch.Choose(buckets[i&(shards-1)])
	}
}

func benchmarkOne(b *testing.B, newch func() shardedkv.Chooser) {
	for _, size := range []int{8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192} {
		b.Run(strconv.Itoa(size), func(b *testing.B) { benchmarkChooser(b, size, newch()) })
	}
}

func BenchmarkKetama(b *testing.B) { benchmarkOne(b, func() shardedkv.Chooser { return ketama.New() }) }
func BenchmarkCHash(b *testing.B)  { benchmarkOne(b, func() shardedkv.Chooser { return chash.New() }) }
func BenchmarkMulti(b *testing.B) {
	benchmarkOne(b, func() shardedkv.Chooser { return mpc.New(hash64seed, seeds, 21) })
}
func BenchmarkJump(b *testing.B) {
	benchmarkOne(b, func() shardedkv.Chooser { return jump.New(hash64) })
}
func BenchmarkRendezvous(b *testing.B) {
	benchmarkOne(b, func() shardedkv.Chooser { return rendezvous.New() })
}

// lousy seeds
var seeds = [2]uint64{1, 2}

func hash64seed(b []byte, s uint64) uint64 { return uint64(metro.Hash64(b, s)) }

func hash64(b []byte) uint64 { return metro.Hash64(b, 0) }

func BenchmarkMaglev(b *testing.B) {
	for _, size := range []int{8, 16, 32, 64, 128, 256, 512} {
		b.Run(strconv.Itoa(size), func(b *testing.B) { benchmarkChooser(b, size, maglev.New()) })
	}
}
