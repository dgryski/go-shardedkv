package choosers

import (
	"fmt"
	"testing"

	"github.com/dchest/siphash"
	"github.com/dgryski/go-shardedkv"
	"github.com/dgryski/go-shardedkv/choosers/chash"
	"github.com/dgryski/go-shardedkv/choosers/jump"
	"github.com/dgryski/go-shardedkv/choosers/ketama"
	"github.com/dgryski/go-shardedkv/choosers/mpc"
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

func BenchmarkKetama8(b *testing.B)    { benchmarkChooser(b, 8, ketama.New()) }
func BenchmarkKetama32(b *testing.B)   { benchmarkChooser(b, 32, ketama.New()) }
func BenchmarkKetama128(b *testing.B)  { benchmarkChooser(b, 128, ketama.New()) }
func BenchmarkKetama512(b *testing.B)  { benchmarkChooser(b, 512, ketama.New()) }
func BenchmarkKetama2048(b *testing.B) { benchmarkChooser(b, 2048, ketama.New()) }
func BenchmarkKetama8192(b *testing.B) { benchmarkChooser(b, 8192, ketama.New()) }

func BenchmarkChash8(b *testing.B)    { benchmarkChooser(b, 8, chash.New()) }
func BenchmarkChash32(b *testing.B)   { benchmarkChooser(b, 32, chash.New()) }
func BenchmarkChash128(b *testing.B)  { benchmarkChooser(b, 128, chash.New()) }
func BenchmarkChash512(b *testing.B)  { benchmarkChooser(b, 512, chash.New()) }
func BenchmarkChash2048(b *testing.B) { benchmarkChooser(b, 2048, chash.New()) }
func BenchmarkChash8192(b *testing.B) { benchmarkChooser(b, 8192, chash.New()) }

func siphash64seed(b []byte, s uint64) uint64 { return uint64(siphash.Hash(0, s, b)) }

// lousy seeds
var seeds = [2]uint64{1, 2}

func BenchmarkMulti8(b *testing.B)    { benchmarkChooser(b, 8, mpc.New(siphash64seed, seeds, 21)) }
func BenchmarkMulti32(b *testing.B)   { benchmarkChooser(b, 32, mpc.New(siphash64seed, seeds, 21)) }
func BenchmarkMulti128(b *testing.B)  { benchmarkChooser(b, 128, mpc.New(siphash64seed, seeds, 21)) }
func BenchmarkMulti512(b *testing.B)  { benchmarkChooser(b, 512, mpc.New(siphash64seed, seeds, 21)) }
func BenchmarkMulti2048(b *testing.B) { benchmarkChooser(b, 2048, mpc.New(siphash64seed, seeds, 21)) }
func BenchmarkMulti8192(b *testing.B) { benchmarkChooser(b, 8192, mpc.New(siphash64seed, seeds, 21)) }

func siphash64(b []byte) uint64 { return siphash.Hash(0, 0, b) }

func BenchmarkJump8(b *testing.B)    { benchmarkChooser(b, 8, jump.New(siphash64)) }
func BenchmarkJump32(b *testing.B)   { benchmarkChooser(b, 32, jump.New(siphash64)) }
func BenchmarkJump128(b *testing.B)  { benchmarkChooser(b, 128, jump.New(siphash64)) }
func BenchmarkJump512(b *testing.B)  { benchmarkChooser(b, 512, jump.New(siphash64)) }
func BenchmarkJump2048(b *testing.B) { benchmarkChooser(b, 2048, jump.New(siphash64)) }
func BenchmarkJump8192(b *testing.B) { benchmarkChooser(b, 8192, jump.New(siphash64)) }
