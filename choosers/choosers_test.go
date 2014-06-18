package choosers

import (
	"fmt"
	"testing"

	"github.com/dgryski/go-shardedkv"
	"github.com/dgryski/go-shardedkv/choosers/chash"
	"github.com/dgryski/go-shardedkv/choosers/jump"
	"github.com/dgryski/go-shardedkv/choosers/ketama"
	"github.com/dgryski/go-spooky"
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

func BenchmarkKetama8(b *testing.B)   { benchmarkChooser(b, 8, ketama.New()) }
func BenchmarkKetama32(b *testing.B)  { benchmarkChooser(b, 32, ketama.New()) }
func BenchmarkKetama128(b *testing.B) { benchmarkChooser(b, 128, ketama.New()) }
func BenchmarkKetama512(b *testing.B) { benchmarkChooser(b, 512, ketama.New()) }

func BenchmarkChash8(b *testing.B)   { benchmarkChooser(b, 8, chash.New()) }
func BenchmarkChash32(b *testing.B)  { benchmarkChooser(b, 32, chash.New()) }
func BenchmarkChash128(b *testing.B) { benchmarkChooser(b, 128, chash.New()) }
func BenchmarkChash512(b *testing.B) { benchmarkChooser(b, 512, chash.New()) }

func BenchmarkJump8(b *testing.B)   { benchmarkChooser(b, 8, jump.New(spooky.Hash64)) }
func BenchmarkJump32(b *testing.B)  { benchmarkChooser(b, 32, jump.New(spooky.Hash64)) }
func BenchmarkJump128(b *testing.B) { benchmarkChooser(b, 128, jump.New(spooky.Hash64)) }
func BenchmarkJump512(b *testing.B) { benchmarkChooser(b, 512, jump.New(spooky.Hash64)) }
