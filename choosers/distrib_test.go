package choosers

import (
	"encoding/binary"
	"flag"
	"fmt"
	"testing"

	"github.com/dgryski/go-shardedkv"
	"github.com/dgryski/go-shardedkv/choosers/chash"
	"github.com/dgryski/go-shardedkv/choosers/jump"
	"github.com/dgryski/go-shardedkv/choosers/ketama"
	"github.com/dgryski/go-shardedkv/choosers/maglev"
	"github.com/dgryski/go-shardedkv/choosers/mpc"
)

var checkDistribution = flag.Bool("checkDistribution", false, "check the distribution of the different choosers")

func testDistribution(t *testing.T, shards int, ch shardedkv.Chooser) {

	if !*checkDistribution {
		t.Skip("skipping distribution check")
	}

	var buckets []string
	for i := 0; i < shards; i++ {
		buckets = append(buckets, fmt.Sprintf("shard-%d", i))
	}

	ch.SetBuckets(buckets)

	hits := make(map[string]int)

	k := make([]byte, 8)
	for i := 0; i < shards*(1e4); i++ {
		binary.LittleEndian.PutUint64(k[:], uint64(i))
		hits[ch.Choose(string(k))]++
	}

	// t.Logf("hits=%v", hits)

	var total int
	var peak int

	for _, v := range hits {
		total += v
		if v > peak {
			peak = v
		}
	}

	avg := float64(total) / float64(shards)
	t.Logf("peak=%v avg=%v ratio=%v", peak, avg, float64(peak)/avg)
}

func TestDistributionKetama8(t *testing.T)    { testDistribution(t, 8, ketama.New()) }
func TestDistributionKetama32(t *testing.T)   { testDistribution(t, 32, ketama.New()) }
func TestDistributionKetama128(t *testing.T)  { testDistribution(t, 128, ketama.New()) }
func TestDistributionKetama512(t *testing.T)  { testDistribution(t, 512, ketama.New()) }
func TestDistributionKetama2048(t *testing.T) { testDistribution(t, 2048, ketama.New()) }
func TestDistributionKetama8192(t *testing.T) { testDistribution(t, 8192, ketama.New()) }

func TestDistributionChash8(t *testing.T)    { testDistribution(t, 8, chash.New()) }
func TestDistributionChash32(t *testing.T)   { testDistribution(t, 32, chash.New()) }
func TestDistributionChash128(t *testing.T)  { testDistribution(t, 128, chash.New()) }
func TestDistributionChash512(t *testing.T)  { testDistribution(t, 512, chash.New()) }
func TestDistributionChash2048(t *testing.T) { testDistribution(t, 2048, chash.New()) }
func TestDistributionChash8192(t *testing.T) { testDistribution(t, 8192, chash.New()) }

func TestDistributionMulti8(t *testing.T) { testDistribution(t, 8, mpc.New(siphash64seed, seeds, 21)) }
func TestDistributionMulti32(t *testing.T) {
	testDistribution(t, 32, mpc.New(siphash64seed, seeds, 21))
}
func TestDistributionMulti128(t *testing.T) {
	testDistribution(t, 128, mpc.New(siphash64seed, seeds, 21))
}
func TestDistributionMulti512(t *testing.T) {
	testDistribution(t, 512, mpc.New(siphash64seed, seeds, 21))
}
func TestDistributionMulti2048(t *testing.T) {
	testDistribution(t, 2048, mpc.New(siphash64seed, seeds, 21))
}
func TestDistributionMulti8192(t *testing.T) {
	testDistribution(t, 8192, mpc.New(siphash64seed, seeds, 21))
}

func TestDistributionJump8(t *testing.T)    { testDistribution(t, 8, jump.New(siphash64)) }
func TestDistributionJump32(t *testing.T)   { testDistribution(t, 32, jump.New(siphash64)) }
func TestDistributionJump128(t *testing.T)  { testDistribution(t, 128, jump.New(siphash64)) }
func TestDistributionJump512(t *testing.T)  { testDistribution(t, 512, jump.New(siphash64)) }
func TestDistributionJump2048(t *testing.T) { testDistribution(t, 2048, jump.New(siphash64)) }
func TestDistributionJump8192(t *testing.T) { testDistribution(t, 8192, jump.New(siphash64)) }

func TestDistributionMaglev8(t *testing.T)   { testDistribution(t, 8, maglev.New()) }
func TestDistributionMaglev32(t *testing.T)  { testDistribution(t, 32, maglev.New()) }
func TestDistributionMaglev128(t *testing.T) { testDistribution(t, 128, maglev.New()) }
func TestDistributionMaglev512(t *testing.T) { testDistribution(t, 512, maglev.New()) }
