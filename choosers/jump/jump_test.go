package jump

import (
	"fmt"
	"github.com/dgryski/go-shardedkv"
	"testing"

	"hash/fnv"
)

func TestCarbonCRelayCompat(t *testing.T) {

	tests := []struct {
		metric string
		r0, r1 int
	}{
		{"foo", 1, 4},
		{"bar", 7, 0},
		{"baz", 1, 6},
		{"qux", 2, 3},
		{"zot", 4, 2},
		{"snorf", 2, 0},
	}

	var buckets []string
	rev := make(map[string]int)
	for i := 0; i < 8; i++ {
		b := fmt.Sprintf("192.168.%d.%d", i, 10+2*i)
		buckets = append(buckets, b)
		rev[b] = i
	}

	f := func(b []byte) uint64 {
		h := fnv.New64a()
		h.Write(b)
		return h.Sum64()
	}

	j := New(f)
	j.SetBuckets(buckets)

	for _, tt := range tests {
		got := j.ChooseReplicas(tt.metric, 2)
		if len(got) != 2 {
			t.Errorf("len(j.ChooseReplicas(%q, 2))=%d, want 2", tt.metric, len(got))
			continue
		}

		r0, ok0 := rev[got[0]]
		r1, ok1 := rev[got[1]]
		if !ok0 || r0 != tt.r0 || !ok1 || r1 != tt.r1 {
			t.Errorf("j.ChooseReplicas(%q)=%v, want [%s,%s] (ok0=%t ok1=%t)\n", tt.metric, got, buckets[tt.r0], buckets[tt.r1], ok0, ok1)
		}

	}
}

var _ shardedkv.Chooser = &Jump{}
