package ketama

import (
	"github.com/dgryski/go-ketama"
)

type Ketama struct {
	k ketama.Continuum
}

func New(servers ...string) (*Ketama, error) {

	b := make([]ketama.Bucket, len(servers))

	for i, s := range servers {
		b[i].Label = s
		b[i].Weight = 1
	}

	ket, err := ketama.New(b)
	if err != nil {
		return nil, err
	}

	return &Ketama{k: ket}, nil
}

func (k *Ketama) Choose(key string) string { return k.k.Hash(key) }
