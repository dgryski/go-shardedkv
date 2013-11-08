package redis

// TODO: http://godoc.org/git.tideland.biz/godm/redis has support for HASH

import (
	"github.com/garyburd/redigo/redis"
)

type Storage struct {
	addr string
	r    redis.Conn
}

func New(addr string) (*Storage, error) {

	conn, err := redis.Dial("tcp", addr)

	if err != nil {
		return nil, err
	}

	return &Storage{r: conn}, nil
}

func (s *Storage) Get(key string) ([]byte, bool, error) {

	repl, err := s.r.Do("GET", key)

	if repl == nil {
		return nil, false, err
	}

	val, err := redis.Bytes(repl, err)

	return val, true, err
}

func (s *Storage) Set(key string, val []byte) error {
	_, err := s.r.Do("SET", key, val)
	return err
}

func (s *Storage) Delete(key string) (bool, error) {
	repl, err := s.r.Do("DEL", key)
	val, err := redis.Int(repl, err)
	return val == 1, err
}

func (s *Storage) ResetConnection(key string) error {
	s.r.Close()

	var err error
	s.r, err = redis.Dial("tcp", s.addr)

	return err
}
