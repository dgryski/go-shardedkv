package fs

import (
	"io/ioutil"
	"os"
	"path"
)

type Storage struct {
	dir string
}

func New(dir string) *Storage {
	return &Storage{
		dir: dir,
	}
}

func (s *Storage) Get(key string) ([]byte, bool, error) {

	val, err := ioutil.ReadFile(path.Join(s.dir, key))
	if os.IsNotExist(err) {
		return nil, false, nil
	}

	if err != nil {
		return nil, false, err
	}

	return val, true, nil
}

func (s *Storage) Set(key string, val []byte) error {
	err := ioutil.WriteFile(path.Join(s.dir, key), val, 0777)
	return err
}

func (s *Storage) Delete(key string) (bool, error) {

	err := os.Remove(path.Join(s.dir, key))

	if os.IsNotExist(err) {
		return false, nil
	}

	if err != nil {
		return false, err
	}

	return true, nil
}

func (s *Storage) ResetConnection(key string) error {
	return nil
}
