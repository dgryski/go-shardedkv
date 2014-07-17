// Package rest provides a key-value store backed by a RESTful API
package rest

import (
	"bytes"
	"errors"
	"io/ioutil"
	"net/http"
)

type Storage struct {
	base   string
	client *http.Client
}

// New returns a rest-backed storage at the given base URL using the default HTTP client
func New(base string) *Storage {
	return NewWithClient(base, http.DefaultClient)
}

// New returns a rest-backed storage at the given base URL using a custom HTTP client
func NewWithClient(base string, client *http.Client) *Storage {
	return &Storage{
		base:   base,
		client: client,
	}
}

func (s *Storage) Get(key string) ([]byte, bool, error) {

	resp, err := s.client.Get(s.base + "/" + key)
	if err != nil || resp.StatusCode >= 400 {
		// TODO(dgryski): handle 404 vs. other errors
		return nil, false, err
	}

	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, false, err
	}

	return body, true, nil
}

func (s *Storage) Set(key string, val []byte) error {

	req, err := http.NewRequest("PUT", s.base+"/"+key, bytes.NewReader(val))
	if err != nil {
		return err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()

	// any status code 200..299 is "success", so fail on anything else
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return errors.New(http.StatusText(resp.StatusCode))
	}

	return nil
}

func (s *Storage) Delete(key string) (bool, error) {

	req, err := http.NewRequest("DELETE", s.base+"/"+key, nil)
	if err != nil {
		return false, err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return false, err
	}
	resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// XXX this is necessary to conform to the actual behaviour of other storage engines
		return false, nil
	} else if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return false, errors.New(http.StatusText(resp.StatusCode))
	}

	// we can't know if the file was there -- DELETE is defined to be idempotent and return OK regardless
	return true, nil
}

func (s *Storage) ResetConnection(key string) error {
	// FIXME(dgryski): Try to clean out cached keep-alive connections the client holds?
	return nil
}
