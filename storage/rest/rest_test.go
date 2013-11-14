package rest

import (
	"github.com/dgryski/go-shardedkv/storagetest"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
)

var storage map[string][]byte

func handler(w http.ResponseWriter, r *http.Request) {
	k := r.URL.String()
	switch r.Method {
	case "GET":
		v := storage[k]
		if v == nil {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.Write(storage[k])
		}
	case "PUT":
		val, _ := ioutil.ReadAll(r.Body)
		storage[k] = val
		w.WriteHeader(http.StatusOK)
	case "DELETE":
		v := storage[k]
		if v == nil {
			w.WriteHeader(http.StatusNotFound)
		} else {
			storage[k] = nil
			w.WriteHeader(http.StatusOK)
		}
	}
}

func TestRest(t *testing.T) {
	storage = make(map[string][]byte)
	ts := httptest.NewServer(http.HandlerFunc(handler))
	defer ts.Close()
	r := New(ts.URL)
	storagetest.StorageTest(t, r)
}
