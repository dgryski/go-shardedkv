package rest

import (
	"github.com/dgryski/go-shardedkv/storagetest"
	"net/http"
	"strconv"
	"testing"
)

var storage map[string][]byte

func handler(w http.ResponseWriter, r *http.Request) {
	k := r.URL.String()
	if r.Method == "GET" {
		v := storage[k]
		if v == nil {
			w.WriteHeader(http.StatusNotFound)
		} else {
			w.Write(storage[k])
		}
	} else if r.Method == "PUT" {
		storage[k] = []byte(k)
		w.WriteHeader(http.StatusOK)
	} else if r.Method == "DELETE" {
		v := storage[k]
		if v == nil {
			w.WriteHeader(http.StatusNotFound)
		} else {
			storage[k] = nil
			w.WriteHeader(http.StatusOK)
		}
	}
}

func startHttpListener(port int) {
	storage = make(map[string][]byte)
	http.HandleFunc("/", handler)
	http.ListenAndServe(":"+strconv.Itoa(port), nil)
}

func TestRest(t *testing.T) {
	go startHttpListener(9876)
	r := New("http://localhost:9876")
	storagetest.StorageTest(t, r)
}
