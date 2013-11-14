package rest

import (
	"github.com/dgryski/go-shardedkv/storagetest"
	"io/ioutil"
	"net/http"
	"strconv"
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
