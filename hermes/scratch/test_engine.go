package main

import (
	"fmt"
	"os"

	"github.com/aadityabinodyadav/hermes/pkg/clock"
	"github.com/aadityabinodyadav/hermes/pkg/storage"
)

func main() {
	os.RemoveAll("/tmp/hermes_test_engine")
	
	hlc := clock.NewHLC("test")
	cfg := storage.DefaultConfig("/tmp/hermes_test_engine")
	
	engine, err := storage.Open(cfg, hlc)
	if err != nil {
		panic(err)
	}
	
	err = engine.Put("user:alice", []byte("1000"))
	fmt.Printf("Put err: %v\n", err)
	
	val, found, err := engine.Get("user:alice")
	fmt.Printf("Get: val=%q, found=%v, err=%v\n", string(val), found, err)
}
