package main

import (
	"fmt"
	"github.com/aadityabinodyadav/hermes/pkg/storage"
)

func main() {
	cmd := storage.NewPutCommand("user:alice", []byte("1000"))
	
	err := storage.ValidateCommand(cmd)
	fmt.Printf("Validate: %v\n", err)
	
	key, val, del := storage.DecodeCommand(cmd)
	fmt.Printf("Decode: key=%q, val=%q, del=%v\n", key, string(val), del)
}
