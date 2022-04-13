package network

import (
	"fmt"
	"log"
	"net/rpc"
)

//
// send an RPC request to the given address, wait for the response.
// returns false if something goes wrong.
//
func Call(address string, rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		log.Fatalf("Failed to connect to server with address: %v. %v", address, err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
