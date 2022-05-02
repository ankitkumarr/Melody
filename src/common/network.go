package common

import (
	"fmt"
	"net/rpc"
	"time"
)

//
// send an RPC request to the given address, wait for the response.
// returns false if something goes wrong.
//
func Call(address string, path string, rpcname string, args interface{}, reply interface{}, timeout time.Duration) bool {
	callCh := make(chan bool, 1)

	go call(address, path, rpcname, args, reply, callCh)

	select {
	case <-time.After(timeout):
		return false
	case ok := <-callCh:
		return ok
	}
}

func call(address string, path string, rpcname string, args interface{}, reply interface{}, ch chan bool) {
	c, err := rpc.DialHTTPPath("tcp", address, path)
	if err != nil {
		// log.Fatalf("Failed to connect to server with address: %v. %v", address, err)
		fmt.Println(err)
		ch <- false
		return
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		ch <- true
	} else {
		fmt.Println(err)
		ch <- false
	}
}
