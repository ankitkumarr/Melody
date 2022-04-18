package dht

import (
	"fmt"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"

	"Melody/chord"

	network "Melody/common"
)

const RpcTimeout = 5 * time.Second

type HashTableNode struct {
	id    string
	data  map[string]interface{}
	chord *chord.Node
	mu    sync.Mutex
}

type GetValueArgs struct {
	Id  string
	Key string
}

type GetValueReply struct {
	Id      string
	Value   interface{}
	Err     string
	Success bool
}

type PutValueArgs struct {
	Id    string
	Key   string
	Value interface{}
}

type PutValueReply struct {
	Id      string
	Err     string
	Success bool
}

func Make(ch *chord.Node) HashTableNode {
	htNode := HashTableNode{}
	htNode.id = strconv.Itoa(os.Getuid())
	htNode.chord = ch
	htNode.data = make(map[string]interface{})

	rpc.Register(htNode)

	return htNode
}

//
// Called by the application to get a key from the DHT
//
func (hn *HashTableNode) Get(key string) interface{} {
	// TODO: Currently this will infinitely attempt to find the key
	// This needs to be fixed to know when key is not present, and only retry a specific number of iterations.
	for {
		hn.mu.Lock()
		if v, ok := hn.data[key]; ok {
			hn.mu.Unlock()
			return v
		}

		if hn.chord.IsMyKey(key) {
			hn.mu.Unlock()
			return nil
		}

		hn.mu.Unlock()
		args := GetValueArgs{}
		args.Id = strconv.Itoa(os.Getuid())
		args.Key = key

		add := hn.chord.Lookup(key)
		reply := GetValueReply{}
		ok := network.Call(add, "HashTableNode.GetValue", &args, &reply, RpcTimeout)
		if ok && reply.Success {
			return reply.Value
		}
	}
}

//
// Called by the application to put a key value in the DHT
//
func (hn *HashTableNode) Put(key string, value interface{}) bool {
	// TODO: Currently this will infinitely attempt to find the key
	// This needs to be fixed to know when key is not present, and only retry a specific number of iterations.
	for {
		hn.mu.Lock()
		if _, ok := hn.data[key]; ok || hn.chord.IsMyKey(key) {
			hn.data[key] = value
			hn.mu.Unlock()
			return true
		}
		hn.mu.Unlock()

		args := PutValueArgs{}
		args.Id = strconv.Itoa(os.Getuid())
		args.Key = key
		args.Value = value

		add := hn.chord.Lookup(key)
		reply := PutValueReply{}
		ok := network.Call(add, "HashTableNode.PutValue", &args, &reply, RpcTimeout)
		if ok && reply.Success {
			return true
		}
	}
}

//
// RPC method called by other Distributed Hash table nodes to get a key
//
func (hn *HashTableNode) GetValue(args *GetValueArgs, reply *GetValueReply) {
	hn.mu.Lock()
	defer hn.mu.Unlock()
	reply.Id = args.Id
	if v, ok := hn.data[args.Key]; ok {
		reply.Value = v
		reply.Success = true
	} else if hn.chord.IsMyKey(args.Key) {
		// This means that even though I don't have the key,
		// this key should be mine. So, we can return an empty response.
		reply.Success = true
		reply.Value = nil
	} else {
		reply.Success = false
		reply.Err = fmt.Sprintf("Hash Table Node %v does not contain Key %v.", hn.id, args.Key)
	}
}

//
// RPC method called by other Distributed Hash table nodes to put a value mapped to a key
//
func (hn *HashTableNode) PutValue(args *PutValueArgs, reply *PutValueReply) {
	hn.mu.Lock()
	defer hn.mu.Unlock()
	reply.Id = args.Id
	if _, ok := hn.data[args.Key]; ok {
		hn.data[args.Key] = args.Value
		reply.Success = true
	} else if hn.chord.IsMyKey(args.Key) {
		// This means that even though I don't have the key,
		// this key should be mine.
		hn.data[args.Key] = args.Value
		reply.Success = true
	} else {
		reply.Success = false
		reply.Err = fmt.Sprintf("Hash Table Node %v is not responsible for Key %v.", hn.id, args.Key)
	}
}
