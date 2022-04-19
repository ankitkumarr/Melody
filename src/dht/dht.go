package dht

import (
	"Melody/chord"
	"Melody/common"
	"fmt"
	"io"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

const RpcTimeout = 5 * time.Second
const RpcPath = "/_goRPC_HashTable"
const RpcDebugPath = "/debug/rpc_HashTable"

type HashTableNode struct {
	id      string
	data    map[string]interface{}
	chord   *chord.Node
	mu      sync.Mutex
	address string
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

func Make(ch *chord.Node, address string) *HashTableNode {
	htNode := HashTableNode{}
	htNode.id = strconv.Itoa(os.Getuid())
	htNode.chord = ch
	htNode.data = make(map[string]interface{})
	htNode.address = address

	newServer := rpc.NewServer()
	newServer.Register(&htNode)
	newServer.HandleHTTP(RpcPath, RpcDebugPath)

	htNode.setupHttpRoutes()

	return &htNode
}

//
// Sets up HTTP GET routes for get and put requests to the hashTable
// This helps with e2e testing and simulating hash table updates
// This would not be part of a released application
//
func (hn *HashTableNode) setupHttpRoutes() {
	get := func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		key := query["key"][0]
		// retrieved := ""
		retrieved := hn.Get(key).(string)
		io.WriteString(w, fmt.Sprintf("Retrieved value for key %v: %v\n", key, retrieved))
		// fmt.Println("GET params were:", r.URL.Query())
	}
	put := func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		key := query["key"][0]
		value := query["value"][0]
		hn.Put(key, value)
		io.WriteString(w, fmt.Sprintf("Add key %v: %v to the DHT\n", key, value))
		// fmt.Println("GET params were:", r.URL.Query())
	}

	http.HandleFunc("/dhtget", get)
	http.HandleFunc("/dhtput", put)
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

		keyHashed := common.KeyHash(key)
		if hn.chord.IsMyKey(keyHashed) {
			hn.mu.Unlock()
			return nil
		}

		hn.mu.Unlock()
		args := GetValueArgs{}
		args.Id = strconv.Itoa(os.Getuid())
		args.Key = key

		add, _ := hn.chord.Lookup(keyHashed)

		// My key
		// Right now chord does not implement IsMyKey
		// TODO: Either remove IsMyKey or remove this
		if add == hn.address {
			hn.mu.Lock()
			defer hn.mu.Unlock()
			return hn.data[key]
		}

		reply := GetValueReply{}
		ok := common.Call(add, RpcPath, "HashTableNode.GetValue", &args, &reply, RpcTimeout)
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
	keyHashed := common.KeyHash(key)
	for {
		hn.mu.Lock()
		if _, ok := hn.data[key]; ok || hn.chord.IsMyKey(keyHashed) {
			hn.data[key] = value
			hn.mu.Unlock()
			return true
		}
		hn.mu.Unlock()

		args := PutValueArgs{}
		args.Id = strconv.Itoa(os.Getuid())
		args.Key = key
		args.Value = value

		add, _ := hn.chord.Lookup(keyHashed)

		// My key
		// Right now chord does not implement IsMyKey
		// TODO: Either remove IsMyKey or remove this
		if add == hn.address {
			hn.mu.Lock()
			defer hn.mu.Unlock()
			hn.data[key] = value
			return true
		}

		reply := PutValueReply{}
		ok := common.Call(add, RpcPath, "HashTableNode.PutValue", &args, &reply, RpcTimeout)
		if ok && reply.Success {
			return true
		}
	}
}

//
// RPC method called by other Distributed Hash table nodes to get a key
//
func (hn *HashTableNode) GetValue(args *GetValueArgs, reply *GetValueReply) error {
	hn.mu.Lock()
	defer hn.mu.Unlock()
	keyHashed := common.KeyHash(args.Key)
	reply.Id = args.Id
	if v, ok := hn.data[args.Key]; ok {
		reply.Value = v
		reply.Success = true
	} else if hn.chord.IsMyKey(keyHashed) {
		// This means that even though I don't have the key,
		// this key should be mine. So, we can return an empty response.
		reply.Success = true
		reply.Value = nil
	} else {
		reply.Success = false
		reply.Err = fmt.Sprintf("Hash Table Node %v does not contain Key %v.", hn.id, args.Key)
	}
	return nil
}

//
// RPC method called by other Distributed Hash table nodes to put a value mapped to a key
//
func (hn *HashTableNode) PutValue(args *PutValueArgs, reply *PutValueReply) error {
	hn.mu.Lock()
	defer hn.mu.Unlock()
	reply.Id = args.Id
	keyHashed := common.KeyHash(args.Key)
	if _, ok := hn.data[args.Key]; ok {
		hn.data[args.Key] = args.Value
		reply.Success = true
	} else if hn.chord.IsMyKey(keyHashed) {
		// This means that even though I don't have the key,
		// this key should be mine.
		hn.data[args.Key] = args.Value
		reply.Success = true
	} else {
		reply.Success = false
		reply.Err = fmt.Sprintf("Hash Table Node %v is not responsible for Key %v.", hn.id, args.Key)
	}
	return nil
}
