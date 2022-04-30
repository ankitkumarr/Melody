package dht

import (
	"Melody/chord"
	"Melody/common"
	"fmt"
	"io"
	"log"
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
	id          string
	data        HashData
	replicas    map[int]*ReplicaInfo
	chord       *chord.Node
	mu          sync.Mutex
	address     string
	successors  []chord.NodeInfo
	predecessor chord.NodeInfo
}

type NodeId struct {
	Uid       string
	HashedUid int
}

type ReplicaInfo struct {
	Node    NodeId
	Replica HashData
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

type ReplicatePutArgs struct {
	DhtId NodeId
	Key   string
	Value interface{}
}

type ReplicatePutReply struct {
	Success bool
	Err     string
}

type StoreReplicasArgs struct {
	DhtId    NodeId
	Replicas map[string]interface{}
}

type StoreReplicasReply struct {
	Success bool
	Err     string
}

type MoveReplicasArgs struct {
	FromId NodeId
	ToId   NodeId
	Data   map[string]interface{} // This is sent mostly for verification purpose
}

type MoveReplicasReply struct {
	Success bool
	Err     string
}

type GetMyDataArgs struct {
	DhtId NodeId
}

type GetMyDataReply struct {
	Data    map[string]interface{}
	Success bool
	Err     string
}

func Make(ch *chord.Node, address string) *HashTableNode {
	htNode := HashTableNode{}
	htNode.id = strconv.Itoa(os.Getuid())
	htNode.chord = ch
	htNode.address = address

	htNode.data = HashData{}
	htNode.data.Data = make(map[string]interface{})

	htNode.replicas = make(map[int]*ReplicaInfo)

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
// This should be called when a new replica server (chord successor) is added for a give node.
// That Node should call this RPC on its successor and provide the new Data along with its ID.
//
func (hn *HashTableNode) StoreReplicas(args *StoreReplicasArgs, reply *StoreReplicasReply) error {
	// Currently not worried about duplicate keys
	hn.mu.Lock()
	defer hn.mu.Unlock()
	hashedId := common.KeyHash(args.DhtId.Uid)

	// TODO: If we want to actually be careful about the size of the replicas,
	// We need to check if the Data in store replica call was already there in some other replicas,
	// if so, we need to perform a move operation
	// TODO: Otherwise, if a new Node A joins and takes some keys off of Node B, we don't just want to
	// add A's newly acquired data, we need to get rid of B's old data too.

	// Helps eliminate a tiny portion of malicious intent
	if hashedId != args.DhtId.HashedUid {
		reply.Success = false
		reply.Err = "The computed hash ID did not match the provided hash ID"
		return nil
	}

	if _, ok := hn.replicas[hashedId]; !ok {
		hd := HashData{}
		hd.Data = make(map[string]interface{})
		// hd.HashToKey = make([]HashKey, 0)
		ri := ReplicaInfo{Replica: hd, Node: args.DhtId}
		hn.replicas[hashedId] = &ri
	}

	hn.replicas[hashedId].Replica.AddRange(args.Replicas)

	// TODO: Evict smallest replica if number of replicas is more than desired.
	// Find the smallest replica that is greater than me. In case my ID is close to boundary.
	// If none greater than me, evict the smallest replica among all.

	reply.Success = true
	return nil
}

// func (hn *HashTableNode) RemoveReplicas(args *RemoveReplicasArgs, reply *RemoveReplicasReply) error {
// 	delete(hn.replicas, args.DhtId)
// 	reply.Success = true
// 	return nil
// }

func (hn *HashTableNode) MoveReplicas(args *MoveReplicasArgs, reply *MoveReplicasReply) error {
	hn.mu.Lock()
	defer hn.mu.Unlock()

	// Helps eliminate some malicious intent
	fromHashed := common.KeyHash(args.FromId.Uid)
	if fromHashed != args.FromId.HashedUid {
		reply.Success = false
		reply.Err = "The FromId's computed hash ID did not match the provided hash ID."
		return nil
	}
	toHashed := common.KeyHash(args.ToId.Uid)
	if toHashed != args.ToId.HashedUid {
		reply.Success = false
		reply.Err = "The ToId's computed hash ID did not match the provided hash ID."
		return nil
	}

	// If we don't have replicas "from", for now we can just add the "To" replicas
	// We can revisit this if we address malicious nodes adding too much data later on.
	if _, ok := hn.replicas[args.FromId.HashedUid]; !ok {
		ri := ReplicaInfo{}
		ri.Node = args.ToId
		ri.Replica = HashData{}
		ri.Replica.AddRange(args.Data)
		reply.Success = true
		return nil
	}

	// TODO: Revisit if we should be so strict about the replica verification.
	// There's valid reasons that the replicas may not match. Maybe we should add
	// caluse to tolerate a certain % of mismatch. And more than that, we do not delete our old data.
	mismatch_count := 0

	oldReplicaData := hn.replicas[args.FromId.HashedUid].Replica.Data
	for k := range args.Data {
		// Not doing an equality check on the value, as that would be difficult for interfaces
		// Revisit if planning to deal with malicious entities all the way.
		if _, ok := oldReplicaData[k]; !ok {
			mismatch_count++
		}
	}

	// Allowing up to 10 mismatches just in case.
	if mismatch_count > 10 {
		reply.Success = false
		reply.Err = "Found more than 10 mismatches between the data provided and the replica that was stored in my Node"
	}

	if _, ok := hn.replicas[args.ToId.HashedUid]; !ok {
		newri := ReplicaInfo{}
		newri.Node = args.ToId
		newri.Replica = HashData{}
		hn.replicas[args.ToId.HashedUid] = &newri
	}

	hn.replicas[args.ToId.HashedUid].Replica.AddRange(args.Data)
	reply.Success = true
	return nil
}

//
// Get all the data that I should own.
// This should be called by a new Node joining the DHT.
//
func (hn *HashTableNode) GetMyData(args *GetMyDataArgs, reply *GetMyDataReply) error {
	hn.mu.Lock()
	defer hn.mu.Unlock()

	// Helps eliminate some malicious intent
	fromHashed := common.KeyHash(args.DhtId.Uid)
	if fromHashed != args.DhtId.HashedUid {
		reply.Success = false
		reply.Err = "The DhtId's computed hash ID did not match the provided hash ID."
		return nil
	}

	data := make(map[string]interface{})

	for k, v := range hn.data.Data {
		hk := common.KeyHash(k)
		if hk <= fromHashed {
			data[k] = v
		}
	}

	reply.Data = data
	reply.Success = true
	return nil
}

//
// Called when this DHT Node needs to upgrade portion of the keys it was replicating
// to be the primary keys. This usually happens when the Chord predecessor for this
// node has been changed, and now this DHT is incharge of more keys.
//
func (hn *HashTableNode) upgradeReplica(fromKey int) {
	// This can be optimized.
	newreplicas := make(map[int]*ReplicaInfo)
	upgradedReplicas := make(map[int]*ReplicaInfo)

	for k, v := range hn.replicas {
		// If this replica is one of the ones within the desired ones,
		// we can upgrade this replica and move it to data.
		// If not, it stays as replicas.
		// The range is all key I know of from the desired key
		if k >= fromKey {
			hn.data.AddRange(v.Replica.Data)
			upgradedReplicas[k] = v
		} else {
			newreplicas[k] = v
		}
	}

	hn.replicas = newreplicas
	myNode := NodeId{Uid: hn.chord.MyRawId(), HashedUid: common.KeyHash(hn.chord.MyRawId())}

	for _, ur := range upgradedReplicas {
		for _, successor := range hn.successors {
			// TODO: Add retries for this rpc
			args := MoveReplicasArgs{FromId: ur.Node, ToId: myNode, Data: ur.Replica.Data}
			reply := MoveReplicasReply{}
			common.Call(successor.Addr, RpcPath, "HashTableNode.MoveReplicas", &args, &reply, RpcTimeout)
			if !reply.Success {
				log.Println("Did not received a success response when attempting to move replicas..")
			}
		}
	}
}

func (hn *HashTableNode) downgradePrimary(newPrimary NodeId, toKey int) {
	newreplica := ReplicaInfo{Node: newPrimary, Replica: HashData{}}
	newdata := HashData{}
	oldData := make(map[string]interface{})

	for k, v := range hn.data.Data {
		hk := common.KeyHash(k)
		// All keys until the new toKey can be downgraded from the primary
		// Presumably, all keys until toKey will now be handled by the newPrimary
		if hk <= toKey {
			newreplica.Replica.Put(k, v)
			oldData[k] = v
		} else {
			newdata.Put(k, v)
		}
	}

	hn.data = newdata
	myNode := NodeId{Uid: hn.chord.MyRawId(), HashedUid: common.KeyHash(hn.chord.MyRawId())}

	for _, successor := range hn.successors {
		// Move the data that I was replicating to be now owned by the new primary node
		args := MoveReplicasArgs{FromId: myNode, ToId: newPrimary, Data: oldData}
		reply := MoveReplicasReply{}
		common.Call(successor.Addr, RpcPath, "HashTableNode.MoveReplicas", &args, &reply, RpcTimeout)
		if !reply.Success {
			log.Println("Did not received a success response when attempting to move replicas..")
		}
	}
}

//
// This is a special operation that is called by DHT nodes whose replicas are stored on this
// server. This operation is different from Put because this operation itself must not be
// re-replicated to avoid an infinite loop of replication.
// Additionally separating replica operation gives finer grained controls.
//
func (hn *HashTableNode) ReplicatePut(args *ReplicatePutArgs, reply *ReplicatePutReply) error {
	hn.mu.Lock()
	defer hn.mu.Unlock()

	// Helps eliminate some malicious intent
	hashedId := common.KeyHash(args.DhtId.Uid)
	if hashedId != args.DhtId.HashedUid {
		reply.Success = false
		reply.Err = "The DhtId's computed hash ID did not match the provided hash ID."
		return nil
	}

	if _, ok := hn.replicas[hashedId]; !ok {
		ri := ReplicaInfo{}
		ri.Node = args.DhtId
		ri.Replica = HashData{}
	}

	hn.replicas[hashedId].Replica.Put(args.Key, args.Value)
	reply.Success = true
	return nil
}

//
// Called by the application to get a key from the DHT
//
func (hn *HashTableNode) Get(key string) interface{} {
	// TODO: Currently this will infinitely attempt to find the key
	// This needs to be fixed to know when key is not present, and only retry a specific number of iterations.
	for {
		hn.mu.Lock()
		if v, ok := hn.data.Get(key); ok {
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
			val, _ := hn.data.Get(key)
			return val
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
		if _, ok := hn.data.Get(key); ok || hn.chord.IsMyKey(keyHashed) {
			hn.localPutAndReplicate(key, value)
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
			hn.localPutAndReplicate(key, value)
			return true
		}

		reply := PutValueReply{}
		ok := common.Call(add, RpcPath, "HashTableNode.PutValue", &args, &reply, RpcTimeout)
		if ok && reply.Success {
			return true
		}
	}
}

func (hn *HashTableNode) localPutAndReplicate(key string, value interface{}) {
	hn.data.Put(key, value)
	for _, su := range hn.successors {
		for i := 0; i < 5; i++ {
			args := PutValueArgs{Key: key, Value: value}
			reply := PutValueReply{}
			ok := common.Call(su.Addr, RpcPath, "HashTableNode.ReplicatePut", &args, &reply, RpcTimeout)
			if ok {
				break
			}
			time.Sleep(100 * time.Millisecond)
			if i == 4 {
				log.Fatalf("Could not replicate data. This can be ignored later. For now, let's be stricter.")
			}
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
	if v, ok := hn.data.Get(args.Key); ok {
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
	if _, ok := hn.data.Get(args.Key); ok {
		hn.localPutAndReplicate(args.Key, args.Value)
		reply.Success = true
	} else if hn.chord.IsMyKey(keyHashed) {
		// This means that even though I don't have the key,
		// this key should be mine.
		hn.localPutAndReplicate(args.Key, args.Value)
		reply.Success = true
	} else {
		reply.Success = false
		reply.Err = fmt.Sprintf("Hash Table Node %v is not responsible for Key %v.", hn.id, args.Key)
	}
	return nil
}

func (hn *HashTableNode) predecessorChanged(old chord.NodeInfo, new chord.NodeInfo) {
	// TODO: might need to use the Chord approximation mod function
	// Example: 7, 8, 9 (new = 7, old = 8, my = 9)
	oldBetNewMe := old.Id > new.Id && old.Id < hn.chord.MyId()

	// Example: 8, 9, 0 (new = 8, old = 9, my = 0)
	oldBetNewMe = oldBetNewMe || (old.Id > new.Id && new.Id > hn.chord.MyId() && old.Id > hn.chord.MyId())

	// Example : 9, 0, 1 (new = 9, old = 0, my = 1)
	oldBetNewMe = oldBetNewMe || (new.Id > old.Id && new.Id > hn.chord.MyId())

	if oldBetNewMe {
		// Old predecessor is lost. I must promote replicas [new-key, old-key] my own.
		hn.upgradeReplica(new.Id)
	} else {
		// Newer predecessor is closer to me. So, I can expire entries that are covered by this
		// TODO: Fill this
		newP := NodeId{HashedUid: new.Id, Uid: ""}
		hn.downgradePrimary(newP, new.Id)
	}
}

func (hn *HashTableNode) joined(successor chord.NodeInfo) {
	for {
		myId := NodeId{Uid: hn.chord.MyRawId(), HashedUid: hn.chord.MyId()}
		args := GetMyDataArgs{DhtId: myId}
		reply := GetMyDataReply{}
		common.Call(successor.Addr, RpcPath, "HashTableNode.GetMyData", &args, &reply, RpcTimeout)

		if reply.Success {
			hn.data = HashData{}
			hn.data.AddRange(reply.Data)
			break
		}
		log.Printf("Could not get the data from my successor. Error: %v", reply.Err)
		time.Sleep(200 * time.Millisecond)
	}
}

func (hn *HashTableNode) successorChanged(oldsuccessors []chord.NodeInfo, newSuccessors []chord.NodeInfo) {
	// TODO: Remove, and add replicas
	oldMap := make(map[string]chord.NodeInfo)
	newMap := make(map[string]chord.NodeInfo)

	for _, v := range oldsuccessors {
		oldMap[v.Addr] = v
	}
	for _, v := range newSuccessors {
		newMap[v.Addr] = v
	}

	myNode := NodeId{Uid: hn.chord.MyRawId(), HashedUid: common.KeyHash(hn.chord.MyRawId())}
	for k, v := range newMap {
		if _, ok := oldMap[k]; !ok {
			// Store replicas in these new successors
			args := StoreReplicasArgs{DhtId: myNode, Replicas: hn.data.Data}
			reply := StoreReplicasReply{}
			common.Call(v.Addr, RpcPath, "HashTableNode.StoreReplicas", &args, &reply, RpcTimeout)
			// TODO: Success timeout?
			if !reply.Success {
				log.Printf("Failed to store the replica. Error was: %v", reply.Err)
			}
		}
	}

	hn.successors = newSuccessors
}
