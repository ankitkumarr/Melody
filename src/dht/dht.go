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
const DhtDebug = 0
const RpcDefaultRetryCount = 5
const RpcRetryMilliSeconds = 100

// var debugStart time.Time = time.Now()

const infolog = true

func InfoLog(format string, a ...interface{}) {
	if infolog {
		log.Printf(format, a...)
	}
}

type HashTableNode struct {
	id            string
	hashedId      int
	data          HashData
	replicas      map[int]*ReplicaInfo
	chord         *chord.Node
	mu            sync.Mutex
	address       string
	successors    []chord.NodeInfo
	lastknownPred chord.NodeInfo
	chordChangeCh chan chord.ChangeNotifyMsg
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
	Version int
	Err     string
	Success bool
}

type PutValueArgs struct {
	Id      string
	Key     string
	Value   interface{}
	Version int
}

type PutValueReply struct {
	Id      string
	Err     string
	Success bool
}

type ReplicatePutArgs struct {
	DhtId   NodeId
	Key     string
	Value   interface{}
	Version int
}

type ReplicatePutReply struct {
	Success bool
	Err     string
}

type StoreReplicasArgs struct {
	DhtId    NodeId
	Replicas map[string]interface{}
	Versions map[string]int
}

type StoreReplicasReply struct {
	Success bool
	Err     string
}

type MoveReplicasArgs struct {
	FromId   NodeId
	ToId     NodeId
	Data     map[string]interface{} // This is sent mostly for verification purpose
	Versions map[string]int
}

type MoveReplicasReply struct {
	Success bool
	Err     string
}

type GetMyDataArgs struct {
	DhtId NodeId
}

type GetMyDataReply struct {
	Data     map[string]interface{}
	Versions map[string]int
	Success  bool
	Err      string
}

func Make(ch *chord.Node, id string, address string, chordChangeCh chan chord.ChangeNotifyMsg) *HashTableNode {
	htNode := HashTableNode{}
	htNode.id = id
	htNode.hashedId = common.KeyHash(id)
	htNode.chord = ch
	htNode.address = address
	htNode.chordChangeCh = chordChangeCh

	htNode.data = HashData{}
	htNode.data.Data = make(map[string]interface{})

	htNode.replicas = make(map[int]*ReplicaInfo)

	newServer := rpc.NewServer()
	newServer.Register(&htNode)
	newServer.HandleHTTP(RpcPath, RpcDebugPath)

	htNode.setupHttpRoutes()
	go htNode.monitorChordChanges()

	return &htNode
}

func (hn *HashTableNode) debugLog(format string, a ...interface{}) {
	if DhtDebug > 0 {
		// time := time.Since(debugStart).Microseconds()
		// time /= 100
		time := time.Now()
		prefix := fmt.Sprintf("%02d:%02d:%02d:%2d [%v] [%v] [%v] [pre: %v]: ", time.Hour(), time.Minute(), time.Second(), time.UnixMilli(), hn.address, hn.id, hn.hashedId, hn.lastknownPred.Id)
		if DhtDebug > 2 {
			prefix = fmt.Sprintf("%v [data: %v]  ", prefix, hn.data)
		}
		format = prefix + format
		log.Printf(format, a...)
	}
}

func (hn *HashTableNode) dataDebugLog(format string, a ...interface{}) {
	if DhtDebug > 1 {
		hn.debugLog(format, a)
	}
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
		val, _ := hn.get(key)
		retrieved, ok := val.(string)
		if !ok {
			retrieved = ""
		}
		io.WriteString(w, fmt.Sprintf("Retrieved value for key %v: %v\n", key, retrieved))
		// fmt.Println("GET params were:", r.URL.Query())
	}
	put := func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		key := query["key"][0]
		value := query["value"][0]
		version := -1
		if len(query["version"]) > 0 {
			v, err := strconv.Atoi(query["version"][0])
			if err != nil {
				io.WriteString(w, "Could not parse version")
			}
			version = v
		}
		success := hn.put(key, value, version)
		if success {
			io.WriteString(w, fmt.Sprintf("Add key %v: %v to the DHT\n", key, value))
		} else {
			io.WriteString(w, fmt.Sprintf("Failed to add key %v: %v to the DHT\n", key, value))
		}
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

	hn.debugLog("Received StoreReplica call from %v for %v replicas", args.DhtId.Uid, len(args.Replicas))

	// TODO: If we want to actually be careful about the size of the replicas,
	// We need to check if the Data in store replica call was already there in some other replicas,
	// if so, we need to perform a move operation
	// TODO: Otherwise, if a new Node A joins and takes some keys off of Node B, we don't just want to
	// add A's newly acquired data, we need to get rid of B's old data too. Hopefully, B is alive enough to
	// call the MoveReplicas itself. But who knows!

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

	hn.replicas[hashedId].Replica.AddRange(args.Replicas, args.Versions)

	// TODO: Evict smallest replica if number of replicas is more than desired.
	// Find the smallest replica that is greater than me. In case my ID is close to boundary.
	// If none greater than me, evict the smallest replica among all.

	hn.debugLog("StoreReplica call from %v for %v replicas succeeded", args.DhtId.Uid, len(args.Replicas))
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

	hn.debugLog("Received MoveReplicas call to move %v replicas from %v to %v", len(args.Data), args.FromId.Uid, args.ToId.Uid)

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
		if _, ook := hn.replicas[args.ToId.HashedUid]; !ook {
			ri := ReplicaInfo{}
			ri.Node = args.ToId
			ri.Replica = HashData{}
			hn.replicas[args.ToId.HashedUid] = &ri
		}

		hn.replicas[args.ToId.HashedUid].Replica.AddRange(args.Data, args.Versions)
		hn.debugLog("MoveReplicas call to move %v replicas from %v to %v succeded. I did not have source data.", len(args.Data), args.FromId.Uid, args.ToId.Uid)
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

	hn.replicas[args.ToId.HashedUid].Replica.AddRange(args.Data, args.Versions)

	// Remove data from the old replica. We can just reset it
	// and let the garabage collector free up memory.
	hn.replicas[args.FromId.HashedUid].Replica.RemoveRange(args.Data, args.Versions)

	reply.Success = true
	hn.debugLog("MoveReplicas call to move %v replicas from %v to %v succeded.", len(args.Data), args.FromId.Uid, args.ToId.Uid)
	return nil
}

//
// Get all the data that I should own.
// This should be called by a new Node joining the DHT.
//
func (hn *HashTableNode) GetMyData(args *GetMyDataArgs, reply *GetMyDataReply) error {
	hn.mu.Lock()
	defer hn.mu.Unlock()
	hn.debugLog("Received GetMyData call to get all data before %v", args.DhtId.Uid)

	// Helps eliminate some malicious intent
	fromHashed := common.KeyHash(args.DhtId.Uid)
	if fromHashed != args.DhtId.HashedUid {
		reply.Success = false
		reply.Err = "The DhtId's computed hash ID did not match the provided hash ID."
		return nil
	}

	data := make(map[string]interface{})
	versions := make(map[string]int)
	myhashed := chord.MyId(hn.chord)

	// This means that the requester's ID is before 0.
	// Eg- I am 100, requestor is 950; I need to share 101 - 950
	// I am 950, requester is 100; I need to share 951 - 999 & 0 - 100
	IamSmaller := myhashed < fromHashed

	for k, v := range hn.data.Data {
		hk := common.KeyHash(k)
		if IamSmaller {
			if hk > myhashed && hk <= fromHashed {
				data[k] = v
				versions[k] = hn.data.Versions[k]
			}
		} else if !IamSmaller {
			if (hk > myhashed && hk > fromHashed) || (hk < myhashed && hk < fromHashed) {
				data[k] = v
				versions[k] = hn.data.Versions[k]
			}
		}
	}

	hn.debugLog("GetMyData call to get all data before %v succeeded with %v entries", args.DhtId.Uid, len(data))
	reply.Data = data
	reply.Versions = versions
	reply.Success = true
	return nil
}

//
// Called when this DHT Node needs to upgrade portion of the keys it was replicating
// to be the primary keys. This usually happens when the Chord predecessor for this
// node has been changed, and now this DHT is incharge of more keys.
// Must hold a lock when called
//
func (hn *HashTableNode) upgradeReplica(fromKey int, oldstringId string) {
	// This can be optimized.
	newreplicas := make(map[int]*ReplicaInfo)
	upgradedReplicas := make(map[int]*ReplicaInfo)
	myhashed := chord.MyId(hn.chord)

	// This means that the fromKey ID is before 0.
	// Eg- I am 100, fromKey is 950, we want keys from 951 - 999 & 0 - 100
	// If I am 950, fromKey is 100, we want keys from 101 - 950
	IamSmaller := myhashed < fromKey

	for k, v := range hn.replicas {
		// If this replica is one of the ones within the desired ones,
		// we can upgrade this replica and move it to data.
		// If not, it stays as replicas.
		// The range is all key I know of from the desired key
		if IamSmaller {
			// key must be less than both or greater than both
			// If I am 100, fromKey is 950, we want 0-100 & 950 - 999
			if (k > fromKey && k >= myhashed) || (k <= fromKey && k <= myhashed) {
				hn.data.AddRange(v.Replica.Data, v.Replica.Versions)
				upgradedReplicas[k] = v
			} else {
				newreplicas[k] = v
			}
		} else if !IamSmaller && k > fromKey && k <= myhashed {
			hn.data.AddRange(v.Replica.Data, v.Replica.Versions)
			upgradedReplicas[k] = v
		} else {
			newreplicas[k] = v
		}
	}
	hn.replicas = newreplicas
	succ := make([]chord.NodeInfo, len(hn.successors))
	copy(succ, hn.successors)
	hn.mu.Unlock()
	defer hn.mu.Lock()

	myNode := NodeId{Uid: chord.MyRawId(hn.chord), HashedUid: common.KeyHash(chord.MyRawId(hn.chord))}

	hn.debugLog("Upgraded %v replica sets to primary. Need to Move these replicas for my successors", len(upgradedReplicas))
	hn.dataDebugLog("I have replicas that look like %v", hn.replicas)
	hn.dataDebugLog("My dataset looks like %v", hn.data)

	moved := false

	for _, ur := range upgradedReplicas {
		// No data to move
		if len(ur.Replica.Data) == 0 {
			continue
		}

		moved = true
		for _, successor := range succ {
			// TODO: This can be parallelized
			for i := 0; i < RpcDefaultRetryCount; i++ {
				args := MoveReplicasArgs{FromId: ur.Node, ToId: myNode, Data: ur.Replica.Data, Versions: ur.Replica.Versions}
				reply := MoveReplicasReply{}
				hn.debugLog("Calling moveReplica on %v for %v key vals to be moved from %v to %v", successor.StringID, len(ur.Replica.Data), ur.Node.Uid, myNode.Uid)
				common.Call(successor.Addr, RpcPath, "HashTableNode.MoveReplicas", &args, &reply, RpcTimeout)
				if !reply.Success {
					// log.Printf("Did not receive a success response when attempting to move replicas. Error: %v", reply.Err)
					hn.debugLog("Did not receive a success response when attempting to move replicas. Error: %v", reply.Err)
				} else {
					hn.debugLog("MoveReplica for %v key vals to be moved from %v to %v succeeded", len(ur.Replica.Data), ur.Node.Uid, myNode.Uid)
					break
				}
				time.Sleep(time.Millisecond * RpcRetryMilliSeconds)
			}
		}
	}

	if moved {
		InfoLog("DHT upgraded some replicas to PRIMARY (likely because %v disconnected) and requested successors to move replicas.", oldstringId)
	}
}

//
// Must hold a lock when called
//
func (hn *HashTableNode) downgradePrimary(newPrimary NodeId, toKey int) {
	newreplica := ReplicaInfo{Node: newPrimary, Replica: HashData{}}
	newdata := HashData{}
	oldData := make(map[string]interface{})
	oldversions := make(map[string]int)

	myhashed := chord.MyId(hn.chord)

	// This means that the toKey is before 0.
	// Eg- I am 100, toKey is 950 ; we want keys from 101-950
	// If I am 950, toKey is 100; we want keys from 951 - 999 & 0 - 100
	IamSmaller := myhashed < toKey

	for k, v := range hn.data.Data {
		hk := common.KeyHash(k)
		// All keys until the new toKey can be downgraded from the primary
		// Presumably, all keys until toKey will now be handled by the newPrimary
		// key must be less than both or greater than both
		if IamSmaller {
			if hk <= toKey && hk > myhashed {
				newreplica.Replica.ForcePut(k, v, hn.data.Versions[k])
				oldData[k] = v
				oldversions[k] = hn.data.Versions[k]
			} else {
				newdata.ForcePut(k, v, hn.data.Versions[k])
			}
		} else {
			// if I am 950 and tokey is 100; we want 950 - 999 & 0 - 100
			if (hk > myhashed && hk >= toKey) || (hk <= toKey && hk <= myhashed) {
				newreplica.Replica.ForcePut(k, v, hn.data.Versions[k])
				oldData[k] = v
				oldversions[k] = hn.data.Versions[k]
			} else {
				newdata.ForcePut(k, v, hn.data.Versions[k])
			}
		}
	}

	hn.data = newdata
	myNode := NodeId{Uid: chord.MyRawId(hn.chord), HashedUid: common.KeyHash(chord.MyRawId(hn.chord))}
	hn.debugLog("Downgraded %v primary data to replicas of %v. Need to ask my successors to move that data", len(oldData), newPrimary.Uid)

	succ := make([]chord.NodeInfo, len(hn.successors))
	copy(succ, hn.successors)
	hn.mu.Unlock()
	defer hn.mu.Lock()

	// No data to move
	if len(oldData) == 0 {
		return
	}

	for _, successor := range succ {
		// Move the data that I was replicating to be now owned by the new primary node
		// TODO: This can be parallelized
		for i := 0; i < RpcDefaultRetryCount; i++ {
			args := MoveReplicasArgs{FromId: myNode, ToId: newPrimary, Data: oldData, Versions: oldversions}
			reply := MoveReplicasReply{}
			hn.debugLog("Calling moveReplica on %v for %v key vals to be moved from %v to %v", successor.StringID, len(oldData), myNode.Uid, newPrimary.Uid)
			common.Call(successor.Addr, RpcPath, "HashTableNode.MoveReplicas", &args, &reply, RpcTimeout)
			if !reply.Success {
				hn.debugLog("Did not receive a success response when attempting to move replicas. Error: %v", reply.Err)
			} else {
				hn.debugLog("MoveReplica on %v for %v key vals to be moved from %v to %v succeeded", successor.StringID, len(oldData), myNode.Uid, newPrimary.Uid)
				break
			}
			time.Sleep(time.Millisecond * RpcRetryMilliSeconds)
		}
	}

	InfoLog("DHT downgraded some PRIMARY data to REPLICAs (likely because %v joined) and requested successors to move replicas.", newPrimary.Uid)
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

	hn.debugLog("Received ReplicatePut from %v for key %v, value %v", args.DhtId.Uid, args.Key, args.Value)
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
		hn.replicas[hashedId] = &ri
	}

	hn.replicas[hashedId].Replica.ForcePut(args.Key, args.Value, args.Version)

	hn.debugLog("ReplicatePut from %v for key %v, value %v succeeded", args.DhtId.Uid, args.Key, args.Value)
	reply.Success = true
	return nil
}

//
// Called by the application to get a key from the DHT
//
func (hn *HashTableNode) get(key string) (interface{}, int) {
	for i := 0; i < RpcDefaultRetryCount; i++ {
		hn.debugLog("Melody called GET for %v", key)
		hn.mu.Lock()
		if val, version, ok := hn.data.Get(key); ok {
			hn.mu.Unlock()
			hn.debugLog("Melody called GET for %v returning %v from local", key, val)
			return val, version
		}

		keyHashed := common.KeyHash(key)
		if chord.IsMyKey(hn.chord, keyHashed) {
			hn.mu.Unlock()
			hn.debugLog("Melody called GET for %v returning nil from local", key)
			return nil, -1
		}

		hn.mu.Unlock()
		args := GetValueArgs{}
		args.Id = strconv.Itoa(os.Getuid())
		args.Key = key

		hn.debugLog("Asking Chord for the IP responsible for the key %v", key)
		add, _, _ := chord.Lookup(hn.chord, keyHashed)
		hn.debugLog("Chord returned %v is responsible for key %v", add, key)

		if add == hn.address {
			hn.mu.Lock()
			defer hn.mu.Unlock()
			val, ver, _ := hn.data.Get(key)
			hn.debugLog("Melody called GET for %v returning %v from local, as chord gave my own IP", key, val)
			return val, ver
		}

		hn.debugLog("Making GetValue call for key %v to address %v", key, add)
		reply := GetValueReply{}
		ok := common.Call(add, RpcPath, "HashTableNode.GetValue", &args, &reply, RpcTimeout)
		if ok && reply.Success {
			hn.debugLog("GetValue call for key %v to address %v succeeded. Returning value %v", key, add, reply.Value)
			return reply.Value, reply.Version
		} else {
			hn.debugLog("GetValue call for key %v to address %v failed. Retrying the entire ordeal...", key, add)
		}
		time.Sleep(time.Millisecond * 500)
	}
	log.Fatalf("Could not communicate with Chord ring to get the required data. Quitting...")
	return "", -1
}

//
// Called by the application to put a key value in the DHT
//
func (hn *HashTableNode) put(key string, value interface{}, version int) bool {
	hn.debugLog("Melody called PUT for %v with value %v", key, value)
	keyHashed := common.KeyHash(key)
	for i := 0; i < RpcDefaultRetryCount; i++ {
		hn.mu.Lock()
		if _, _, ok := hn.data.Get(key); ok || chord.IsMyKey(hn.chord, keyHashed) {
			err := hn.localPutAndReplicate(key, value, version)

			// TODO: Can be due to connection issue or conflict
			// Just return false for now. Would be helpful to have a proper
			// error contract with Melody
			if err != nil {
				hn.mu.Unlock()
				return false
			}
			hn.mu.Unlock()
			hn.debugLog("Melody called PUT for %v added to local", key)
			return true
		}
		hn.mu.Unlock()

		hn.debugLog("Asking Chord for the IP responsible for the key %v", key)
		add, _, _ := chord.Lookup(hn.chord, keyHashed)
		hn.debugLog("Chord returned %v is responsible for key %v", add, key)

		if add == hn.address {
			hn.mu.Lock()
			defer hn.mu.Unlock()
			err := hn.localPutAndReplicate(key, value, version)

			if err != nil {
				return false
			}
			hn.debugLog("Melody called PUT for %v added to local because Chord gave my own IP", key)
			return true
		}

		args := PutValueArgs{}
		args.Id = strconv.Itoa(os.Getuid())
		args.Key = key
		args.Value = value
		args.Version = version

		hn.debugLog("Making PutValue call for key %v to address %v", key, add)
		reply := PutValueReply{}
		ok := common.Call(add, RpcPath, "HashTableNode.PutValue", &args, &reply, RpcTimeout)
		if ok && reply.Success {
			hn.debugLog("PutValue call for key %v to address %v succeeded.", key, add)
			return true
		} else {
			hn.debugLog("PutValue call for key %v to address %v failed. Retrying the entire ordeal...", key, add)
		}
		time.Sleep(500 * time.Millisecond)
	}
	InfoLog("Could not communicate with Chord ring to get the required data. Or maybe there was a conflict.")
	return false
}

func Put(hn *HashTableNode, key string, value interface{}, version int) bool {
	return hn.put(key, value, version)
}

func (hn *HashTableNode) localPutAndReplicate(key string, value interface{}, version int) error {
	err := hn.data.Put(key, value, version)

	if err != nil {
		return err
	}

	succ := make([]chord.NodeInfo, len(hn.successors))
	copy(succ, hn.successors)
	hn.mu.Unlock()
	defer hn.mu.Lock()

	for _, su := range succ {
		for i := 0; i < RpcDefaultRetryCount; i++ {
			hn.debugLog("Attempting to replicate put call for key %v, value %v to address %v", key, value, su.Addr)
			myNode := NodeId{Uid: chord.MyRawId(hn.chord), HashedUid: chord.MyId(hn.chord)}
			args := ReplicatePutArgs{Key: key, Value: value, DhtId: myNode, Version: version}
			reply := ReplicatePutReply{}
			ok := common.Call(su.Addr, RpcPath, "HashTableNode.ReplicatePut", &args, &reply, RpcTimeout)
			if ok && reply.Success {
				hn.debugLog("Replicate put call for key %v, value %v to address %v succeeded", key, value, su.Addr)
				break
			} else {
				hn.debugLog("Replicate put call for key %v, value %v to address %v failed. Error: %v. May retry..", key, value, su.Addr, reply.Err)
			}
			if i == 4 {
				hn.debugLog("Could not replicate data. This may lead to data loss. Ignoring...")
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

	return nil
}

//
// RPC method called by other Distributed Hash table nodes to get a key
//
func (hn *HashTableNode) GetValue(args *GetValueArgs, reply *GetValueReply) error {
	hn.mu.Lock()
	defer hn.mu.Unlock()
	hn.debugLog("Received GetValue call for key %v", args.Key)
	keyHashed := common.KeyHash(args.Key)
	reply.Id = args.Id
	if val, ver, ok := hn.data.Get(args.Key); ok {
		reply.Value = val
		reply.Version = ver
		reply.Success = true
		hn.debugLog("GetValue call for key %v succeeded with value %v", args.Key, val)
	} else if chord.IsMyKey(hn.chord, keyHashed) {
		// This means that even though I don't have the key,
		// this key should be mine. So, we can return an empty response.
		reply.Success = true
		hn.debugLog("GetValue call for key %v succeeded with nil", args.Key)
		reply.Value = nil
	} else {
		hn.debugLog("GetValue call for key %v failed becaue it's not my key", args.Key)
		reply.Success = false
		reply.Err = fmt.Sprintf("Hash Table Node %v does not contain Key %v.", hn.id, args.Key)
	}
	InfoLog("DHT Node received GET call from another DHT Node for key %v.", args.Key)
	return nil
}

//
// RPC method called by other Distributed Hash table nodes to put a value mapped to a key
//
func (hn *HashTableNode) PutValue(args *PutValueArgs, reply *PutValueReply) error {
	hn.mu.Lock()
	defer hn.mu.Unlock()
	hn.debugLog("Received PutValue call for key %v with value %v", args.Key, args.Value)
	reply.Id = args.Id
	keyHashed := common.KeyHash(args.Key)
	// TODO: Maybe replication should be in the background. Eventual consistency.
	// can't expect all replicas to always be up
	if _, _, ok := hn.data.Get(args.Key); ok {
		hn.localPutAndReplicate(args.Key, args.Value, args.Version)
		reply.Success = true
	} else if chord.IsMyKey(hn.chord, keyHashed) {
		// This means that even though I don't have the key,
		// this key should be mine.
		hn.localPutAndReplicate(args.Key, args.Value, args.Version)
		reply.Success = true
	} else {
		reply.Success = false
		reply.Err = fmt.Sprintf("Hash Table Node %v is not responsible for Key %v.", hn.id, args.Key)
	}

	if reply.Success {
		hn.debugLog("PutValue call for key %v with value %v succeeded", args.Key, args.Value)
	} else {
		hn.debugLog("PutValue call for key %v with value %v failed because I am not responsible for that key", args.Key, args.Value)
	}
	InfoLog("DHT Node received PUT call from another DHT Node for key %v.", args.Key)
	return nil
}

func (hn *HashTableNode) predecessorChanged(old chord.NodeInfo, new chord.NodeInfo) {
	hn.mu.Lock()
	defer hn.mu.Unlock()

	hn.debugLog("Predecessor has changed from %v (id: %v) to %v (id %v), my ID is %v", old.StringID, old.Id, new.StringID, new.Id, chord.MyId(hn.chord))
	if (old.Id <= 0 || old.Addr == "") && (hn.lastknownPred.Addr == "" || hn.lastknownPred.Id <= 0) {
		hn.lastknownPred = new
		newP := NodeId{HashedUid: new.Id, Uid: new.StringID}
		hn.debugLog("Predecessor was assigned. Did not have one before. Downgrade primary keys before %v to be replicas for %v", new.Id, new.StringID)
		hn.downgradePrimary(newP, new.Id)
		return
	} else if old.Id <= 0 || old.Addr == "" {
		old = hn.lastknownPred
	}

	// I am the only one left in the ring. Promote all the replicas
	if new.Addr == hn.address {
		// Make sure old was not empty or myself, then we should track this old.
		if old.StringID != "" && old.Id >= 0 && old.Addr != hn.address {
			hn.lastknownPred = old
		}

		// Upgrade everthing to my own data. So we can say upgrade until my keys
		hn.upgradeReplica(chord.MyId(hn.chord)+1, old.StringID)
	}

	// Sometimes, if only Node left in the ring, the chord new predecessor
	// node can be itself.
	if new.Addr == "" || new.Id <= 0 || new.Addr == hn.address {
		hn.lastknownPred = old
		return
	}

	hn.lastknownPred = new

	// Example: 7, 8, 9 (new = 7, old = 8, my = 9)
	oldBetNewMe := old.Id > new.Id && old.Id < chord.MyId(hn.chord)

	// Example: 8, 9, 0 (new = 8, old = 9, my = 0)
	oldBetNewMe = oldBetNewMe || (old.Id > new.Id && new.Id > chord.MyId(hn.chord) && old.Id > chord.MyId(hn.chord))

	// Example : 9, 0, 1 (new = 9, old = 0, my = 1)
	oldBetNewMe = oldBetNewMe || (new.Id > old.Id && new.Id > chord.MyId(hn.chord) && old.Id < chord.MyId(hn.chord))

	if oldBetNewMe {
		// Old predecessor is lost. I must promote replicas [new-key, old-key] my own.
		hn.debugLog("An old predecessor was lost. Need to promote replicas from %v to be my data", new.Id)
		hn.upgradeReplica(new.Id, old.StringID)
	} else {
		// Newer predecessor is closer to me. So, I can expire entries that are covered by this
		newP := NodeId{HashedUid: new.Id, Uid: new.StringID}
		hn.debugLog("An new predecessor was added. Need to downgrade my data until %v to be replica for %v", new.Id, new.StringID)
		hn.downgradePrimary(newP, new.Id)
	}
}

func (hn *HashTableNode) joined(successor chord.NodeInfo) {
	// Retry forever here, because without joining, one can't do much
	for {
		hn.debugLog("I joined the ring. Need data until %v from %v at address %v", successor.Id, successor.StringID, successor.Addr)
		myId := NodeId{Uid: chord.MyRawId(hn.chord), HashedUid: chord.MyId(hn.chord)}
		args := GetMyDataArgs{DhtId: myId}
		reply := GetMyDataReply{}
		common.Call(successor.Addr, RpcPath, "HashTableNode.GetMyData", &args, &reply, RpcTimeout)

		if reply.Success {
			hn.data = HashData{}
			hn.data.AddRange(reply.Data, reply.Versions)
			hn.debugLog("Received data until %v from %v at address %v successfully. Got %v key vals. My hashId is %v.", successor.Id, successor.StringID, successor.Addr, len(reply.Data), myId.HashedUid)
			hn.dataDebugLog("The key vals I was assigned are %v", reply.Data)
			if len(reply.Data) > 0 {
				InfoLog("DHT received %v key value pairs from another DHT (%v) at address %v", len(reply.Data), successor.StringID, successor.Addr)
			}
			break
		} else {
			hn.debugLog("Did not receive data until %v from %v at address %v. Error: %v. Retrying forever...", successor.Id, successor.StringID, successor.Addr, reply.Err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (hn *HashTableNode) monitorChordChanges() {
	for change := range hn.chordChangeCh {
		// We may want to be careful with races here
		// Given that this can kickoff multiple concurrent changes.
		// For now, this is OK. Revisit if we see concurrency issues.
		if change.JoinEvent {
			// Cannot verify here before the length of successors is always same
			// if len(change.OldSuccessors) != 0 && len(change.NewSuccessors) != 1 {
			// 	log.Printf("Expected old successor list to be 0 and new successor list to be 1, "+
			// 		"but instead got %v and %v from Chord", len(change.OldSuccessors), len(change.NewSuccessors))
			// }
			go hn.joined(change.NewSuccessors[0])
			hn.debugLog("Received join event from Chord")
		} else if change.SuccesssorChange {
			go hn.successorChanged(change.OldSuccessors, change.NewSuccessors)
			hn.debugLog("Received successor change event from Chord")
		} else if change.PredecessorChange {
			go hn.predecessorChanged(change.OldPredecessor, change.NewPredecessor)
			hn.debugLog("Received predecessor change event from Chord")
		} else {
			hn.debugLog("Chord sent DHT a NOOP change. Unexpected!")
		}
		hn.debugLog("Waiting for next event!")
	}
}

func (hn *HashTableNode) successorChanged(oldsuccessors []chord.NodeInfo, newSuccessors []chord.NodeInfo) {
	oldMap := make(map[string]chord.NodeInfo)
	newMap := make(map[string]chord.NodeInfo)
	fixedNewSuccessors := make([]chord.NodeInfo, 0)

	for _, v := range oldsuccessors {
		if v.Addr == "" || v.Id == 0 {
			continue
		}
		oldMap[v.Addr] = v
	}
	for _, v := range newSuccessors {
		if v.Addr == "" || v.Id == 0 {
			continue
		}
		// TODO: This is temporary. Chord currenly can make itself its own successor. We should address that in Chord
		if v.Addr == hn.address {
			continue
		}
		fixedNewSuccessors = append(fixedNewSuccessors, v)
		newMap[v.Addr] = v
	}

	hn.debugLog("Successors has changed from %v count to %v count", len(oldMap), len(newMap))

	// No need to replicate if no data or successors to replicate
	if len(hn.data.Data) != 0 && len(newMap) != 0 {
		myNode := NodeId{Uid: chord.MyRawId(hn.chord), HashedUid: common.KeyHash(chord.MyRawId(hn.chord))}
		for k, v := range newMap {
			if _, ok := oldMap[k]; !ok {
				for i := 0; i < RpcDefaultRetryCount; i++ {
					hn.debugLog("Calling StoreReplica to replicate my data in the new successor %v with address %v", v.StringID, v.Addr)
					// Store replicas in these new successors
					args := StoreReplicasArgs{DhtId: myNode, Replicas: hn.data.Data, Versions: hn.data.Versions}
					reply := StoreReplicasReply{}
					common.Call(v.Addr, RpcPath, "HashTableNode.StoreReplicas", &args, &reply, RpcTimeout)
					if !reply.Success {
						hn.debugLog("Calling StoreReplica to replicate my data in the new successor %v with address %v failed. Error: %v. Retrying possibly...", v.StringID, v.Addr, reply.Err)
					} else {
						hn.debugLog("StoreReplica to replicate my data in the new successor %v with address %v succeeded", v.StringID, v.Addr)
						break
					}
					time.Sleep(time.Millisecond * RpcRetryMilliSeconds)
				}
			}
		}
	}

	hn.debugLog("Successors change waiting for lock")
	hn.mu.Lock()
	defer hn.mu.Unlock()

	uniqueSucc := make([]chord.NodeInfo, 0)
	for _, v := range newMap {
		uniqueSucc = append(uniqueSucc, v)
	}

	hn.successors = uniqueSucc
	hn.debugLog("Successors change handled and released lock")
}

func (hn *HashTableNode) getSuccessors() []string {
	hn.mu.Lock()
	defer hn.mu.Unlock()

	successors := make([]string, len(hn.successors))
	for i, s := range hn.successors {
		successors[i] = s.Addr
	}

	return successors
}

func (hn *HashTableNode) getData() map[string]interface{} {
	return hn.data.Data
}

func GetSuccessors(hn *HashTableNode) []string {
	return hn.getSuccessors()
}

func GetData(hn *HashTableNode) map[string]interface{} {
	return hn.getData()
}

func Get(hn *HashTableNode, key string) (interface{}, int) {
	return hn.get(key)
}
