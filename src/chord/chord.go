package chord

import (
	"Melody/common"
	"fmt"
	"log"
	"math"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	"sync/atomic"
)

const RpcTimeout = 5 * time.Second

const repeat = 3

type LogsTopic string

const (
	dFinds      LogsTopic = "FINDS"
	dClosestp   LogsTopic = "ClSPRE"
	dCreate     LogsTopic = "CRET"
	dJoin       LogsTopic = "JOIN"
	dStable     LogsTopic = "STAB"
	dNotify     LogsTopic = "NOTE"
	dFixFingers LogsTopic = "FIXF"
	dCheckPred  LogsTopic = "CHKP"
)

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = 1
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func debug_print(topic LogsTopic, format string, a ...interface{}) {
	if debugVerbosity > 0 {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

// const (
// 	OK      = "OK"
// 	ErrFail = "ErrFail"
// )

// type Err string

// see table 1 of chord paper
type Node struct {
	mu          sync.Mutex
	me          int        // id of the node, lives in range [0, 2^m - 1]
	addr        string     // the node's own address
	finger      []NodeInfo // finger table, length m-2, entry i is the successor to me+2^(i+1)
	successors  []NodeInfo // list of r successors, see sec E.3 of chord paper
	predecessor NodeInfo
	next        int   // used for fixing finger table, see fix_fingers() in Fig 6 of chord paper
	dead        int32 // set by Kill()
	ring_size   int   // length of the ring, 2 ** (len(finger) + 1)
}

// for storing info of other nodes
// also used as the args type for RPCs
type NodeInfo struct {
	Addr string
	Id   int
}

type RPCReply struct {
	Addr string
	Id   int
	// Err  Err
	Success bool
}

func (n *Node) Kill() {
	atomic.StoreInt32(&n.dead, 1)
	// Your code here, if desired.
}

func (n *Node) Restart() {
	go n.stabilize_ticker()
}

func (n *Node) killed() bool {
	z := atomic.LoadInt32(&n.dead)
	return z == 1
}

// initialize a new chord node
func Make(me int, addr string, m int, createRing bool, joinNodeId int, joinNodeAddr string) *Node {
	n := &Node{}
	n.me = me
	n.addr = addr
	n.finger = make([]NodeInfo, m-2)
	n.successors = make([]NodeInfo, m)
	n.ring_size = int(math.Pow(2, float64(m)))
	newServer := rpc.NewServer()
	newServer.Register(n)
	rpcPath := "/_goRPC_" + strconv.Itoa(me)
	debugPath := "/debug/rpc_" + strconv.Itoa(me)
	newServer.HandleHTTP(rpcPath, debugPath)
	if createRing {
		n.Create()
	} else if joinNodeId >= 0 {
		debug_print(dJoin, "N%d just created, asked to join ring from N%d, with joinaddr %v", me, joinNodeId, joinNodeAddr)
		joinNode := &NodeInfo{}
		joinNode.Addr = joinNodeAddr
		joinNode.Id = joinNodeId
		// debug_print(dJoin, "N%d actually joining ring", me)
		n.Join(joinNode)
	}
	go n.stabilize_ticker()
	go n.check_predecessor_ticker()
	// go n.fix_fingers_ticker()
	return n
}

func mod(n int, base int) int {
	n = n % base
	if n < 0 {
		n += base
	}
	return n
}

// check if ind is in range [start, end], modulo length
func isInRange(start int, end int, ind int, length int) bool {
	start = mod(start, length)
	end = mod(end, length)
	ind = mod(ind, length)
	if end >= start {
		if start <= ind && ind <= end {
			return true
		} else {
			return false
		}
	} else {
		if ind <= end || ind >= start {
			return true
		} else {
			return false
		}
	}
}

// check if end - start != 1, mod length
// need this because sometimes we need the range (start, end)
// then if end - start = 1, this range is empty and will mess up the usage of isInRange
func diffNotOne(start int, end int, length int) bool {
	return (end-start != 1) && !(start == length-1 && end == 0)
}

// create a new chord ring
func (n *Node) Create() {
	// clear the predecessor
	n.mu.Lock()
	n.predecessor = NodeInfo{}
	n.predecessor.Id = -1

	n.successors[0].Addr = n.addr
	n.successors[0].Id = n.me
	debug_print(dCreate, "N%d just created ring", n.me)
	n.mu.Unlock()
}

func (n *Node) FindSuccessor(args *NodeInfo, reply *RPCReply) error {
	n.mu.Lock()
	isSuccessor := isInRange(n.me+1, n.successors[0].Id, args.Id, n.ring_size)
	debug_print(dFinds, "N%d received findsuccessor request with id %d, cur suc %d, cur pred %d, is cur suc the successor of id? %v", n.me, args.Id, n.successors[0].Id, n.predecessor.Id, isSuccessor)
	if isSuccessor {
		reply.Addr = n.successors[0].Addr
		reply.Id = n.successors[0].Id
		reply.Success = true
		debug_print(dFinds, "N%d received findsuccessor request with id %d, cur suc %d, cur pred %d, cur suc is successor, returning", n.me, args.Id, n.successors[0].Id, n.predecessor.Id)
		defer n.mu.Unlock()
		return nil
	} else {
		// n_preced := n.closest_preceding_node(args.Id)

		// look for the closest precedding node

		// first search finger table
		var n_preced NodeInfo
		// for i := len(n.finger) - 1; i >= 0; i-- {
		// 	if n.finger[i].Addr != "" {
		// 		is_pred := isInRange(n.me+1, args.Id-1, n.finger[i].Id, n.ring_size) && diffNotOne(n.me, args.Id, n.ring_size)
		// 		if is_pred {
		// 			n_preced.Addr = n.finger[i].Addr
		// 			n_preced.Id = n.finger[i].Id
		// 			// debug_print(dClosestp, "N%d actually using finger table! preceeding note is the n + 2 ** %d entry at index N%d", n.me, i, n_preced.Id)
		// 			debug_print(dFinds, "N%d received findsuccessor request with id %d, cur suc %d, cur pred %d, closest preceding node is %v, from 2 ** %d term of fingers", n.me, args.Id, n.successors[0].Id, n.predecessor.Id, n_preced.Id, i+1)
		// 			n.mu.Unlock()

		// 			rpcPath := "/_goRPC_" + strconv.Itoa(n_preced.Id)
		// 			for i := 0; i < repeat; i++ {
		// 				var reply_inner *RPCReply
		// 				ok := common.Call(n_preced.Addr, rpcPath, "Node.FindSuccessor", &args, &reply_inner, RpcTimeout)
		// 				if ok {
		// 					reply.Addr = reply_inner.Addr
		// 					reply.Id = reply_inner.Id
		// 					reply.Success = reply_inner.Success
		// 					return nil
		// 				} else {
		// 					time.Sleep(10 * time.Millisecond)
		// 				}
		// 			}
		// 			n.mu.Lock()
		// 			debug_print(dFinds, "N%d received findsuccessor request with id %d, cur suc %d, cur pred %d, closest preceding node is %v, from 2 ** %d term of fingers, but that node has failed", n.me, args.Id, n.successors[0].Id, n.predecessor.Id, n_preced.Id, i+1)
		// 			// called n_preced repeat times, all failed
		// 			// assume that that finger has failed
		// 			n.finger[i] = NodeInfo{}
		// 			n.finger[i].Id = -1
		// 		}
		// 	}
		// }

		// next search successor list
		for i := len(n.successors) - 1; i >= 0; i-- {
			if n.successors[i].Addr != "" {
				is_pred := isInRange(n.me+1, args.Id-1, n.successors[i].Id, n.ring_size) && diffNotOne(n.me, args.Id, n.ring_size)
				if is_pred {
					n_preced.Addr = n.successors[i].Addr
					n_preced.Id = n.successors[i].Id
					debug_print(dFinds, "N%d received findsuccessor request with id %d, cur suc %d, cur pred %d, closest preceding node is %v", n.me, args.Id, n.successors[0].Id, n.predecessor.Id, n_preced.Id)
					n.mu.Unlock()

					rpcPath := "/_goRPC_" + strconv.Itoa(n_preced.Id)
					for i := 0; i < repeat; i++ {
						var reply_inner *RPCReply
						ok := common.Call(n_preced.Addr, rpcPath, "Node.FindSuccessor", &args, &reply_inner, RpcTimeout)
						if ok {
							reply.Addr = reply_inner.Addr
							reply.Id = reply_inner.Id
							reply.Success = reply_inner.Success
							return nil
						} else {
							time.Sleep(10 * time.Millisecond)
						}
					}
					n.mu.Lock()
					// assume successor i failed
					debug_print(dFinds, "N%d received findsuccessor request with id %d, cur suc %d, cur pred %d, closest preceding node is %v, but that node has failed", n.me, args.Id, n.successors[0].Id, n.predecessor.Id, n_preced.Id)
					n.successors[i] = NodeInfo{}
					n.successors[i].Id = -1
				}
			}
		}

		n.mu.Unlock()
		//UPSTREAM
		// by this point, all the fingers and successors must have failed, so we have to declare that the entire system has failed.
		reply.Success = false
		return nil
	}
}

// search local table for highest predecessor of id, not called by RPC
// assume lock is held when called
// func (n *Node) closest_preceding_node(id int) NodeInfo {
// 	var n_preced NodeInfo
// 	for i := len(n.finger) - 1; i >= 0; i-- {
// 		if n.finger[i].Addr != "" {
// 			is_pred := isInRange(n.me+1, id-1, n.finger[i].Id, n.ring_size) && diffNotOne(n.me, id, n.ring_size)
// 			if is_pred {
// 				n_preced.Addr = n.finger[i].Addr
// 				n_preced.Id = n.finger[i].Id
// 				debug_print(dClosestp, "N%d actually using finger table! returning n + 2 ** %d at index N%d", n.me, i, n_preced.Id)
// 				return n_preced
// 			}
// 		}
// 	}
// 	is_pred := isInRange(n.me+1, id-1, n.successor.Id, n.ring_size) && diffNotOne(n.me, id, n.ring_size)
// 	// (id-n.me != 1) && !(n.me == n.ring_size-1 && n.successor.Id == 0)
// 	if is_pred {
// 		n_preced.Addr = n.successor.Addr
// 		n_preced.Id = n.successor.Id
// 		return n_preced
// 	}
// 	n_preced.Addr = n.addr
// 	n_preced.Id = n.me
// 	return n_preced
// }

// join a chord ring containing n_current
func (n *Node) Join(n_current *NodeInfo) {
	n.mu.Lock()
	debug_print(dJoin, "N%d just started join, calling N%d", n.me, n_current.Id)
	n.predecessor = NodeInfo{}
	n.predecessor.Id = -1
	var args NodeInfo
	args.Id = n.me
	args.Addr = n.addr
	n.mu.Unlock()

	for {
		var reply RPCReply
		rpcPath := "/_goRPC_" + strconv.Itoa(n_current.Id)
		ok := common.Call(n_current.Addr, rpcPath, "Node.FindSuccessor", &args, &reply, RpcTimeout)
		debug_print(dJoin, "received findsuccessor response, success? %v", ok)
		if ok {
			n.mu.Lock()
			n.successors[0].Addr = reply.Addr
			n.successors[0].Id = reply.Id
			debug_print(dJoin, "N%d just set successor as N%d", n.me, reply.Id)
			n.mu.Unlock()
			return
		}
	}
}

func (n *Node) GetPredecessor(args *NodeInfo, reply *RPCReply) error {
	n.mu.Lock()
	reply.Addr = n.predecessor.Addr
	reply.Id = n.predecessor.Id
	reply.Success = true
	n.mu.Unlock()
	return nil
}

func (n *Node) stabilize_ticker() {
	for !n.killed() {
		n.mu.Lock()
		onlyNode := (n.successors[0].Id == n.me) && (n.predecessor.Addr == "")
		debug_print(dStable, "N%d in stabilize, onlynode? %v", n.me, onlyNode)
		n.mu.Unlock()
		if !onlyNode {
			// if successor fails keeps looking through successors until a successor succeeds
			n.mu.Lock()
			nSuc := len(n.successors)
			n.mu.Unlock()
			for i := 1; i < nSuc; i++ {
				n.mu.Lock()
				var args NodeInfo
				var reply RPCReply
				successor_addr := n.successors[0].Addr
				rpcPath := "/_goRPC_" + strconv.Itoa(n.successors[0].Id)
				debug_print(dStable, "N%d in stabilize, calling %d-th suc N%d at addr %v to get suc.pred, current suc lsit is %v", n.me, i, n.successors[0].Id, n.successors[0].Addr, n.successors)
				var ok bool
				n.mu.Unlock()

				// make rpc calls, if fails too many times, assume that the node failed
				for j := 0; j < repeat; j++ {
					reply = RPCReply{}
					ok = common.Call(successor_addr, rpcPath, "Node.GetPredecessor", &args, &reply, RpcTimeout)
					if ok {
						break
					} else {
						time.Sleep(10 * time.Millisecond)
					}
				}

				if ok {
					n.mu.Lock()
					if reply.Addr != "" {
						is_suc := isInRange(n.me+1, n.successors[0].Id-1, reply.Id, n.ring_size) && diffNotOne(n.me, n.successors[0].Id, n.ring_size)
						if is_suc {
							debug_print(dStable, "N%d in stabilize, prev suc is N%d, but suc.pred is N%d, so will change successor", n.me, n.successors[0].Id, reply.Id)
							old_suc := n.successors
							n.successors = make([]NodeInfo, len(old_suc)-1)
							copy(n.successors, old_suc[:(len(old_suc)-1)])
							new_suc := NodeInfo{}
							new_suc.Addr = reply.Addr
							new_suc.Id = reply.Id
							n.successors = append([]NodeInfo{new_suc}, n.successors...)
						}
					}
					successor_addr := n.successors[0].Addr
					rpcPath := "/_goRPC_" + strconv.Itoa(n.successors[0].Id)
					args.Addr = n.addr
					args.Id = n.me
					debug_print(dStable, "N%d in stabilize, calling notify to suc N%d", n.me, n.successors[0].Id)
					n.mu.Unlock()

					for j := 0; j < repeat; j++ {
						var reply NotifyReply
						ok = common.Call(successor_addr, rpcPath, "Node.Notify", &args, &reply, RpcTimeout)
						if ok {
							if reply.Success {
								n.mu.Lock()
								cur_suc := NodeInfo{}
								cur_suc.Addr = n.successors[0].Addr
								cur_suc.Id = n.successors[0].Id
								n.successors = make([]NodeInfo, len(reply.Successors))
								copy(n.successors, reply.Successors)
								n.successors = append([]NodeInfo{cur_suc}, n.successors...)
								n.mu.Unlock()
							}
							break
						} else {
							time.Sleep(10 * time.Millisecond)
						}
					}

					if ok {
						break
					} else {
						n.mu.Lock()
						// current suc[0] has failed because it does not reply to our Notify call, will reshuffle successor list
						old_suc := n.successors
						n.successors = make([]NodeInfo, len(old_suc)-1)
						copy(n.successors, old_suc[1:])
						new_suc := NodeInfo{}
						n.successors = append(n.successors, new_suc)
						n.mu.Unlock()
					}
				} else {
					n.mu.Lock()
					//UPSTREAM
					// current suc[0] has failed, will reshuffle successor list
					debug_print(dStable, "N%d in stabilize, %d-th suc N%d failed, removing it from suc list", n.me, i, n.successors[0].Id)
					old_suc := n.successors
					n.successors = make([]NodeInfo, len(old_suc)-1)
					copy(n.successors, old_suc[1:])
					new_suc := NodeInfo{}
					n.successors = append(n.successors, new_suc)
					debug_print(dStable, "N%d in stabilize, removed %d-th node, new suc list is %v", n.me, i, n.successors)
					n.mu.Unlock()
				}
			}
			// this means that all of our successors have failed. that means the whole chord has failed
			//UPSTREAM we could add a method that calls the DHT node and notifies it that the whole chord ring has failed
			// There is one interesting edge case: all other nodes have failed, except for one. Technically we still have a ring of size 1... do we declare failure?
		}
		time.Sleep(100 * time.Millisecond)
	}
}

type NotifyReply struct {
	Addr       string
	Id         int
	Success    bool
	Successors []NodeInfo
}

// to accomodate successor lists, should return list of successors
func (n *Node) Notify(args *NodeInfo, reply *NotifyReply) error {
	n.mu.Lock()
	debug_print(dNotify, "N%d received notify from N%d, cur pred is %d", n.me, args.Id, n.predecessor.Id)
	is_pred := isInRange(n.predecessor.Id+1, n.me-1, args.Id, n.ring_size) && diffNotOne(n.predecessor.Id, n.me, n.ring_size)
	if (n.predecessor.Id < 0) || is_pred {
		n.predecessor.Addr = args.Addr
		n.predecessor.Id = args.Id
	}
	debug_print(dNotify, "N%d received notify from N%d, changed pred to %d, returning", n.me, args.Id, n.predecessor.Id)
	reply.Successors = make([]NodeInfo, len(n.successors)-1)
	copy(reply.Successors, n.successors[:(len(n.successors)-1)])
	reply.Success = true
	defer n.mu.Unlock()
	return nil
}

func (n *Node) fix_fingers_ticker() {
	for !n.killed() {
		var args NodeInfo
		var reply RPCReply
		n.mu.Lock()
		n.next++
		if n.next >= len(n.finger) {
			n.next = 0
		}
		args.Id = mod(n.me+int(math.Pow(2, float64(n.next+1))), n.ring_size)
		// if args.Id < 0 {
		// 	args.Id += n.ring_size
		// }
		debug_print(dFixFingers, "N%d in fixfingers, about to look for finger %d at index %d", n.me, n.next, args.Id)
		n.mu.Unlock()
		n.FindSuccessor(&args, &reply)
		n.mu.Lock()
		n.finger[n.next].Addr = reply.Addr
		n.finger[n.next].Id = reply.Id
		debug_print(dFixFingers, "N%d in fixfingers, found finger %d at index %d, successor is N%d", n.me, n.next, args.Id, reply.Id)
		longerSleep := n.next == 0
		n.mu.Unlock()
		if longerSleep {
			time.Sleep(50 * time.Millisecond)
		} else {
			time.Sleep(20 * time.Millisecond)
		}
	}
}

// go routine that calls predecessor periodically to check if it has failed
func (n *Node) check_predecessor_ticker() {
	for !n.killed() {
		n.mu.Lock()
		pred_addr := n.predecessor.Addr
		rpcPath := "/_goRPC_" + strconv.Itoa(n.predecessor.Id)
		debug_print(dCheckPred, "N%d in check pred, current pred is N%d, sending msg", n.me, n.predecessor.Id)
		n.mu.Unlock()
		if pred_addr != "" {
			var args NodeInfo
			var reply RPCReply
			ok := common.Call(pred_addr, rpcPath, "Node.Alive", &args, &reply, RpcTimeout)
			n.mu.Lock()
			if !(ok && reply.Success) {
				debug_print(dCheckPred, "N%d in check pred, pred was N%d, but it has failed, changing pred to null", n.me, n.predecessor.Id)
				n.predecessor = NodeInfo{}
				n.predecessor.Id = -1
			}
			n.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (n *Node) Alive(args *NodeInfo, reply *RPCReply) error {
	reply.Success = true
	return nil
}

//
// Lookup a given key in the chord ring
// Returns the Ip address of the chord server with the key
// and the id of the server for debugging
//
func (n *Node) Lookup(key int) (string, int) {
	var args NodeInfo
	var reply RPCReply
	args.Id = key
	n.mu.Lock()
	debug_print(dFinds, "N%d in lookup, about to call findsuccessor", n.me)
	n.mu.Unlock()

	n.FindSuccessor(&args, &reply)
	return reply.Addr, reply.Id
}

//
// Lets the caller know if this chord node is responsible for the
// specific key.
//
func (n *Node) IsMyKey(key int) bool {
	return false
}

func (n *Node) MyId() int {
	return n.me
}

func (n *Node) MyRawId() string {
	// TODO: Discuss with Chen and fix
	return ""
}
