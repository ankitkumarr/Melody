package chord

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"sync/atomic"
)

const RpcTimeout = 5 * time.Second

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
	me          int        // id of the node, lives in range [0, 2^m-1]
	addr        string     // the node's own address
	finger      []NodeInfo // finger table
	successor   NodeInfo   // list of r successors, see sec E.3 of chord paper
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

func (n *Node) killed() bool {
	z := atomic.LoadInt32(&n.dead)
	return z == 1
}

// initialize a new chord node
func Make(me int, addr string, m int, createRing bool, joinNodeId int, joinNodeAddr string) *Node {
	n := &Node{}
	n.me = me
	n.addr = addr
	n.finger = make([]NodeInfo, m)
	n.ring_size = int(math.Pow(2, float64(m+1)))
	newServer := rpc.NewServer()
	newServer.Register(n)
	rpcPath := "/_goRPC_" + strconv.Itoa(me)
	debugPath := "/debug/rpc_" + strconv.Itoa(me)
	newServer.HandleHTTP(rpcPath, debugPath)
	l, e := net.Listen("tcp", addr)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// go http.Serve(l, nil)
	var httpServer http.Server
	go httpServer.Serve(l)

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
	// go n.check_predecessor_ticker()
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

// check if end - start = 1, mod length
// need this because sometimes we need the range (start, end)
// if end-start = 1, this will mess up the usage of isInRange
// func isDiffOne(start int, end int, length int) bool {
// 	diff := (end - start) % length
// 	if diff == 1 || diff == length-1 {
// 		return true
// 	} else {
// 		return false
// 	}
// }

// create a new chord ring
func (n *Node) Create() {
	// clear the predecessor
	n.mu.Lock()
	n.predecessor = NodeInfo{}
	n.predecessor.Id = -1

	n.successor.Addr = n.addr
	n.successor.Id = n.me
	debug_print(dCreate, "N%d just created ring", n.me)
	n.mu.Unlock()
}

func (n *Node) FindSuccessor(args *NodeInfo, reply *RPCReply) error {
	n.mu.Lock()
	isSuccessor := isInRange(n.me+1, n.successor.Id, args.Id, n.ring_size)
	debug_print(dFinds, "N%d received findsuccessor request with id %d, cur suc %d, cur pred %d, is cur suc the successor of id? %v", n.me, args.Id, n.successor.Id, n.predecessor.Id, isSuccessor)
	if isSuccessor {
		reply.Addr = n.successor.Addr
		reply.Id = n.successor.Id
		reply.Success = true
		debug_print(dFinds, "N%d received findsuccessor request with id %d, cur suc %d, cur pred %d, cur suc is successor, returning", n.me, args.Id, n.successor.Id, n.predecessor.Id)
		defer n.mu.Unlock()
		return nil
	} else {
		n_preced := n.closest_preceding_node(args.Id)
		debug_print(dFinds, "N%d received findsuccessor request with id %d, cur suc %d, cur pred %d, closest preceding node is %v", n.me, args.Id, n.successor.Id, n.predecessor.Id, n_preced.Id)
		n.mu.Unlock()

		// call n_preced.FindSuccessor
		for {
			var reply_inner *NodeInfo
			rpcPath := "/_goRPC_" + strconv.Itoa(n_preced.Id)
			ok := Call(n_preced.Addr, rpcPath, "Node.FindSuccessor", &args, &reply_inner, RpcTimeout)
			if ok {
				reply.Addr = reply_inner.Addr
				reply.Id = reply_inner.Id
				reply.Success = true
				return nil
			}
		}
	}
}

// search local table for highest predecessor of id, not called by RPC
// assume lock is held when called
func (n *Node) closest_preceding_node(id int) NodeInfo {
	var n_preced NodeInfo
	// for i := len(n.finger) - 1; i >= 0; i-- {
	// 	if n.finger[i].Addr != "" && n.finger[i].Id < id {
	// 		n_preced.Addr = n.finger[i].Addr
	// 		n_preced.Id = n.finger[i].Id
	// 		return n_preced
	// 	}
	// }
	// for i := len(n.finger) - 1; i >= 0; i-- {
	// 	if n.finger[i].Addr != "" {
	// 		is_pred := isInRange(n.me+1, id-1, n.finger[i].Id, n.ring_size) && !isDiffOne(n.me, id, n.ring_size)
	// 		if is_pred {
	// 			n_preced.Addr = n.finger[i].Addr
	// 			n_preced.Id = n.finger[i].Id
	// 			return n_preced
	// 		}
	// 	}
	// }
	is_pred := isInRange(n.me+1, id-1, n.successor.Id, n.ring_size) && (id-n.me != 1) && !(n.me == n.ring_size-1 && n.successor.Id == 0)
	if is_pred {
		n_preced.Addr = n.successor.Addr
		n_preced.Id = n.successor.Id
		return n_preced
	}
	n_preced.Addr = n.addr
	n_preced.Id = n.me
	return n_preced
}

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
		ok := Call(n_current.Addr, rpcPath, "Node.FindSuccessor", &args, &reply, RpcTimeout)
		debug_print(dJoin, "received findsuccessor response, success? %v", ok)
		if ok {
			n.mu.Lock()
			n.successor.Addr = reply.Addr
			n.successor.Id = reply.Id
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
		onlyNode := (n.successor.Id == n.me) && (n.predecessor.Addr == "")
		debug_print(dStable, "N%d in stabilize, onlynode? %v", n.me, onlyNode)
		n.mu.Unlock()
		if !onlyNode {
			var args NodeInfo
			var reply RPCReply
			n.mu.Lock()
			successor_addr := n.successor.Addr
			rpcPath := "/_goRPC_" + strconv.Itoa(n.successor.Id)
			debug_print(dStable, "N%d in stabilize, calling suc N%d at addr %v to get suc.pred", n.me, n.successor.Id, n.successor.Addr)
			n.mu.Unlock()
			ok := Call(successor_addr, rpcPath, "Node.GetPredecessor", &args, &reply, RpcTimeout)
			if ok {
				n.mu.Lock()
				if reply.Addr != "" {
					is_suc := isInRange(n.me+1, n.successor.Id-1, reply.Id, n.ring_size) && (n.successor.Id-n.me != 1) && !(n.me == n.ring_size-1 && n.successor.Id == 0)
					if is_suc {
						debug_print(dStable, "N%d in stabilize, prev suc is N%d, but suc.pred is N%d, so will change successor", n.me, n.successor.Id, reply.Id)
						n.successor.Addr = reply.Addr
						n.successor.Id = reply.Id
					}
				}
				successor_addr := n.successor.Addr
				rpcPath := "/_goRPC_" + strconv.Itoa(n.successor.Id)
				args.Addr = n.addr
				args.Id = n.me
				debug_print(dStable, "N%d in stabilize, calling notify to suc N%d", n.me, n.successor.Id)
				n.mu.Unlock()
				var reply RPCReply
				_ = Call(successor_addr, rpcPath, "Node.Notify", &args, &reply, RpcTimeout)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// to accomodate successor lists, should return list of successors
func (n *Node) Notify(args *NodeInfo, reply *RPCReply) error {
	n.mu.Lock()
	debug_print(dNotify, "N%d received notify from N%d, cur pred is %d", n.me, args.Id, n.predecessor.Id)
	is_pred := isInRange(n.predecessor.Id+1, n.me-1, args.Id, n.ring_size) && (n.me-n.predecessor.Id != 1) && !(n.predecessor.Id == n.ring_size-1 && n.me == 0)
	if (n.predecessor.Id < 0) || is_pred {
		n.predecessor.Addr = args.Addr
		n.predecessor.Id = args.Id
	}
	debug_print(dNotify, "N%d received notify from N%d, changed pred to %d, returning", n.me, args.Id, n.predecessor.Id)
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
		args.Id = (n.me + int(math.Pow(2, float64(n.next)))) % n.ring_size
		if args.Id < 0 {
			args.Id += n.ring_size
		}
		n.mu.Unlock()
		n.FindSuccessor(&args, &reply)
		n.mu.Lock()
		n.finger[n.next].Addr = reply.Addr
		n.finger[n.next].Id = reply.Id
		n.mu.Unlock()
		time.Sleep(5 * time.Millisecond)
	}
}

// go routine that calls predecessor periodically to check if it has failed
func (n *Node) check_predecessor_ticker() {
	for !n.killed() {
		n.mu.Lock()
		pred_addr := n.predecessor.Addr
		rpcPath := "/_goRPC_" + strconv.Itoa(n.predecessor.Id)
		n.mu.Unlock()
		if pred_addr != "" {
			var args NodeInfo
			var reply RPCReply
			ok := Call(pred_addr, rpcPath, "Node.Alive", &args, &reply, RpcTimeout)
			n.mu.Lock()
			if !(ok && reply.Success) {
				var null_node NodeInfo
				n.predecessor = null_node
			}
			n.mu.Unlock()
		}
		time.Sleep(5 * time.Millisecond)
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
	// log.Printf("about to call addr %v, rpc %v, args %v, reply %v\n", address, rpcname, args, reply)
	c, err := rpc.DialHTTPPath("tcp", address, path)
	// log.Printf("just called, err %v", err)
	if err != nil {
		log.Fatalf("Failed to connect to server with address: %v. %v", address, err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		ch <- true
	} else {
		// fmt.Println(err)
		log.Printf("called addr %v, rpc %v, with args %v, reply %v, err %v", address, rpcname, args, reply, err)
		ch <- false
	}
}
