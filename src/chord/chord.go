package chord

import (
	"math"
	"net/rpc"
	"sync"

	network "../common"
	"sync/atomic"
	"time"
)

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

// create a new chord ring
func (n *Node) Create(address string) {
	// clear the predecessor
	var n_pred NodeInfo
	n.predecessor = n_pred
	n.addr = address

	n.successor.Addr = n.addr
	n.successor.Id = n.me

	rpc.Register(n)
}

func (n *Node) FindSuccessor(args *NodeInfo, reply *RPCReply) {
	n.mu.Lock()
	if args.Id > n.me && args.Id <= n.successor.Id {
		reply.Addr = n.successor.Addr
		reply.Id = n.successor.Id
		reply.Success = true
		defer n.mu.Unlock()
		return
	} else {
		n_preced := n.closest_preceding_node(args.Id)
		n.mu.Unlock()

		// call n_preced.FindSuccessor
		for {
			var reply_inner *NodeInfo
			ok := network.Call(n_preced.Addr, "Node.FindSuccessor", args, reply_inner)
			if ok {
				reply.Addr = reply_inner.Addr
				reply.Id = reply_inner.Id
				reply.Success = true
				return
			}
		}
	}
}

// search local table for highest predecessor of id, not called by RPC
// assume lock is held when called
func (n *Node) closest_preceding_node(id int) NodeInfo {
	var n_preced NodeInfo
	for i := len(n.finger) - 1; i >= 0; i-- {
		if n.finger[i].Addr != "" && n.finger[i].Id < id {
			n_preced.Addr = n.finger[i].Addr
			n_preced.Id = n.finger[i].Id
			return n_preced
		}
	}
	n_preced.Addr = n.addr
	n_preced.Id = n.me
	return n_preced
}

// join a chord ring containing n_current
func (n *Node) join(n_current *NodeInfo) {
	n.mu.Lock()
	var n_pred NodeInfo
	n.predecessor = n_pred
	var args *NodeInfo
	args.Id = n.me
	args.Addr = n.addr
	n.mu.Unlock()

	for {
		var reply RPCReply
		ok := network.Call(n_current.Addr, "Node.FindSuccessor", args, reply)
		if ok {
			n.mu.Lock()
			n.successor.Addr = reply.Addr
			n.successor.Id = reply.Id
			n.mu.Unlock()
			return
		}
	}
}

func (n *Node) GetPredecessor(args *NodeInfo, reply *RPCReply) {
	n.mu.Lock()
	reply.Addr = n.predecessor.Addr
	reply.Id = n.predecessor.Id
	reply.Success = true
	defer n.mu.Unlock()
	return
}

func (n *Node) stabilize_ticker() {
	for !n.killed() {
		var args NodeInfo
		var reply RPCReply
		n.mu.Lock()
		successor_addr := n.successor.Addr
		n.mu.Unlock()
		ok := network.Call(successor_addr, "Node.GetPredecessor", args, reply)
		if ok {
			n.mu.Lock()
			if reply.Id > n.me && reply.Id < n.successor.Id {
				n.successor.Addr = reply.Addr
				n.successor.Id = reply.Id
			}
			successor_addr := n.successor.Addr
			args.Addr = n.addr
			args.Id = n.me
			n.mu.Unlock()
			var reply RPCReply
			_ = network.Call(successor_addr, "Node.Notify", args, reply)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (n *Node) Notify(args *NodeInfo, reply *RPCReply) {
	n.mu.Lock()
	if (n.predecessor.Addr == "") || (args.Id > n.predecessor.Id && args.Id < n.me) {
		n.predecessor.Addr = args.Addr
		n.predecessor.Id = args.Id
	}
	reply.Success = true
	defer n.mu.Unlock()
	return
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
		args.Id = (n.me + int(math.Pow(2, float64(n.next)))) % int(math.Pow(2, float64(len(n.finger))))
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
		n.mu.Unlock()
		if pred_addr != "" {
			var args NodeInfo
			var reply RPCReply
			ok := call(pred_addr, "Node.Alive", &args, &reply)
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

func (n *Node) Alive(args *NodeInfo, reply *RPCReply) {
	reply.Success = true
}

//
// Lookup a given key in the chord ring
// Returns the Ip address of the chord server with the key
//
func (n *Node) Lookup(key string) string {
	return ""
}

//
// Lets the caller know if this chord node is responsible for the
// specific key.
//
func (n *Node) IsMyKey(key string) bool {
	return false
}
