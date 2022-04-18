package chord

import (
	"fmt"
	"log"
	"math"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"time"

	"sync/atomic"

	network "Melody/common"
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
	rpc.Register(n)
	l, e := net.Listen("tcp", addr)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go newServer.Accept(l)

	if createRing {
		n.Create()
	} else if joinNodeId >= 0 {
		debug_print(dJoin, "N%d just created, joining ring", me)
		joinNode := &NodeInfo{}
		joinNode.Addr = joinNodeAddr
		joinNode.Id = joinNodeId
		debug_print(dJoin, "N%d actually joining ring", me)
		n.Join(joinNode)
	}
	go n.stabilize_ticker()
	go n.check_predecessor_ticker()
	go n.fix_fingers_ticker()
	return n
}

func isInRange(start int, end int, ind int) bool {
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

// create a new chord ring
func (n *Node) Create() {
	// clear the predecessor
	n.mu.Lock()
	n.predecessor = NodeInfo{}

	n.successor.Addr = n.addr
	n.successor.Id = n.me
	debug_print(dCreate, "N%d just created ring", n.me)
	n.mu.Unlock()
}

func (n *Node) FindSuccessor(args *NodeInfo, reply *RPCReply) error {
	n.mu.Lock()
	isSuccessor := isInRange(n.me+1, n.successor.Id, args.Id)
	if isSuccessor {
		reply.Addr = n.successor.Addr
		reply.Id = n.successor.Id
		reply.Success = true
		defer n.mu.Unlock()
		return nil
	} else {
		n_preced := n.closest_preceding_node(args.Id)
		n.mu.Unlock()

		// call n_preced.FindSuccessor
		for {
			var reply_inner *NodeInfo
			ok := network.Call(n_preced.Addr, "Node.FindSuccessor", &args, &reply_inner, RpcTimeout)
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
func (n *Node) Join(n_current *NodeInfo) {
	n.mu.Lock()
	debug_print(dJoin, "N%d just started join, calling N%d", n.me, n_current.Id)
	n.predecessor = NodeInfo{}
	var args NodeInfo
	args.Id = n.me
	args.Addr = n.addr
	n.mu.Unlock()

	debug_print(dJoin, "N9 waiting for reply from N%d", n_current.Id)
	for {
		var reply RPCReply
		ok := network.Call(n_current.Addr, "Node.FindSuccessor", &args, &reply, RpcTimeout)
		if ok {
			n.mu.Lock()
			n.successor.Addr = reply.Addr
			n.successor.Id = reply.Id
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
		onlyNode := n.successor.Id == n.me
		n.mu.Unlock()
		if !onlyNode {
			var args NodeInfo
			var reply RPCReply
			n.mu.Lock()
			successor_addr := n.successor.Addr
			n.mu.Unlock()
			ok := network.Call(successor_addr, "Node.GetPredecessor", args, reply, RpcTimeout)
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
				_ = network.Call(successor_addr, "Node.Notify", args, reply, RpcTimeout)
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// to accomodate successor lists, should return list of successors
func (n *Node) Notify(args *NodeInfo, reply *RPCReply) error {
	n.mu.Lock()
	if n.successor.Id == n.me {
		n.predecessor.Addr = args.Addr
		n.predecessor.Id = args.Id

		n.successor.Addr = args.Addr
		n.successor.Id = args.Id
	} else if (n.predecessor.Addr == "") || (args.Id > n.predecessor.Id && args.Id < n.me) {
		n.predecessor.Addr = args.Addr
		n.predecessor.Id = args.Id
	}
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
		n.mu.Unlock()
		if pred_addr != "" {
			var args NodeInfo
			var reply RPCReply
			ok := network.Call(pred_addr, "Node.Alive", &args, &reply, RpcTimeout)
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
func (n *Node) Lookup(key string) (string, int) {
	keyN, _ := strconv.Atoi(key)
	var args NodeInfo
	var reply RPCReply
	args.Id = keyN
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
func (n *Node) IsMyKey(key string) bool {
	return false
}
