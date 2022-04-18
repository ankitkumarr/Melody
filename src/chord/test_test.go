package chord

import (
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"testing"
	"time"
)

func httpServer(port string) {
	// TODO: All the rpc struct need to be registered with rpc.Register(...)
	// mux := http.NewServeMux()
	// mux.Handler("/request", requesthandler)
	// http.ListenAndServe(":9000", nil)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", port)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func TestChordBasic(t *testing.T) {
	m := 5
	n_nodes := 15
	ring_size := int(math.Pow(2, float64(m)))
	a := make([]int, ring_size)
	for i := 0; i < ring_size; i++ {
		a[i] = i
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] })
	ids := a[:n_nodes]

	chord_nodes := make([]*Node, n_nodes)
	for i := 0; i < n_nodes; i++ {
		log.Printf("starting node %v\n", i)
		addr := "127.0.0.1:" + strconv.Itoa(8000+ids[i])
		// httpServer(addr)
		if i == 0 {
			n_new := Make(ids[i], addr, m-1, true, -1, "")
			chord_nodes[i] = n_new
		} else {
			n_new := Make(ids[i], addr, m-1, false, ids[0], chord_nodes[0].addr)
			chord_nodes[i] = n_new
		}
		// wait for create or join to complete
		time.Sleep(100 * time.Millisecond)
		if i > 0 {
			for {
				cont := true
				preds := make([]int, i+1)
				sucs := make([]int, i+1)
				pred_counts := make(map[int]int)
				suc_counts := make(map[int]int)
				for j, node := range chord_nodes[:(i + 1)] {
					node.mu.Lock()
					preds[j] = node.predecessor.Id
					sucs[j] = node.successor.Id
					node.mu.Unlock()
					if preds[j] == -1 {
						cont = false
					}
					_, ok := pred_counts[preds[j]]
					if ok {
						cont = false
					} else {
						pred_counts[preds[j]] = 1
					}

					_, ok = suc_counts[sucs[j]]
					if ok {
						cont = false
					} else {
						suc_counts[sucs[j]] = 1
					}
				}
				if cont {
					log.Printf("chord has stabilized, current ring %v, current predecessors %v, current successors %v", ids[:(i+1)], preds, sucs)
					break
				}
				log.Printf("chord still waiting to stabilize, current ring %v, current predecessors %v, current successors %v", ids[:(i+1)], preds, sucs)
				time.Sleep(100 * time.Millisecond)
			}
		}

		log.Printf("current chord ring contains %v\n", ids[:(i+1)])
		log.Printf("shuffled list is %v\n", a)

		// do some lookups
		for j := 0; j < 5; j++ {
			query_node := rand.Intn(i + 1)
			keyN := rand.Intn(ring_size)
			key := strconv.Itoa(keyN)
			log.Printf("iteration %v, # %v, key value %v\n", i, j, keyN)
			_, suc_id := chord_nodes[query_node].Lookup(key)

			if i == 0 {
				if suc_id != ids[0] {
					log.Fatalf("Lookup for key %v failed: only have one node with id %v, returned id %v\n", key, ids[0], suc_id)
				}
			} else {
				// find the successor of key manually
				min := ids[0]
				suc := ring_size
				cur_ring := ids[:(i + 1)]
				// log.Printf("finding valid successor for key %v, cur ring %v\n", keyN, cur_ring)
				for _, id := range cur_ring {
					// log.Printf("node %d\n", id)
					if id >= keyN && id < suc {
						suc = id
					}
					if id < min {
						min = id
					}
				}
				if suc == ring_size {
					suc = min
				}
				if suc != suc_id {
					log.Fatalf("Lookup for key %v failed: current id list %v, returned id %v, should have returned %v\n", keyN, ids[:(i+1)], suc_id, suc)
				}
				log.Printf("Lookup for key %v: current id list %v, returned id %v, should have returned %v\n", keyN, ids[:(i+1)], suc_id, suc)
			}
		}
	}
	log.Println("Test chord basic: success")
}
