package chord

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func GenericTest(t *testing.T, leave bool) {
	start := time.Now()
	m := 5
	n_nodes := 10
	if !leave {
		fmt.Printf("Test Chord Basic: %d nodes, successive join, no leaves\n", n_nodes)
	} else {
		fmt.Printf("Test Chord Leave: %d nodes, successive join, then random leave + join\n", n_nodes)
	}
	ring_size := int(math.Pow(2, float64(m)))
	a := make([]int, ring_size)
	for i := 0; i < ring_size; i++ {
		a[i] = i
	}
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] })
	ids := a[:n_nodes]

	chord_nodes := make([]*Node, n_nodes)
	http_servers := make([]http.Server, n_nodes)
	listeners := make([]net.Listener, n_nodes)
	for i := 0; i < n_nodes; i++ {
		log.Printf("starting node %v\n", i)
		addr := "127.0.0.1:" + strconv.Itoa(8000+ids[i])
		// httpServer(addr)
		if i == 0 {
			chord_nodes[i] = Make(ids[i], addr, m, true, -1, "")
			listeners[i], http_servers[i] = startNewServer(addr)
		} else {
			chord_nodes[i] = Make(ids[i], addr, m, false, ids[0], chord_nodes[0].addr)
			listeners[i], http_servers[i] = startNewServer(addr)
		}
		// wait for create or join to complete
		time.Sleep(100 * time.Millisecond)
		if i > 0 {
			for {
				if time.Since(start) > (20 * time.Second) {
					log.Fatalf("Taking too long\n")
				}
				cont := true
				preds := make([]int, i+1)
				sucs := make([]int, i+1)
				pred_counts := make(map[int]int)
				suc_counts := make(map[int]int)
				for j, node := range chord_nodes[:(i + 1)] {
					node.mu.Lock()
					preds[j] = node.predecessor.Id
					sucs[j] = node.successors[0].Id
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
			log.Printf("iteration %v, # %v, key value %v\n", i, j, keyN)
			_, suc_id := chord_nodes[query_node].Lookup(keyN)

			if i == 0 {
				if suc_id != ids[0] {
					log.Fatalf("Lookup for key %v failed: only have one node with id %v, returned id %v\n", keyN, ids[0], suc_id)
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

	if leave {
		log.Println("shutting down nodes sequentially")
		//reshuffle the order, and shut down the servers one by one
		to_leave_ids := make([]int, n_nodes)
		copy(to_leave_ids, a[:n_nodes])
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(to_leave_ids), func(i, j int) { to_leave_ids[i], to_leave_ids[j] = to_leave_ids[j], to_leave_ids[i] })

		for i := 0; i < n_nodes-2; i++ {
			log.Printf("shutting down node %v\n", to_leave_ids[i])
			// to_leave_ids has been shuffled, so need to find the original index of the node
			var index int
			for ind, old_id := range ids {
				if old_id == to_leave_ids[i] {
					index = ind
					break
				}
			}
			log.Printf("finding original ind of %d-th node N%d, to_leave_ids is %v, old ids is %v, original ind is %d\n", i, to_leave_ids[i], to_leave_ids, ids, index)
			// if err := http_servers[index].Shutdown(context.Background()); err != nil {
			// 	log.Printf("HTTP server N%d shutdown: %v\n", to_leave_ids[i], err)
			// }
			listeners[index].Close()
			log.Printf("just shut down N%d listener\n", to_leave_ids[i])
			// http_servers[index].Close()
			log.Printf("just shut down N%d http server\n", to_leave_ids[i])
			chord_nodes[index].Kill()
			log.Printf("just shut down N%d\n", to_leave_ids[i])
			// wait for create or join to complete
			time.Sleep(100 * time.Millisecond)
			for {
				if time.Since(start) > (20 * time.Second) {
					log.Fatalf("Taking too long\n")
				}
				cont := true
				preds := make([]int, n_nodes-1-i)
				sucs := make([]int, n_nodes-1-i)
				pred_counts := make(map[int]int)
				suc_counts := make(map[int]int)
				for j, id := range to_leave_ids[(i + 1):] {
					// to_leave_ids has been shuffled, so need to find the original index
					log.Printf("checking pred suc for N%d, at index %d", id, j)
					var index int
					for ind, old_id := range ids {
						if old_id == id {
							index = ind
							break
						}
					}
					chord_nodes[index].mu.Lock()
					preds[j] = chord_nodes[index].predecessor.Id
					sucs[j] = chord_nodes[index].successors[0].Id
					chord_nodes[index].mu.Unlock()
					log.Printf("checking pred suc for N%d, obtained pred and suc", id)
					if preds[j] == -1 {
						cont = false
					}
					for k := 0; k <= i; k++ {
						if preds[j] == to_leave_ids[k] || sucs[j] == to_leave_ids[k] {
							cont = false
						}
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
					log.Printf("chord has stabilized after N%d left, current ring %v, current predecessors %v, current successors %v", to_leave_ids[i], to_leave_ids[(i+1):], preds, sucs)
					break
				}
				log.Printf("chord still waiting to stabilize after N%d left, current ring %v, current predecessors %v, current successors %v", to_leave_ids[i], to_leave_ids[(i+1):], preds, sucs)
				time.Sleep(100 * time.Millisecond)
			}

			log.Printf("current chord ring contains %v\n", to_leave_ids[(i+1):])
			log.Printf("shuffled list is %v\n", to_leave_ids)

			// do some lookups
			for j := 0; j < 5; j++ {
				query_ind := rand.Intn(n_nodes-i-1) + i
				query_id := to_leave_ids[query_ind]
				var index int
				for ind, old_id := range ids {
					if old_id == query_id {
						index = ind
						break
					}
				}
				keyN := rand.Intn(ring_size)
				log.Printf("iteration after shutdown %v, # %v, key value %v\n", i, j, keyN)
				_, suc_id := chord_nodes[index].Lookup(keyN)

				if i == n_nodes-2 {
					if suc_id != to_leave_ids[n_nodes-1] {
						log.Fatalf("Lookup for key %v failed: only have one node with id %v, returned id %v\n", keyN, to_leave_ids[n_nodes-1], suc_id)
					}
				} else {
					// find the successor of key manually
					min := to_leave_ids[i+1]
					suc := ring_size
					cur_ring := to_leave_ids[(i + 1):]
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
						log.Fatalf("Lookup for key %v failed: current id list %v, returned id %v, should have returned %v\n", keyN, to_leave_ids[(i+1):], suc_id, suc)
					}
					log.Printf("Lookup for key %v: current id list %v, returned id %v, should have returned %v\n", keyN, to_leave_ids[(i+1):], suc_id, suc)
				}
			}
		}
	}
	log.Println("Test chord basic: success")
}

func startNewServer(addr string) (net.Listener, http.Server) {
	l, e := net.Listen("tcp", addr)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// go http.Serve(l, nil)
	var httpServer http.Server
	go httpServer.Serve(l)
	return l, httpServer
}

func TestChordBasic(t *testing.T) {
	GenericTest(t, false)
}

func TestChordLeave(t *testing.T) {
	GenericTest(t, true)
}
