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
	m := 4
	n_nodes := 5
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
		addr := "127.0.0.1:" + strconv.Itoa(7000+ids[i])
		// httpServer(addr)
		if i == 0 {
			n_new := Make(ids[i], addr, m-1, true, -1, "")
			chord_nodes[i] = n_new
		} else {
			n_new := Make(ids[i], addr, m-1, false, ids[0], chord_nodes[0].addr)
			chord_nodes[i] = n_new
		}
		// wait for create or join to complete
		time.Sleep(20 * time.Millisecond)

		log.Printf("current chord ring contains %v\n", ids[:(i+1)])
		log.Printf("shuffled list is %v\n", a)

		// do some lookups
		for j := 0; j < 5; j++ {
			query_node := rand.Intn(i + 1)
			keyN := rand.Intn(ring_size)
			key := strconv.Itoa(keyN)
			// log.Printf("iteration %v, # %v, key value %v\n", i, j, keyN)
			_, suc_id := chord_nodes[query_node].Lookup(key)

			if i == 0 {
				if suc_id != ids[0] {
					log.Fatalf("Lookup for key %v failed: only have one node with id %v, returned id %v\n", key, ids[0], suc_id)
				}
			} else {
				// find the successor of key manually
				min := ids[0]
				suc := ring_size
				for id := range ids {
					if id > keyN && id < suc {
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
			}
		}
	}
	log.Println("Test chord basic: success")
}
