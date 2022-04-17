package chord

import (
	"log"
	"math"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestChordBasic(t *testing.T) {
	m := 4
	n_nodes := 5
	ring_size := int(math.Pow(2, float64(m)))
	a := make([]int, ring_size)
	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] })
	ids := a[:n_nodes]

	chord_nodes := make([]*Node, n_nodes)
	for i := 0; i < n_nodes; i++ {
		if i == 0 {
			n_new := Make(ids[i], "127.0.0.1:"+strconv.Itoa(ids[i]), true, -1, "")
			chord_nodes[i] = n_new
		} else {
			n_new := Make(ids[i], "127.0.0.1:"+strconv.Itoa(ids[i]), false, ids[0], chord_nodes[0].addr)
			chord_nodes[i] = n_new
		}
		// wait for create or join to complete
		time.Sleep(20 * time.Millisecond)

		// log.Printf("current chord ring contains %v\n", chord_nodes)

		// do some lookups
		for j := 0; j < 5; j++ {
			query_node := rand.Intn(i + 1)
			keyN := rand.Intn(ring_size)
			key := strconv.Itoa(keyN)
			_, suc_id := chord_nodes[query_node].Lookup(key)

			if i == 0 {
				if suc_id != 0 {
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
					log.Fatalf("Lookup for key %v failed: current id list %v, returned id %v, should have returned %v\n", keyN, ids[:i], suc_id, suc)
				}
			}
		}
	}
	log.Println("Test chord basic: success")
}
