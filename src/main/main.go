package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"

	"Melody/chord"
	"Melody/common"
	"Melody/dht"
	"Melody/melody"
)

func main() {
	if len(os.Args) < 4 {
		log.Fatalf("Usage: ./main <unique-id> <port> <createRing> [joinNodeId] [joinNodeAddr]")
	}
	chord_id := os.Args[1]
	chord_hashed_id := common.KeyHash(chord_id)
	// log.Printf("CHORD HASHED: %v and ID %v", chord_hashed_id, chord_id)
	port := os.Args[2]
	create, err := strconv.ParseBool(os.Args[3])
	if err != nil {
		log.Fatalf(err.Error())
	}

	my_address := "127.0.0.1:" + port
	joinNodeId := ""
	joinNodeIdHashed := -1
	joinNodeAdd := my_address
	if len(os.Args) > 4 {
		if len(os.Args) < 6 {
			log.Fatalf("Must have both [joinNodeId] [joinNodeAddr] or None")
		}
		joinNodeId = os.Args[4]
		joinNodeIdHashed = common.KeyHash(joinNodeId)
		joinNodeAdd = os.Args[5]
	}

	// Create DHT and start chord here.
	// Channel needs at least 1 buffer for Chord to start up without blocking.
	// But might as well give a generous buffer to avoid blocks
	chordChangeCh := make(chan chord.ChangeNotifyMsg, 100)
	ch := chord.Make(chord_hashed_id, chord_id, my_address, 31, create, joinNodeIdHashed, joinNodeAdd, chordChangeCh)
	dht := dht.Make(ch, chord_id, my_address, chordChangeCh)
	melody.Make(dht, chord_hashed_id, my_address)

	httpServer(port)
}

func httpServer(port string) {
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 100
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", "127.0.0.1:"+port)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}
