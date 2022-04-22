package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"

	"Melody/chord"
	"Melody/dht"
	"Melody/melody"
)

func main() {
	if len(os.Args) < 4 {
		log.Fatalf("Usage: ./main <unique-id> <port> <createRing> [joinNodeId] [joinNodeAddr]")
	}
	chord_id, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf(err.Error())
	}
	port := os.Args[2]
	create, err := strconv.ParseBool(os.Args[3])
	if err != nil {
		log.Fatalf(err.Error())
	}

	my_address := "127.0.0.1:" + port
	joinNodeId := -1
	joinNodeAdd := my_address
	if len(os.Args) > 4 {
		if len(os.Args) < 6 {
			log.Fatalf("Must have both [joinNodeId] [joinNodeAddr] or None")
		}
		joinNodeId, err = strconv.Atoi(os.Args[4])
		if err != nil {
			log.Fatalf(err.Error())
		}
		joinNodeAdd = os.Args[5]
	}

	// Create DHT and start chord here.
	ch := chord.Make(chord_id, my_address, 10, create, joinNodeId, joinNodeAdd)
	dht := dht.Make(ch, my_address)
	melody.Make(dht, my_address)

	httpServer(port)
}

func httpServer(port string) {
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", "127.0.0.1:"+port)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}
