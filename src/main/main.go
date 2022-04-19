package main

import (
	"crypto/rand"
	"log"
	"math/big"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"

	"Melody/chord"
	hash "Melody/common"
	"Melody/dht"
	"Melody/melody"
)

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: ./main <port> <createRing> [joinNodeId] [joinNodeAddr]")
	}
	port := os.Args[1]
	create, err := strconv.ParseBool(os.Args[2])
	if err != nil {
		log.Fatalf(err.Error())
	}

	// Create DHT and start chord here.
	chord_id := hash.KeyHash(string(nrand()))
	ch := chord.Make(chord_id, "127.0.0.1:"+port, 10, create, 1, "127.0.0.1:"+port)
	dht := dht.Make(ch)
	melody := melody.Make(&dht)

	httpServer(port)
}

func httpServer(port string) {
	// TODO: All the rpc struct need to be registered with rpc.Register(...)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":"+port)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}
