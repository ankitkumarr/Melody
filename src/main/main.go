package main

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"

	"Melody/chord"
	"Melody/dht"
	"Melody/melody"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatalf("Usage: ./main <port>")
	}
	port := os.Args[1]

	// Create DHT and start chord here.
	ch := chord.Node{}
	ch.Create("127.0.0.1:" + port)
	dht := dht.Make(&ch)
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
	go http.Serve(l, nil)
}
