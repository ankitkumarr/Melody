# Melody

Melody is a decentralized peer-to-peer network for media sharing. Centralized authorities can be built on top of the Melody to provide key features such as encouraging user seeding, enabling better search, and moderating content.

This repository holds the source code for Melody and [a sample Authority (Melority)](https://github.com/ankitkumarr/Melody/tree/main/src/authority).

To start a Melody network, one needs to do the following:

```
cd src/main
go build main.go
./main <unique-id> <port> true
```

Example first server run above: `./main 11 8001 true` (`true` indicates a new network / chain)

To connect additional servers to the network, one should start more servers, and provide the address of another server in the network. Example -

```
cd src/main
go build main.go
./main <unique-id> <port> false 11 127.0.0.1:<port-1>
```

Example another server join: `./main 22 8002 false 11 127.0.0.1:8001` (`false` indicates a no new network / ring)

To access the client, open a connection to either of the servers from your browser. Example `http://localhost:8001`.

A Melody server has three components - Chord node, DHT node, and the App node. These components make a Melody server binary. A Melody server uses its DHT node to store file metadata and search keyword lookup in the DHT. The DHT node uses its Chord node to lookup other DHT nodes in the network.

Our tests are structured as follows:
- `src/chord/test_test.go` has chord specific tests.
- `test/test_dht_basic_e2e.sh` has basic E2E tests for the DHT.
- `test/test_dht_failed_nodes_e2e.sh` has E2E tests that provide some basic coverage for nodes joining and leaving the network.
