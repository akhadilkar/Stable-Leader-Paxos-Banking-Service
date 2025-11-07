// Acknowledgements:
// I used google and openai for learning more about theory and edge test cases related to stable leder paxos.
// Also, as I was not familiar with Golang I refred to some online resources to understand the syntax and structure of golang.

package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"
)

func main() {
	id := flag.Int("id", 0, "node id (1..5)")
	addr := flag.String("addr", "", "listen addr, e.g. :8001")
	base := flag.Int("base", 8001, "base port for peers (default 8001 => :8001..:8005)")
	total_nodes := flag.Int("n", 5, "total nodes (default 5)")
	role := flag.String("role", "node", "node | client")
	csvPath := flag.String("csv", "", "csv file for driver")
	flag.Parse()

	if *role == "driver" {
		if *csvPath == "" {
			log.Fatal("driver requires -csv=<file>")
		}
		runDriver(*csvPath, *base, *total_nodes)
		return
	}

	if *role == "client" || *role == "clients" {
		runClientsRepl(*base, *total_nodes)
		return
	}

	if *id < 1 || *id > *total_nodes || *addr == "" {
		log.Fatalf("usage: -id=1..%d -addr=:<port> [-base=%d -n=%d]", *total_nodes, *base, *total_nodes)
	}

	peers := make([]Peer, *total_nodes)
	for i := 0; i < *total_nodes; i++ {
		peers[i] = Peer{ID: i + 1, Addr: fmt.Sprintf(":%d", *base+i)}
	}

	n := &Node{
		id:           *id,
		addr:         *addr,
		peers:        peers,
		bank:         make(map[string]int64),
		isLeader:     false,
		NextSeq:      1,
		CommitIndex:  0,
		ExecuteIndex: 0,
		AcceptLog:    make(map[int]AcceptLogEntry),
		SlotStatus:   make(map[int]SlotStatus),
		LastTau:      make(map[ClientID]int64),
		LastReply:    make(map[ClientID]ReplyArgs),
		PendingSeqs:  make([]int, 0),
	}
	n.lastLeaderMsg = time.Now()
	n.lastPrepareSeen = time.Now()

	n.initAccounts()

	srv := rpc.NewServer()
	if err := srv.RegisterName("Node", n); err != nil {
		log.Fatalf("rpc register: %v", err)
	}

	lis, err := net.Listen("tcp", n.addr)
	if err != nil {
		log.Fatalf("listen %s: %v", n.addr, err)
	}

	log.Printf("[node %d] listening on %s", n.id, n.addr)

	go repl(n)
	go n.electionLoop()

	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}

		go srv.ServeConn(conn)
	}

}
