package main

import (
	"log"
	"net/http"
	"os"
	"strconv"
)

func mustEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func nodeID() string {
	if v := os.Getenv("RAFT_NODE_ID"); v != "" {
		return v
	}
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("RAFT_NODE_ID not set and failed to read hostname: %v", err)
	}
	return hostname
}

func main() {
	id := nodeID()
	bindAddr := mustEnv("RAFT_BIND_ADDR", id+":7000")
	httpAddr := mustEnv("HTTP_ADDR", ":8080")
	mockServerURL := mustEnv("MOCK_SERVER_URL", "http://mock-server:8090/poll")
	pollWorkers := envInt("POLL_WORKERS", 8)
	walPath := mustEnv("WAL_PATH", "/data/wal.db")

	peersEnv := os.Getenv("RAFT_PEERS")
	if peersEnv == "" {
		log.Fatal("RAFT_PEERS is required, e.g. node1=node1:7000,node2=node2:7000,node3=node3:7000")
	}
	peers, err := parsePeers(peersEnv)
	if err != nil {
		log.Fatalf("invalid RAFT_PEERS: %v", err)
	}

	wal, err := OpenWAL(walPath)
	if err != nil {
		log.Fatalf("[%s] failed to open wal: %v", id, err)
	}
	defer wal.Close()

	r, err := newRaftNode(id, bindAddr, peers)
	if err != nil {
		log.Fatalf("[%s] failed to start raft node: %v", id, err)
	}

	go leaderSemaphore(id, r, mockServerURL, pollWorkers, wal)

	http.HandleFunc("/status", statusHandler(id, r, peers))
	http.HandleFunc("/events", eventsHandler(wal))

	log.Printf("[%s] http status API listening on %s (raft bind %s)", id, httpAddr, bindAddr)
	log.Fatal(http.ListenAndServe(httpAddr, nil))
}
