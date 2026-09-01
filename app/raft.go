package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

// PeerList maps a Raft node ID to its advertised TCP address (host:port).
type PeerList map[string]string

// parsePeers parses "id1=addr1,id2=addr2,..." as configured via RAFT_PEERS.
func parsePeers(s string) (PeerList, error) {
	peers := PeerList{}
	for _, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid peer entry %q, expected id=addr", pair)
		}
		peers[parts[0]] = parts[1]
	}
	return peers, nil
}

// newRaftNode builds a raft.Raft instance using in-memory log/stable/snapshot
// stores (RNF02) and attempts to bootstrap the cluster with the full peer
// set known upfront (RF01). Every node calls BootstrapCluster; nodes that
// join a cluster that already has state simply get raft.ErrCantBootstrap
// back, which is safe to ignore.
func newRaftNode(nodeID, bindAddr string, peers PeerList) (*raft.Raft, error) {
	config := raft.DefaultConfig()
	config.LocalID = raft.ServerID(nodeID)

	addr, err := net.ResolveTCPAddr("tcp", bindAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve bind addr %q: %w", bindAddr, err)
	}

	transport, err := raft.NewTCPTransport(bindAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("new tcp transport: %w", err)
	}

	logStore := raft.NewInmemStore()
	stableStore := raft.NewInmemStore()
	snapshotStore := raft.NewInmemSnapshotStore()

	r, err := raft.NewRaft(config, &noopFSM{}, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("new raft: %w", err)
	}

	servers := make([]raft.Server, 0, len(peers))
	for id, address := range peers {
		servers = append(servers, raft.Server{
			ID:      raft.ServerID(id),
			Address: raft.ServerAddress(address),
		})
	}

	future := r.BootstrapCluster(raft.Configuration{Servers: servers})
	if err := future.Error(); err != nil && err != raft.ErrCantBootstrap {
		log.Printf("[%s] bootstrap skipped: %v", nodeID, err)
	}

	return r, nil
}
