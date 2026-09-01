package main

import (
	"encoding/json"
	"net/http"

	"github.com/hashicorp/raft"
)

type statusResponse struct {
	NodeID        string   `json:"node_id"`
	State         string   `json:"state"`
	LeaderAddress string   `json:"leader_address"`
	Peers         []string `json:"peers"`
}

// statusHandler implements the GET /status contract (RF06): current node
// state, known leader address and the static peer set.
func statusHandler(nodeID string, r *raft.Raft, peers PeerList) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		addrs := make([]string, 0, len(peers))
		for _, addr := range peers {
			addrs = append(addrs, addr)
		}

		leaderAddr, _ := r.LeaderWithID()

		resp := statusResponse{
			NodeID:        nodeID,
			State:         r.State().String(),
			LeaderAddress: string(leaderAddr),
			Peers:         addrs,
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(resp)
	}
}
