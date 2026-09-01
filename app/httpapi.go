package main

import (
	"encoding/json"
	"net/http"
	"sort"

	"github.com/hashicorp/raft"
	"go.etcd.io/bbolt"
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

// eventsHandler implements GET /events: lists every event persisted in this
// node's local WAL (bbolt). The bucket is keyed by event id, so it does not
// iterate in arrival order — results are sorted by each event's own
// "timestamp" field instead.
func eventsHandler(wal *bbolt.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		type record struct {
			raw       json.RawMessage
			timestamp string
		}

		var records []record
		err := wal.View(func(tx *bbolt.Tx) error {
			bucket := tx.Bucket(eventsBucket)
			if bucket == nil {
				return nil
			}
			return bucket.ForEach(func(_, v []byte) error {
				raw := append(json.RawMessage(nil), v...)

				var meta struct {
					Timestamp string `json:"timestamp"`
				}
				_ = json.Unmarshal(raw, &meta)

				records = append(records, record{raw: raw, timestamp: meta.Timestamp})
				return nil
			})
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		sort.Slice(records, func(i, j int) bool {
			return records[i].timestamp < records[j].timestamp
		})

		events := make([]json.RawMessage, len(records))
		for i, rec := range records {
			events[i] = rec.raw
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(events)
	}
}
