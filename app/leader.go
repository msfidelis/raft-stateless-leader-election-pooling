package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/hashicorp/raft"
)

// leaderSemaphore watches raft leadership transitions (RF03/RF04) and
// starts/stops the long-polling loop accordingly — only the current leader
// polls the mock-server; a node that loses leadership cancels its loop
// immediately.
func leaderSemaphore(nodeID string, r *raft.Raft, mockServerURL string, workers int) {
	var cancel context.CancelFunc
	stop := func() {
		if cancel != nil {
			cancel()
			cancel = nil
		}
	}
	defer stop()

	for isLeader := range r.LeaderCh() {
		stop()
		if isLeader {
			cancel = startPolling(nodeID, mockServerURL, workers)
		}
	}
}

// startPolling launches `workers` concurrent long-polling goroutines and
// returns a CancelFunc the caller uses to stop all of them once leadership
// is lost. Running several long-poll connections in parallel against the
// mock-server is what actually raises throughput — a single sequential loop
// is bottlenecked by the mock-server's own per-call delay.
func startPolling(nodeID, mockServerURL string, workers int) context.CancelFunc {
	ctx, cancel := context.WithCancel(context.Background())
	log.Printf("[%s] became LEADER — starting polling loop (%d concurrent workers)", nodeID, workers)

	// Shared client/transport so workers reuse keep-alive connections to
	// the mock-server instead of each dialing its own.
	client := &http.Client{
		Timeout: 40 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: workers,
		},
	}

	for i := 0; i < workers; i++ {
		go pollLoop(ctx, nodeID, mockServerURL, client)
	}
	return cancel
}

// pollLoop repeatedly long-polls the mock-server and prints received events
// (RF03/RF07) until ctx is cancelled.
func pollLoop(ctx context.Context, nodeID, url string, client *http.Client) {
	for ctx.Err() == nil {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			log.Printf("[%s] poll request error: %v", nodeID, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("[%s] poll error: %v", nodeID, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}

		handlePollResponse(nodeID, resp)
	}
}

func handlePollResponse(nodeID string, resp *http.Response) {
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		var event map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&event); err != nil {
			log.Printf("[%s] decode error: %v", nodeID, err)
			return
		}
		log.Printf("[%s][LEADER] event received: %+v", nodeID, event)
	case http.StatusNoContent:
		// no new event within the mock-server's max wait — reopen immediately
	default:
		log.Printf("[%s] unexpected poll status: %s", nodeID, resp.Status)
	}
}
