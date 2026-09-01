package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
)

type Event struct {
	ID        string         `json:"id"`
	EventType string         `json:"event_type"`
	Payload   map[string]any `json:"payload"`
	Timestamp string         `json:"timestamp"`
}

var eventTypes = []string{"user.created", "order.updated", "payment.processed", "session.expired"}

func randomEvent() Event {
	return Event{
		ID:        uuid.NewString(),
		EventType: eventTypes[rand.Intn(len(eventTypes))],
		Payload: map[string]any{
			"actor":   gofakeit.Name(),
			"email":   gofakeit.Email(),
			"company": gofakeit.Company(),
			"amount":  gofakeit.Price(1, 1000),
		},
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
}

// pollHandler implements a real long-polling endpoint: it holds the
// connection open until a random "new event" delay elapses, or until
// maxWait is reached (in which case it replies 204 with no body).
func pollHandler(minDelay, maxDelay, maxWait time.Duration) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		delay := minDelay + time.Duration(rand.Int63n(int64(maxDelay-minDelay+1)))

		select {
		case <-time.After(delay):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(randomEvent())
		case <-time.After(maxWait):
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
			return
		}
	}
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func main() {
	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8090"
	}

	minDelay := envDuration("POLL_MIN_DELAY", 500*time.Millisecond)
	maxDelay := envDuration("POLL_MAX_DELAY", 1*time.Second)
	maxWait := envDuration("POLL_MAX_WAIT", 1*time.Second)

	http.HandleFunc("/poll", pollHandler(minDelay, maxDelay, maxWait))
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("mock-server listening on %s (delay %s-%s, maxWait %s)", addr, minDelay, maxDelay, maxWait)
	log.Fatal(http.ListenAndServe(addr, nil))
}
