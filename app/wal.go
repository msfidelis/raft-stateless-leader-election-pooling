package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.etcd.io/bbolt"
)

var eventsBucket = []byte("events")

// OpenWAL opens (creating if needed) a local bbolt file used as an
// append-only event log (RF08-RF11). Fsync is left at bbolt's default
// (NoSync: false), so a transaction is only considered committed once it
// has hit disk.
func OpenWAL(path string) (*bbolt.DB, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("create wal dir %q: %w", dir, err)
		}
	}

	db, err := bbolt.Open(path, 0o600, &bbolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open bbolt db %q: %w", path, err)
	}

	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(eventsBucket)
		return err
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("create events bucket: %w", err)
	}

	return db, nil
}

// SaveEvent appends one event to the WAL, keyed by its own "id" field
// (RF08/RF09). The key is the event's uuid, not a sequential offset, so
// bucket iteration order does not reflect arrival order — readers that need
// chronological order must sort by the event's own "timestamp" field.
func SaveEvent(db *bbolt.DB, event map[string]any) error {
	id, _ := event["id"].(string)
	if id == "" {
		return fmt.Errorf("event has no string \"id\" field")
	}

	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	return db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(eventsBucket).Put([]byte(id), value)
	})
}
