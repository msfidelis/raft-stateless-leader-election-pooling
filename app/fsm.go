package main

import (
	"io"

	"github.com/hashicorp/raft"
)

// noopFSM is a minimal state machine required by the raft.Raft interface.
// This POC only needs Raft's leader election, not a real replicated state
// machine, so Apply/Snapshot/Restore do nothing.
type noopFSM struct{}

func (f *noopFSM) Apply(*raft.Log) any { return nil }

func (f *noopFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &noopSnapshot{}, nil
}

func (f *noopFSM) Restore(rc io.ReadCloser) error {
	return rc.Close()
}

type noopSnapshot struct{}

func (s *noopSnapshot) Persist(sink raft.SnapshotSink) error { return sink.Close() }

func (s *noopSnapshot) Release() {}
