package raft

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

// Capturing snapshot store + FSM that actually roundtrip bytes, so a test can
// assert the exact payload handed to FSM.Restore after an InstallSnapshot
// transfer. (The shared memSnapshotStore/echoFSM discard data.)

type capSink struct {
	store *capStore
	id    string
	buf   bytes.Buffer
}

func (s *capSink) Write(p []byte) (int, error) { return s.buf.Write(p) }
func (s *capSink) Close() error {
	s.store.data[s.id] = append([]byte(nil), s.buf.Bytes()...)
	return nil
}
func (s *capSink) Cancel() error { return nil }
func (s *capSink) ID() string    { return s.id }

type capStore struct{ data map[string][]byte }

func (m *capStore) Create(_ SnapshotVersion, index, term uint64, _ Configuration) (SnapshotSink, error) {
	return &capSink{store: m, id: fmt.Sprintf("%d-%d", term, index)}, nil
}
func (m *capStore) Open(id string) (Snapshot, *SnapshotMeta, error) {
	return &capSnap{data: m.data[id]}, &SnapshotMeta{ID: id}, nil
}
func (m *capStore) List() ([]*SnapshotMeta, error) { return nil, nil }
func (m *capStore) Delete(string) error            { return nil }

type capSnap struct{ data []byte }

func (s *capSnap) Index() uint64         { return 0 }
func (s *capSnap) Term() uint64          { return 0 }
func (s *capSnap) Reader() io.ReadCloser { return io.NopCloser(bytes.NewReader(s.data)) }

type capFSM struct{ restored []byte }

func (f *capFSM) Apply(e []byte) ([]byte, error) { return e, nil }
func (f *capFSM) Snapshot() (Snapshot, error)    { return &capSnap{}, nil }
func (f *capFSM) Restore(r io.Reader) error {
	b, err := io.ReadAll(r)
	f.restored = b
	return err
}

func newCapRaftNode(t *testing.T) (*raft, *capFSM) {
	t.Helper()
	cfg := Configuration{Servers: []Server{{ID: "n1"}}}
	trans := newChanTransport("n1")
	fsm := &capFSM{}
	rc := &Config{
		LocalID:              "n1",
		ElectionTick:         5,
		HeartbeatTick:        1,
		InitialConfiguration: cfg,
	}
	r, err := newRaft(rc, "n1", newMemLogStore(), newMemStableStore(),
		&capStore{data: map[string][]byte{}}, fsm, trans)
	if err != nil {
		t.Fatalf("newRaft: %v", err)
	}
	return r, fsm
}

// TestInstallSnapshotReassemblesMultipleChunks verifies that a snapshot larger
// than snapshotChunkSize, delivered as the sequence of Offset-ordered chunks the
// leader actually sends (one InstallSnapshot RPC per chunk, Done only on the
// last), is fully reassembled before FSM.Restore — not restored from only the
// final chunk.
func TestInstallSnapshotReassemblesMultipleChunks(t *testing.T) {
	r, fsm := newCapRaftNode(t)

	// A payload spanning >2 full chunks plus a partial tail.
	payload := make([]byte, snapshotChunkSize*2+777)
	for i := range payload {
		payload[i] = byte((i*31 + 7) % 251)
	}

	total := uint64(len(payload))
	for off := uint64(0); off < total; off += snapshotChunkSize {
		end := off + snapshotChunkSize
		if end > total {
			end = total
		}
		resp := r.handleInstallSnapshot(&InstallSnapshotRequest{
			Term:              5,
			LeaderID:          "leader",
			LastIncludedIndex: 100,
			LastIncludedTerm:  5,
			Offset:            off,
			Data:              payload[off:end],
			Done:              end == total,
		})
		if resp == nil {
			t.Fatalf("nil response at offset %d", off)
		}
	}

	if !bytes.Equal(fsm.restored, payload) {
		t.Fatalf("FSM restored %d bytes, want %d (multi-chunk snapshot must be reassembled before restore)",
			len(fsm.restored), len(payload))
	}
}

// TestInstallSnapshotSingleChunkStillWorks guards the common (<= 1 chunk) path.
func TestInstallSnapshotSingleChunkStillWorks(t *testing.T) {
	r, fsm := newCapRaftNode(t)

	payload := []byte("small snapshot payload")
	resp := r.handleInstallSnapshot(&InstallSnapshotRequest{
		Term:              5,
		LeaderID:          "leader",
		LastIncludedIndex: 42,
		LastIncludedTerm:  5,
		Offset:            0,
		Data:              payload,
		Done:              true,
	})
	if resp == nil {
		t.Fatal("nil response")
	}
	if !bytes.Equal(fsm.restored, payload) {
		t.Fatalf("FSM restored %q, want %q", fsm.restored, payload)
	}
}
