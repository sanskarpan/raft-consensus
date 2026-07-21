package transport_test

import (
	"bytes"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
	"github.com/sanskarpan/raft-consensus/pkg/transport"
)

func TestBinaryRoundTripAppendEntriesReq(t *testing.T) {
	orig := &transport.AppendEntriesReq{
		Term:         7,
		LeaderID:     "leader1",
		PrevLogIndex: 42,
		PrevLogTerm:  6,
		LeaderCommit: 41,
		Entries: []*raft.LogEntry{
			{Term: 6, Index: 42, Type: raft.EntryNormal, Data: []byte("hello")},
			{Term: 7, Index: 43, Type: raft.EntryConfiguration, Data: nil},
		},
	}
	data, err := transport.MarshalAppendEntriesReq(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := transport.UnmarshalAppendEntriesReq(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Term != orig.Term || got.LeaderID != orig.LeaderID {
		t.Errorf("scalar fields mismatch: got %+v", got)
	}
	if len(got.Entries) != 2 {
		t.Fatalf("entries len = %d, want 2", len(got.Entries))
	}
	if got.Entries[0].Term != 6 || string(got.Entries[0].Data) != "hello" {
		t.Errorf("entry[0] mismatch: %+v", got.Entries[0])
	}
	if got.Entries[1].Index != 43 {
		t.Errorf("entry[1] index mismatch: %+v", got.Entries[1])
	}
}

func TestBinaryRoundTripAppendEntriesResp(t *testing.T) {
	orig := &transport.AppendEntriesResp{Term: 8, Success: true, Index: 55, ConflictTerm: 3}
	data, err := transport.MarshalAppendEntriesResp(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := transport.UnmarshalAppendEntriesResp(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Term != orig.Term || got.Success != orig.Success || got.Index != orig.Index || got.ConflictTerm != orig.ConflictTerm {
		t.Errorf("mismatch: got %+v, want %+v", got, orig)
	}
}

func TestBinaryRoundTripRequestVoteReq(t *testing.T) {
	orig := &transport.RequestVoteReq{
		Term: 3, CandidateID: "cand-2", LastLogIndex: 10, LastLogTerm: 2,
		PreVote: true, LeaderTransfer: false,
	}
	data, err := transport.MarshalRequestVoteReq(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := transport.UnmarshalRequestVoteReq(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Term != orig.Term || got.CandidateID != orig.CandidateID || got.PreVote != orig.PreVote {
		t.Errorf("mismatch: got %+v, want %+v", got, orig)
	}
}

func TestBinaryRoundTripRequestVoteResp(t *testing.T) {
	orig := &transport.RequestVoteResp{Term: 4, VoteGranted: true, Reason: "already voted"}
	data, err := transport.MarshalRequestVoteResp(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := transport.UnmarshalRequestVoteResp(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Term != orig.Term || got.VoteGranted != orig.VoteGranted || got.Reason != orig.Reason {
		t.Errorf("mismatch: got %+v, want %+v", got, orig)
	}
}

func TestBinaryRoundTripInstallSnapshot(t *testing.T) {
	origReq := &transport.InstallSnapshotReq{
		Term: 9, LeaderID: "l1", LastIncludedIndex: 100, LastIncludedTerm: 8,
		Offset: 4096, Data: []byte("snapdata"), Done: true,
	}
	data, err := transport.MarshalInstallSnapshotReq(origReq)
	if err != nil {
		t.Fatalf("marshal req: %v", err)
	}
	gotReq, err := transport.UnmarshalInstallSnapshotReq(data)
	if err != nil {
		t.Fatalf("unmarshal req: %v", err)
	}
	if gotReq.Term != origReq.Term || gotReq.LeaderID != origReq.LeaderID ||
		gotReq.LastIncludedIndex != origReq.LastIncludedIndex || gotReq.Done != origReq.Done ||
		!bytes.Equal(gotReq.Data, origReq.Data) {
		t.Errorf("req mismatch: got %+v, want %+v", gotReq, origReq)
	}

	origResp := &transport.InstallSnapshotResp{Term: 9}
	dataR, err := transport.MarshalInstallSnapshotResp(origResp)
	if err != nil {
		t.Fatalf("marshal resp: %v", err)
	}
	gotResp, err := transport.UnmarshalInstallSnapshotResp(dataR)
	if err != nil {
		t.Fatalf("unmarshal resp: %v", err)
	}
	if gotResp.Term != origResp.Term {
		t.Errorf("resp mismatch: got %+v, want %+v", gotResp, origResp)
	}
}

func TestBinaryRoundTripTimeoutNow(t *testing.T) {
	origReq := &transport.TimeoutNowReq{ServerID: "srv-3"}
	data, err := transport.MarshalTimeoutNowReq(origReq)
	if err != nil {
		t.Fatalf("marshal req: %v", err)
	}
	gotReq, err := transport.UnmarshalTimeoutNowReq(data)
	if err != nil {
		t.Fatalf("unmarshal req: %v", err)
	}
	if gotReq.ServerID != origReq.ServerID {
		t.Errorf("ServerID mismatch: %q != %q", gotReq.ServerID, origReq.ServerID)
	}

	// TimeoutNowResp is empty — marshal produces zero bytes, unmarshal is a no-op.
	dataR, err := transport.MarshalTimeoutNowResp(&transport.TimeoutNowResp{})
	if err != nil {
		t.Fatalf("marshal resp: %v", err)
	}
	if len(dataR) != 0 {
		t.Errorf("TimeoutNowResp marshal should be empty, got %d bytes", len(dataR))
	}
	_, err = transport.UnmarshalTimeoutNowResp(dataR)
	if err != nil {
		t.Fatalf("unmarshal resp: %v", err)
	}
}

func TestBinaryHandlesEmptyEntries(t *testing.T) {
	orig := &transport.AppendEntriesReq{
		Term: 1, LeaderID: "l", PrevLogIndex: 0, PrevLogTerm: 0, LeaderCommit: 0,
		Entries: nil,
	}
	data, err := transport.MarshalAppendEntriesReq(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := transport.UnmarshalAppendEntriesReq(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got.Entries) != 0 {
		t.Errorf("expected empty entries, got %d", len(got.Entries))
	}
}

func TestBinaryHandlesLargeData(t *testing.T) {
	big := make([]byte, 1<<20) // 1 MiB
	for i := range big {
		big[i] = byte(i & 0xff)
	}
	orig := &transport.InstallSnapshotReq{
		Term: 1, LeaderID: "l", LastIncludedIndex: 1, LastIncludedTerm: 1,
		Offset: 0, Data: big, Done: true,
	}
	data, err := transport.MarshalInstallSnapshotReq(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	got, err := transport.UnmarshalInstallSnapshotReq(data)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !bytes.Equal(got.Data, orig.Data) {
		t.Error("large Data round-trip mismatch")
	}
}
