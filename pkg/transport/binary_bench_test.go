package transport_test

import (
	"encoding/json"
	"testing"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
	"github.com/sanskarpan/raft-consensus/pkg/transport"
)

var benchmarkReq = &transport.AppendEntriesReq{
	Term:         42,
	LeaderID:     "node-1",
	PrevLogIndex: 100,
	PrevLogTerm:  41,
	LeaderCommit: 99,
	Entries: []*raft.LogEntry{
		{Term: 42, Index: 101, Type: raft.EntryNormal, Data: []byte(`{"key":"foo","value":"bar"}`)},
		{Term: 42, Index: 102, Type: raft.EntryNormal, Data: []byte(`{"key":"baz","value":"qux"}`)},
	},
}

func BenchmarkMarshalBinaryAppendEntriesReq(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := transport.MarshalAppendEntriesReq(benchmarkReq); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMarshalJSONAppendEntriesReq(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := json.Marshal(benchmarkReq); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshalBinaryAppendEntriesReq(b *testing.B) {
	data, _ := transport.MarshalAppendEntriesReq(benchmarkReq)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := transport.UnmarshalAppendEntriesReq(data); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkUnmarshalJSONAppendEntriesReq(b *testing.B) {
	data, _ := json.Marshal(benchmarkReq)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var r transport.AppendEntriesReq
		if err := json.Unmarshal(data, &r); err != nil {
			b.Fatal(err)
		}
	}
}
