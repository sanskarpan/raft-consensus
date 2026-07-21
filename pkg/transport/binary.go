// Package transport — binary codec for TCP RPC types.
//
// Encoding scheme: varint/uvarint for uint64; 1-byte bool; length-prefixed
// UTF-8 for strings; length-prefixed bytes for []byte slices. No reflection.
package transport

import (
	"encoding/binary"
	"fmt"

	"github.com/sanskarpan/raft-consensus/pkg/raft"
)

// RPC type-tag constants. Each constant maps to one of the 8 message types
// exchanged over the TCP transport binary frame protocol.
const (
	TagAppendEntriesReq    uint8 = 1
	TagAppendEntriesResp   uint8 = 2
	TagRequestVoteReq      uint8 = 3
	TagRequestVoteResp     uint8 = 4
	TagInstallSnapshotReq  uint8 = 5
	TagInstallSnapshotResp uint8 = 6
	TagTimeoutNowReq       uint8 = 7
	TagTimeoutNowResp      uint8 = 8
)

// TypeTagForRPCType returns the binary type-tag for the given JSON rpc type string.
// Returns 0 if not recognized.
func TypeTagForRPCType(rpcType string) uint8 {
	switch rpcType {
	case "AppendEntries":
		return TagAppendEntriesReq
	case "AppendEntriesResponse":
		return TagAppendEntriesResp
	case "RequestVote":
		return TagRequestVoteReq
	case "RequestVoteResponse":
		return TagRequestVoteResp
	case "InstallSnapshot":
		return TagInstallSnapshotReq
	case "InstallSnapshotResponse":
		return TagInstallSnapshotResp
	case "TimeoutNow":
		return TagTimeoutNowReq
	case "TimeoutNowResponse":
		return TagTimeoutNowResp
	default:
		return 0
	}
}

// RPCTypeForTag returns the JSON rpc type string for the given binary type-tag.
func RPCTypeForTag(tag uint8) string {
	switch tag {
	case TagAppendEntriesReq:
		return "AppendEntries"
	case TagAppendEntriesResp:
		return "AppendEntriesResponse"
	case TagRequestVoteReq:
		return "RequestVote"
	case TagRequestVoteResp:
		return "RequestVoteResponse"
	case TagInstallSnapshotReq:
		return "InstallSnapshot"
	case TagInstallSnapshotResp:
		return "InstallSnapshotResponse"
	case TagTimeoutNowReq:
		return "TimeoutNow"
	case TagTimeoutNowResp:
		return "TimeoutNowResponse"
	default:
		return ""
	}
}

// --- low-level encode/decode helpers ---

// appendUvarint appends v to dst as a uvarint and returns the new slice.
func appendUvarint(dst []byte, v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	return append(dst, buf[:n]...)
}

// appendBool appends b as a single byte (0 or 1) to dst.
func appendBool(dst []byte, b bool) []byte {
	if b {
		return append(dst, 1)
	}
	return append(dst, 0)
}

// appendString appends s as a uvarint length prefix followed by UTF-8 bytes.
func appendString(dst []byte, s string) []byte {
	dst = appendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

// appendBytes appends b as a uvarint length prefix followed by the raw bytes.
func appendBytes(dst []byte, b []byte) []byte {
	dst = appendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

// decodeUvarint decodes a uvarint from data[off:] and returns (value, newOff, err).
func decodeUvarint(data []byte, off int) (uint64, int, error) {
	v, n := binary.Uvarint(data[off:])
	if n <= 0 {
		return 0, off, fmt.Errorf("binary: uvarint decode failed at offset %d (n=%d)", off, n)
	}
	return v, off + n, nil
}

// decodeBool decodes a single byte as bool from data[off:].
func decodeBool(data []byte, off int) (bool, int, error) {
	if off >= len(data) {
		return false, off, fmt.Errorf("binary: bool decode out of range at offset %d", off)
	}
	return data[off] != 0, off + 1, nil
}

// decodeString decodes a length-prefixed string from data[off:].
func decodeString(data []byte, off int) (string, int, error) {
	const maxDecodeLen = 1 << 30 // 1 GiB sanity cap
	length, off, err := decodeUvarint(data, off)
	if err != nil {
		return "", off, err
	}
	if length > maxDecodeLen {
		return "", off, fmt.Errorf("binary: string length %d exceeds max %d", length, maxDecodeLen)
	}
	if length > uint64(len(data)-off) {
		return "", off, fmt.Errorf("binary: string length %d exceeds remaining data (%d bytes)", length, len(data)-off)
	}
	end := off + int(length)
	return string(data[off:end]), end, nil
}

// decodeBytes decodes a length-prefixed byte slice from data[off:].
func decodeBytes(data []byte, off int) ([]byte, int, error) {
	const maxDecodeLen = 1 << 30 // 1 GiB sanity cap
	length, off, err := decodeUvarint(data, off)
	if err != nil {
		return nil, off, err
	}
	if length > maxDecodeLen {
		return nil, off, fmt.Errorf("binary: bytes length %d exceeds max %d", length, maxDecodeLen)
	}
	if length > uint64(len(data)-off) {
		return nil, off, fmt.Errorf("binary: bytes length %d exceeds remaining data (%d bytes)", length, len(data)-off)
	}
	end := off + int(length)
	out := make([]byte, length)
	copy(out, data[off:end])
	return out, end, nil
}

// --- AppendEntriesReq ---

// MarshalAppendEntriesReq encodes req into a compact binary payload.
func MarshalAppendEntriesReq(r *AppendEntriesReq) ([]byte, error) {
	var dst []byte
	dst = appendUvarint(dst, r.Term)
	dst = appendString(dst, r.LeaderID)
	dst = appendUvarint(dst, r.PrevLogIndex)
	dst = appendUvarint(dst, r.PrevLogTerm)
	dst = appendUvarint(dst, r.LeaderCommit)
	dst = appendUvarint(dst, uint64(len(r.Entries)))
	for _, e := range r.Entries {
		dst = appendUvarint(dst, e.Term)
		dst = appendUvarint(dst, e.Index)
		dst = append(dst, uint8(e.Type))
		dst = appendBytes(dst, e.Data)
	}
	return dst, nil
}

// UnmarshalAppendEntriesReq decodes data produced by MarshalAppendEntriesReq.
func UnmarshalAppendEntriesReq(data []byte) (*AppendEntriesReq, error) {
	var off int
	var err error
	r := &AppendEntriesReq{}

	r.Term, off, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}
	r.LeaderID, off, err = decodeString(data, off)
	if err != nil {
		return nil, err
	}
	r.PrevLogIndex, off, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}
	r.PrevLogTerm, off, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}
	r.LeaderCommit, off, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}

	var nEntries uint64
	nEntries, off, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}
	const maxEntries = 10_000
	if nEntries > maxEntries {
		return nil, fmt.Errorf("binary: nEntries %d exceeds max %d", nEntries, maxEntries)
	}
	if nEntries > 0 {
		r.Entries = make([]*raft.LogEntry, nEntries)
		for i := uint64(0); i < nEntries; i++ {
			e := &raft.LogEntry{}
			e.Term, off, err = decodeUvarint(data, off)
			if err != nil {
				return nil, err
			}
			e.Index, off, err = decodeUvarint(data, off)
			if err != nil {
				return nil, err
			}
			if off >= len(data) {
				return nil, fmt.Errorf("binary: entry type byte out of range")
			}
			e.Type = raft.EntryType(data[off])
			off++
			e.Data, off, err = decodeBytes(data, off)
			if err != nil {
				return nil, err
			}
			r.Entries[i] = e
		}
	}
	return r, nil
}

// --- AppendEntriesResp ---

// MarshalAppendEntriesResp encodes resp into binary.
func MarshalAppendEntriesResp(r *AppendEntriesResp) ([]byte, error) {
	var dst []byte
	dst = appendUvarint(dst, r.Term)
	dst = appendBool(dst, r.Success)
	dst = appendUvarint(dst, r.Index)
	dst = appendUvarint(dst, r.ConflictTerm)
	return dst, nil
}

// UnmarshalAppendEntriesResp decodes data produced by MarshalAppendEntriesResp.
func UnmarshalAppendEntriesResp(data []byte) (*AppendEntriesResp, error) {
	var off int
	var err error
	r := &AppendEntriesResp{}
	r.Term, off, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}
	r.Success, off, err = decodeBool(data, off)
	if err != nil {
		return nil, err
	}
	r.Index, off, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}
	r.ConflictTerm, _, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// --- RequestVoteReq ---

// MarshalRequestVoteReq encodes req into binary.
func MarshalRequestVoteReq(r *RequestVoteReq) ([]byte, error) {
	var dst []byte
	dst = appendUvarint(dst, r.Term)
	dst = appendString(dst, r.CandidateID)
	dst = appendUvarint(dst, r.LastLogIndex)
	dst = appendUvarint(dst, r.LastLogTerm)
	dst = appendBool(dst, r.PreVote)
	dst = appendBool(dst, r.LeaderTransfer)
	return dst, nil
}

// UnmarshalRequestVoteReq decodes data produced by MarshalRequestVoteReq.
func UnmarshalRequestVoteReq(data []byte) (*RequestVoteReq, error) {
	var off int
	var err error
	r := &RequestVoteReq{}
	r.Term, off, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}
	r.CandidateID, off, err = decodeString(data, off)
	if err != nil {
		return nil, err
	}
	r.LastLogIndex, off, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}
	r.LastLogTerm, off, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}
	r.PreVote, off, err = decodeBool(data, off)
	if err != nil {
		return nil, err
	}
	r.LeaderTransfer, _, err = decodeBool(data, off)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// --- RequestVoteResp ---

// MarshalRequestVoteResp encodes resp into binary.
func MarshalRequestVoteResp(r *RequestVoteResp) ([]byte, error) {
	var dst []byte
	dst = appendUvarint(dst, r.Term)
	dst = appendBool(dst, r.VoteGranted)
	dst = appendString(dst, r.Reason)
	return dst, nil
}

// UnmarshalRequestVoteResp decodes data produced by MarshalRequestVoteResp.
func UnmarshalRequestVoteResp(data []byte) (*RequestVoteResp, error) {
	var off int
	var err error
	r := &RequestVoteResp{}
	r.Term, off, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}
	r.VoteGranted, off, err = decodeBool(data, off)
	if err != nil {
		return nil, err
	}
	r.Reason, _, err = decodeString(data, off)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// --- InstallSnapshotReq ---

// MarshalInstallSnapshotReq encodes req into binary.
func MarshalInstallSnapshotReq(r *InstallSnapshotReq) ([]byte, error) {
	var dst []byte
	dst = appendUvarint(dst, r.Term)
	dst = appendString(dst, r.LeaderID)
	dst = appendUvarint(dst, r.LastIncludedIndex)
	dst = appendUvarint(dst, r.LastIncludedTerm)
	dst = appendUvarint(dst, r.Offset)
	dst = appendBytes(dst, r.Data)
	dst = appendBool(dst, r.Done)
	return dst, nil
}

// UnmarshalInstallSnapshotReq decodes data produced by MarshalInstallSnapshotReq.
func UnmarshalInstallSnapshotReq(data []byte) (*InstallSnapshotReq, error) {
	var off int
	var err error
	r := &InstallSnapshotReq{}
	r.Term, off, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}
	r.LeaderID, off, err = decodeString(data, off)
	if err != nil {
		return nil, err
	}
	r.LastIncludedIndex, off, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}
	r.LastIncludedTerm, off, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}
	r.Offset, off, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}
	r.Data, off, err = decodeBytes(data, off)
	if err != nil {
		return nil, err
	}
	r.Done, _, err = decodeBool(data, off)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// --- InstallSnapshotResp ---

// MarshalInstallSnapshotResp encodes resp into binary.
func MarshalInstallSnapshotResp(r *InstallSnapshotResp) ([]byte, error) {
	var dst []byte
	dst = appendUvarint(dst, r.Term)
	return dst, nil
}

// UnmarshalInstallSnapshotResp decodes data produced by MarshalInstallSnapshotResp.
func UnmarshalInstallSnapshotResp(data []byte) (*InstallSnapshotResp, error) {
	var off int
	var err error
	r := &InstallSnapshotResp{}
	r.Term, _, err = decodeUvarint(data, off)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// --- TimeoutNowReq ---

// MarshalTimeoutNowReq encodes req into binary.
func MarshalTimeoutNowReq(r *TimeoutNowReq) ([]byte, error) {
	var dst []byte
	dst = appendString(dst, r.ServerID)
	return dst, nil
}

// UnmarshalTimeoutNowReq decodes data produced by MarshalTimeoutNowReq.
func UnmarshalTimeoutNowReq(data []byte) (*TimeoutNowReq, error) {
	var off int
	var err error
	r := &TimeoutNowReq{}
	r.ServerID, _, err = decodeString(data, off)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// --- TimeoutNowResp (empty) ---

// MarshalTimeoutNowResp encodes resp (empty) — produces zero bytes.
func MarshalTimeoutNowResp(_ *TimeoutNowResp) ([]byte, error) {
	return []byte{}, nil
}

// UnmarshalTimeoutNowResp decodes data (always empty) into a TimeoutNowResp.
func UnmarshalTimeoutNowResp(_ []byte) (*TimeoutNowResp, error) {
	return &TimeoutNowResp{}, nil
}
