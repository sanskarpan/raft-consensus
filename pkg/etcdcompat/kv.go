// Package etcdcompat provides an etcd v3 gRPC compatibility layer for
// raft-consensus. It exposes a KVServer and a WatchServer that translate
// etcd v3 gRPC calls into the internal fsm.KVStore and fsm.WatchManager APIs.
//
// Type mapping summary:
//   - etcd bytes key/value  ↔  string([]byte) / []byte(string)
//   - RangeEnd == ""          → single-key lookup
//   - RangeEnd == key+"\x00" → prefix scan (HasPrefix)
//   - RangeEnd == "\x00"     → all keys >= key (treat as prefix "" when key=="")
//   - Compare targets/results → fsm.Compare fields
//   - etcd Event PUT/DELETE   → fsm.EventPut / fsm.EventDelete
package etcdcompat

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/sanskarpan/raft-consensus/pkg/fsm"
	"github.com/sanskarpan/raft-consensus/pkg/raft"
	pb "github.com/sanskarpan/raft-consensus/proto/etcdcompat"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RaftApplier is the subset of the raft.Raft interface that KVServer needs to
// propose writes via the Raft log. This narrow interface makes the KVServer
// unit-testable without a real Raft node.
type RaftApplier interface {
	Apply(ctx context.Context, cmd []byte) ([]byte, error)
	Leader() raft.ServerID
	State() raft.RaftState
}

// KVServer implements the pb.KVServer gRPC service interface. It translates
// etcd v3 KV RPCs into internal fsm operations, routing reads to the local
// KVStore (stale) or through Raft (linearizable) and writes through Raft.
type KVServer struct {
	pb.UnimplementedKVServer

	raft      RaftApplier  // for Apply (writes and linearizable reads)
	kv        *fsm.KVStore // for stale reads and revision
	clusterID uint64       // echoed in ResponseHeader.ClusterId
	memberID  uint64       // echoed in ResponseHeader.MemberId
}

// NewKVServer creates a KVServer.
func NewKVServer(raft RaftApplier, kv *fsm.KVStore, clusterID, memberID uint64) *KVServer {
	return &KVServer{
		raft:      raft,
		kv:        kv,
		clusterID: clusterID,
		memberID:  memberID,
	}
}

// header builds a ResponseHeader with the current cluster revision.
func (s *KVServer) header() *pb.ResponseHeader {
	return &pb.ResponseHeader{
		ClusterId: s.clusterID,
		MemberId:  s.memberID,
		Revision:  s.kv.GetRevision(),
		RaftTerm:  0, // term not exposed via fsm; omit
	}
}

// ---------------------------------------------------------------------------
// Range
// ---------------------------------------------------------------------------

// Range implements the KV.Range RPC. When serializable is false (default) the
// read is linearizable: the op travels through the Raft log via Apply("get_v2"
// or "range") so it reflects the committed cluster state. When serializable is
// true the read is served directly from the local FSM state without a Raft
// round-trip (may be stale on a follower).
func (s *KVServer) Range(ctx context.Context, req *pb.RangeRequest) (*pb.RangeResponse, error) {
	key := string(req.Key)
	rangeEnd := string(req.RangeEnd)

	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	if req.Limit < 0 {
		return nil, status.Error(codes.InvalidArgument, "limit must be non-negative")
	}

	isPrefix := isRangeEndPrefix(key, rangeEnd)
	isSingleKey := !isPrefix && rangeEnd == ""

	// Linearizable single-key read: use get_v2 (goes through Raft log).
	if !req.Serializable && isSingleKey {
		return s.linearizableGet(ctx, key, req)
	}

	// Linearizable prefix / all-key range read: use range op (goes through Raft).
	if !req.Serializable && !isSingleKey {
		return s.linearizableRange(ctx, key, rangeEnd, req)
	}

	// Serializable (stale) read: serve from local FSM directly.
	return s.staleRange(key, rangeEnd, req)
}

// linearizableGet routes a single-key read through the Raft log (get_v2).
func (s *KVServer) linearizableGet(ctx context.Context, key string, req *pb.RangeRequest) (*pb.RangeResponse, error) {
	cmd, err := fsm.EncodeCommand("get_v2", key, "")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode get_v2: %v", err)
	}
	raw, err := s.raft.Apply(ctx, cmd)
	if err != nil {
		return nil, mapRaftErr(err)
	}
	kv, err := fsm.DecodeKeyValueResult(raw)
	if err != nil {
		// key not found → empty response
		return &pb.RangeResponse{Header: s.header(), Count: 0}, nil
	}
	if req.CountOnly {
		return &pb.RangeResponse{Header: s.header(), Count: 1}, nil
	}
	pbKV := fsmKVToPb(kv)
	if req.KeysOnly {
		pbKV.Value = nil
	}
	return &pb.RangeResponse{
		Header: s.header(),
		Kvs:    []*pb.KeyValue{pbKV},
		Count:  1,
	}, nil
}

// linearizableRange routes a prefix/range read through the Raft log.
func (s *KVServer) linearizableRange(ctx context.Context, key, rangeEnd string, req *pb.RangeRequest) (*pb.RangeResponse, error) {
	prefix := rangePrefix(key, rangeEnd)
	cmd, err := fsm.EncodeCommand("range", prefix, "")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode range: %v", err)
	}
	raw, err := s.raft.Apply(ctx, cmd)
	if err != nil {
		return nil, mapRaftErr(err)
	}
	kvs, err := fsm.DecodeKeyValuesResult(raw)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "decode range result: %v", err)
	}

	kvs = filterAndSortRange(kvs, key, rangeEnd, req)
	total := int64(len(kvs))

	if req.CountOnly {
		return &pb.RangeResponse{Header: s.header(), Count: total}, nil
	}

	var limited bool
	if req.Limit > 0 && int64(len(kvs)) > req.Limit {
		kvs = kvs[:req.Limit]
		limited = true
	}

	pbKVs := make([]*pb.KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		pbKV := fsmKVToPb(kv)
		if req.KeysOnly {
			pbKV.Value = nil
		}
		pbKVs = append(pbKVs, pbKV)
	}
	return &pb.RangeResponse{
		Header: s.header(),
		Kvs:    pbKVs,
		More:   limited,
		Count:  total,
	}, nil
}

// staleRange serves a range read from the local FSM (no Raft round-trip).
func (s *KVServer) staleRange(key, rangeEnd string, req *pb.RangeRequest) (*pb.RangeResponse, error) {
	prefix := rangePrefix(key, rangeEnd)
	var kvs []*fsm.KeyValue
	var err error

	if prefix == "" {
		// All keys: use Range("") which returns everything
		kvs, err = s.kv.Range("")
	} else {
		kvs, err = s.kv.Range(prefix)
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "range: %v", err)
	}

	kvs = filterAndSortRange(kvs, key, rangeEnd, req)
	total := int64(len(kvs))

	if req.CountOnly {
		return &pb.RangeResponse{Header: s.header(), Count: total}, nil
	}

	var limited bool
	if req.Limit > 0 && int64(len(kvs)) > req.Limit {
		kvs = kvs[:req.Limit]
		limited = true
	}

	pbKVs := make([]*pb.KeyValue, 0, len(kvs))
	for _, kv := range kvs {
		pbKV := fsmKVToPb(kv)
		if req.KeysOnly {
			pbKV.Value = nil
		}
		pbKVs = append(pbKVs, pbKV)
	}
	return &pb.RangeResponse{
		Header: s.header(),
		Kvs:    pbKVs,
		More:   limited,
		Count:  total,
	}, nil
}

// ---------------------------------------------------------------------------
// Put
// ---------------------------------------------------------------------------

// Put implements the KV.Put RPC. Writes always go through Raft regardless of
// serializable mode.
func (s *KVServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	key := string(req.Key)
	value := string(req.Value)

	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	// prev_kv: fetch the previous value before the put, stale-read is OK here.
	var prevKV *pb.KeyValue
	if req.PrevKv {
		prev, err := s.kv.Get(key)
		if err == nil && prev != nil {
			prevKV = fsmKVToPb(prev)
		}
	}

	cmd, err := fsm.EncodeCommand("put", key, value)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode put: %v", err)
	}
	raw, err := s.raft.Apply(ctx, cmd)
	if err != nil {
		return nil, mapRaftErr(err)
	}
	// Decode to confirm no FSM-level error.
	res, decErr := fsm.DecodeResult(raw)
	if decErr != nil {
		return nil, status.Errorf(codes.Internal, "decode put result: %v", decErr)
	}
	if res.Error != "" {
		return nil, status.Errorf(codes.Internal, "put failed: %s", res.Error)
	}

	return &pb.PutResponse{
		Header: s.header(),
		PrevKv: prevKV,
	}, nil
}

// ---------------------------------------------------------------------------
// DeleteRange
// ---------------------------------------------------------------------------

// DeleteRange implements the KV.DeleteRange RPC. Supports single-key delete
// and prefix delete.
func (s *KVServer) DeleteRange(ctx context.Context, req *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	key := string(req.Key)
	rangeEnd := string(req.RangeEnd)

	if key == "" {
		return nil, status.Error(codes.InvalidArgument, "key must not be empty")
	}

	isPrefix := isRangeEndPrefix(key, rangeEnd)

	if isPrefix {
		return s.deletePrefixRange(ctx, key, rangeEnd, req)
	}
	return s.deleteSingleKey(ctx, key, req)
}

// deleteSingleKey deletes exactly one key.
func (s *KVServer) deleteSingleKey(ctx context.Context, key string, req *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	var prevKV *pb.KeyValue
	if req.PrevKv {
		prev, err := s.kv.Get(key)
		if err == nil && prev != nil {
			prevKV = fsmKVToPb(prev)
		}
	}

	cmd, err := fsm.EncodeCommand("delete", key, "")
	if err != nil {
		return nil, status.Errorf(codes.Internal, "encode delete: %v", err)
	}
	raw, err := s.raft.Apply(ctx, cmd)
	if err != nil {
		return nil, mapRaftErr(err)
	}
	res, decErr := fsm.DecodeResult(raw)
	if decErr != nil {
		return nil, status.Errorf(codes.Internal, "decode delete result: %v", decErr)
	}
	// "key not found" is not a hard error; deleted = 0.
	var deleted int64
	if res.Error == "" {
		deleted = 1
	}

	resp := &pb.DeleteRangeResponse{
		Header:  s.header(),
		Deleted: deleted,
	}
	if req.PrevKv && prevKV != nil {
		resp.PrevKvs = []*pb.KeyValue{prevKV}
	}
	return resp, nil
}

// deletePrefixRange deletes all keys in the given range by issuing individual
// deletes through Raft for each matching key. A future optimization would add
// a native "delete_range" op to the FSM; for now the implementation is
// correct and atomic at the Raft-log level per key.
func (s *KVServer) deletePrefixRange(ctx context.Context, key, rangeEnd string, req *pb.DeleteRangeRequest) (*pb.DeleteRangeResponse, error) {
	prefix := rangePrefix(key, rangeEnd)
	existing, err := s.kv.Range(prefix)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "range before delete: %v", err)
	}

	// Filter to exact range bounds.
	var toDelete []*fsm.KeyValue
	for _, kv := range existing {
		if inRange(kv.Key, key, rangeEnd) {
			toDelete = append(toDelete, kv)
		}
	}

	var prevKVs []*pb.KeyValue
	if req.PrevKv {
		for _, kv := range toDelete {
			prevKVs = append(prevKVs, fsmKVToPb(kv))
		}
	}

	var deleted int64
	for _, kv := range toDelete {
		cmd, encErr := fsm.EncodeCommand("delete", kv.Key, "")
		if encErr != nil {
			return nil, status.Errorf(codes.Internal, "encode delete: %v", encErr)
		}
		raw, applyErr := s.raft.Apply(ctx, cmd)
		if applyErr != nil {
			return nil, mapRaftErr(applyErr)
		}
		res, decErr := fsm.DecodeResult(raw)
		if decErr != nil {
			return nil, status.Errorf(codes.Internal, "decode delete result: %v", decErr)
		}
		if res.Error == "" {
			deleted++
		}
	}

	return &pb.DeleteRangeResponse{
		Header:  s.header(),
		Deleted: deleted,
		PrevKvs: prevKVs,
	}, nil
}

// ---------------------------------------------------------------------------
// Txn
// ---------------------------------------------------------------------------

// Txn implements the KV.Txn RPC. It translates the etcd Compare/RequestOp
// tree into a flat fsm.TxnRequest (single level — nested TxnRequest ops are
// not supported in the current FSM and will return Unimplemented).
func (s *KVServer) Txn(ctx context.Context, req *pb.TxnRequest) (*pb.TxnResponse, error) {
	// Translate etcd Compare → fsm.Compare.
	fsmCmps, err := translateCompares(req.Compare)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "compare: %v", err)
	}

	// Translate success branch.
	fsmSuccess, err := translateOps(req.Success)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "success: %v", err)
	}

	// Translate failure branch.
	fsmFailure, err := translateOps(req.Failure)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "failure: %v", err)
	}

	fsmReq := &fsm.TxnRequest{
		Compare: fsmCmps,
		Success: fsmSuccess,
		Failure: fsmFailure,
	}

	encoded, encErr := fsm.EncodeTxn(fsmReq)
	if encErr != nil {
		return nil, status.Errorf(codes.Internal, "encode txn: %v", encErr)
	}
	raw, applyErr := s.raft.Apply(ctx, encoded)
	if applyErr != nil {
		return nil, mapRaftErr(applyErr)
	}

	txnResp, decErr := fsm.DecodeTxnResult(raw)
	if decErr != nil {
		return nil, status.Errorf(codes.Internal, "decode txn result: %v", decErr)
	}

	// Build ResponseOp list from TxnResults.
	ops := req.Success
	if !txnResp.Succeeded {
		ops = req.Failure
	}
	responses := make([]*pb.ResponseOp, 0, len(txnResp.Results))
	for i, res := range txnResp.Results {
		var op *pb.ResponseOp
		if i < len(ops) {
			op = buildResponseOp(ops[i], res, s.header())
		} else {
			op = &pb.ResponseOp{}
		}
		responses = append(responses, op)
	}

	return &pb.TxnResponse{
		Header:    s.header(),
		Succeeded: txnResp.Succeeded,
		Responses: responses,
	}, nil
}

// ---------------------------------------------------------------------------
// Translation helpers
// ---------------------------------------------------------------------------

// fsmKVToPb converts a fsm.KeyValue to a proto KeyValue.
func fsmKVToPb(kv *fsm.KeyValue) *pb.KeyValue {
	if kv == nil {
		return nil
	}
	return &pb.KeyValue{
		Key:            []byte(kv.Key),
		Value:          []byte(kv.Value),
		CreateRevision: kv.CreateRevision,
		ModRevision:    kv.ModRevision,
		Version:        kv.Version,
	}
}

// translateCompares converts etcd Compare messages to fsm.Compare values.
func translateCompares(cmps []*pb.Compare) ([]fsm.Compare, error) {
	out := make([]fsm.Compare, 0, len(cmps))
	for _, c := range cmps {
		fc := fsm.Compare{Key: string(c.Key)}

		switch c.Result {
		case pb.Compare_EQUAL:
			fc.Result = "equal"
		case pb.Compare_NOT_EQUAL:
			fc.Result = "not_equal"
		case pb.Compare_GREATER:
			fc.Result = "greater"
		case pb.Compare_LESS:
			fc.Result = "less"
		default:
			return nil, fmt.Errorf("unknown compare result %v", c.Result)
		}

		switch c.Target {
		case pb.Compare_VERSION:
			fc.Target = "version"
			if v, ok := c.TargetUnion.(*pb.Compare_Version); ok {
				fc.Rev = v.Version
			}
		case pb.Compare_CREATE:
			fc.Target = "create_revision"
			if v, ok := c.TargetUnion.(*pb.Compare_CreateRevision); ok {
				fc.Rev = v.CreateRevision
			}
		case pb.Compare_MOD:
			fc.Target = "mod_revision"
			if v, ok := c.TargetUnion.(*pb.Compare_ModRevision); ok {
				fc.Rev = v.ModRevision
			}
		case pb.Compare_VALUE:
			fc.Target = "value"
			if v, ok := c.TargetUnion.(*pb.Compare_Value); ok {
				fc.Value = string(v.Value)
			}
		default:
			return nil, fmt.Errorf("unknown compare target %v", c.Target)
		}

		out = append(out, fc)
	}
	return out, nil
}

// translateOps converts a slice of etcd RequestOp to fsm.TxnOp.
// Nested Txn ops (request_txn) are rejected.
func translateOps(ops []*pb.RequestOp) ([]fsm.TxnOp, error) {
	out := make([]fsm.TxnOp, 0, len(ops))
	for _, op := range ops {
		switch v := op.Request.(type) {
		case *pb.RequestOp_RequestPut:
			out = append(out, fsm.TxnOp{
				Type:  0, // put
				Key:   string(v.RequestPut.Key),
				Value: string(v.RequestPut.Value),
			})
		case *pb.RequestOp_RequestDeleteRange:
			// Only single-key deletes are supported in the fsm TxnOp.
			if len(v.RequestDeleteRange.RangeEnd) > 0 {
				return nil, fmt.Errorf("range deletes in transactions are not supported")
			}
			out = append(out, fsm.TxnOp{
				Type: 1, // delete
				Key:  string(v.RequestDeleteRange.Key),
			})
		case *pb.RequestOp_RequestRange:
			// Read ops inside transactions are not yet supported.
			return nil, fmt.Errorf("range ops inside transactions are not supported")
		case *pb.RequestOp_RequestTxn:
			return nil, fmt.Errorf("nested transactions are not supported")
		default:
			return nil, fmt.Errorf("unsupported RequestOp type")
		}
	}
	return out, nil
}

// buildResponseOp wraps a fsm.TxnResult in the appropriate ResponseOp type.
func buildResponseOp(req *pb.RequestOp, res fsm.TxnResult, hdr *pb.ResponseHeader) *pb.ResponseOp {
	switch req.Request.(type) {
	case *pb.RequestOp_RequestPut:
		return &pb.ResponseOp{
			Response: &pb.ResponseOp_ResponsePut{
				ResponsePut: &pb.PutResponse{
					Header: hdr,
				},
			},
		}
	case *pb.RequestOp_RequestDeleteRange:
		var deleted int64
		if res.Error == "" {
			deleted = 1
		}
		return &pb.ResponseOp{
			Response: &pb.ResponseOp_ResponseDeleteRange{
				ResponseDeleteRange: &pb.DeleteRangeResponse{
					Header:  hdr,
					Deleted: deleted,
				},
			},
		}
	default:
		return &pb.ResponseOp{}
	}
}

// ---------------------------------------------------------------------------
// Range helpers
// ---------------------------------------------------------------------------

// isRangeEndPrefix reports whether rangeEnd signals a prefix scan starting at
// key. etcd convention: rangeEnd == key+"\x00" is a prefix scan.
// Additionally rangeEnd == "\x00" with key == "" means all keys.
func isRangeEndPrefix(key, rangeEnd string) bool {
	if rangeEnd == "" {
		return false
	}
	if rangeEnd == "\x00" {
		return true // all keys >= key
	}
	// rangeEnd == key + "\x00"  means all keys with prefix key.
	if len(rangeEnd) == len(key)+1 &&
		strings.HasPrefix(rangeEnd, key) &&
		rangeEnd[len(rangeEnd)-1] == 0 {
		return true
	}
	return false
}

// rangePrefix returns the FSM-level prefix to pass to kv.Range() based on the
// etcd range semantics. Returns "" to mean "all keys".
func rangePrefix(key, rangeEnd string) string {
	if rangeEnd == "\x00" && key == "" {
		return "" // all keys
	}
	if rangeEnd == "\x00" {
		return key // all keys >= key — use key as prefix
	}
	// rangeEnd == key+"\x00" → prefix scan with key as prefix
	return key
}

// inRange reports whether a key falls within [start, end) using etcd's range
// semantics. end=="" means single key equal to start. end=="\x00" means
// all keys >= start. end==start+"\x00" (the standard etcd prefix-scan
// convention) means all keys with prefix start.
func inRange(key, start, end string) bool {
	if end == "" {
		return key == start
	}
	if end == "\x00" {
		return key >= start
	}
	// etcd prefix-scan convention: rangeEnd == key + "\x00".
	// Since '\x00' < any printable character, lexicographic comparison
	// start < key < end would incorrectly exclude keys like "prefix/a"
	// (0x61 > 0x00). Detect this pattern and use HasPrefix instead.
	if len(end) == len(start)+1 && end[:len(start)] == start && end[len(end)-1] == 0 {
		return strings.HasPrefix(key, start)
	}
	return key >= start && key < end
}

// filterAndSortRange filters a slice of KeyValues to those within [key, rangeEnd)
// and applies revision filters and sort order from req.
func filterAndSortRange(kvs []*fsm.KeyValue, key, rangeEnd string, req *pb.RangeRequest) []*fsm.KeyValue {
	var out []*fsm.KeyValue
	for _, kv := range kvs {
		if !inRange(kv.Key, key, rangeEnd) {
			continue
		}
		if req.MinModRevision > 0 && kv.ModRevision < req.MinModRevision {
			continue
		}
		if req.MaxModRevision > 0 && kv.ModRevision > req.MaxModRevision {
			continue
		}
		if req.MinCreateRevision > 0 && kv.CreateRevision < req.MinCreateRevision {
			continue
		}
		if req.MaxCreateRevision > 0 && kv.CreateRevision > req.MaxCreateRevision {
			continue
		}
		out = append(out, kv)
	}
	sortKVs(out, req.SortOrder, req.SortTarget)
	return out
}

// sortKVs sorts a slice of KeyValues by the given field and order.
func sortKVs(kvs []*fsm.KeyValue, order pb.RangeRequest_SortOrder, target pb.RangeRequest_SortTarget) {
	if order == pb.RangeRequest_NONE {
		// Default ascending by key (fsm.Range already returns sorted by key,
		// but keep sort here for correctness when called after filtering).
		sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })
		return
	}
	less := func(i, j int) bool {
		var lt bool
		switch target {
		case pb.RangeRequest_VERSION:
			lt = kvs[i].Version < kvs[j].Version
		case pb.RangeRequest_CREATE:
			lt = kvs[i].CreateRevision < kvs[j].CreateRevision
		case pb.RangeRequest_MOD:
			lt = kvs[i].ModRevision < kvs[j].ModRevision
		case pb.RangeRequest_VALUE:
			lt = kvs[i].Value < kvs[j].Value
		default: // KEY
			lt = kvs[i].Key < kvs[j].Key
		}
		if order == pb.RangeRequest_DESCEND {
			return !lt
		}
		return lt
	}
	sort.Slice(kvs, less)
}

// mapRaftErr converts a Raft-level error to an appropriate gRPC status error.
func mapRaftErr(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case strings.Contains(err.Error(), "not leader"):
		return status.Errorf(codes.FailedPrecondition, "not leader: %v", err)
	case strings.Contains(err.Error(), "context canceled"):
		return status.Errorf(codes.Canceled, "request canceled: %v", err)
	case strings.Contains(err.Error(), "context deadline exceeded"),
		strings.Contains(err.Error(), "timeout"):
		return status.Errorf(codes.DeadlineExceeded, "deadline exceeded: %v", err)
	default:
		return status.Errorf(codes.Internal, "raft apply: %v", err)
	}
}

