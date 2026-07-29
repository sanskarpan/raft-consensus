package multiraft

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/fsm"
	"github.com/sanskarpan/raft-consensus/pkg/raft"
	"github.com/sanskarpan/raft-consensus/pkg/storage"
	"github.com/sanskarpan/raft-consensus/pkg/transport"
	"go.uber.org/zap"
)

// GroupState manages all resources for a single Raft shard: WAL, stable store,
// snapshot store, KVStore FSM, WatchManager, TCP transport, and Raft node.
//
// The resource lifecycle follows the same three-step ordering used in
// cmd/raftd/main.go initRaft():
//
//  1. Create raftHandlerWrapper (holds a pointer to raftNode, initially nil).
//  2. Create TCP transport with the wrapper as MessageHandler.
//  3. Create raftNode, then set wrapper.raftNode = raftNode.
type GroupState struct {
	id  GroupID
	cfg GroupConfig

	raftNode raft.Raft
	kvStore  *fsm.KVStore
	watchMgr *fsm.WatchManager
	tp       raft.Transport

	watchCtxCancel context.CancelFunc
	logger         *zap.Logger
}

// groupHandlerWrapper satisfies transport.MessageHandler and forwards RPCs to
// the raft node once it has been created.  This is the same pattern as
// raftHandlerWrapper in cmd/raftd/main.go.
type groupHandlerWrapper struct {
	raftNode raft.Raft
}

func (h *groupHandlerWrapper) HandleAppendEntries(req *transport.AppendEntriesReq) *transport.AppendEntriesResp {
	if h.raftNode == nil {
		return &transport.AppendEntriesResp{Term: 0, Success: false}
	}
	rn, ok := h.raftNode.(interface {
		HandleAppendEntriesRPC(*raft.AppendEntriesRequest) *raft.AppendEntriesResponse
	})
	if !ok {
		return &transport.AppendEntriesResp{Term: 0, Success: false}
	}
	raftReq := &raft.AppendEntriesRequest{
		Term:         req.Term,
		LeaderID:     raft.ServerID(req.LeaderID),
		PrevLogIndex: req.PrevLogIndex,
		PrevLogTerm:  req.PrevLogTerm,
		Entries:      req.Entries,
		LeaderCommit: req.LeaderCommit,
	}
	resp := rn.HandleAppendEntriesRPC(raftReq)
	if resp == nil {
		return &transport.AppendEntriesResp{}
	}
	return &transport.AppendEntriesResp{
		Term:         resp.Term,
		Success:      resp.Success,
		Index:        resp.Index,
		ConflictTerm: resp.ConflictTerm,
	}
}

func (h *groupHandlerWrapper) HandleRequestVote(req *transport.RequestVoteReq) *transport.RequestVoteResp {
	if h.raftNode == nil {
		return &transport.RequestVoteResp{Term: 0, VoteGranted: false}
	}
	rn, ok := h.raftNode.(interface {
		HandleRequestVoteRPC(*raft.RequestVoteRequest) *raft.RequestVoteResponse
	})
	if !ok {
		return &transport.RequestVoteResp{Term: 0, VoteGranted: false}
	}
	raftReq := &raft.RequestVoteRequest{
		Term:           req.Term,
		CandidateID:    raft.ServerID(req.CandidateID),
		LastLogIndex:   req.LastLogIndex,
		LastLogTerm:    req.LastLogTerm,
		PreVote:        req.PreVote,
		LeaderTransfer: req.LeaderTransfer,
	}
	resp := rn.HandleRequestVoteRPC(raftReq)
	if resp == nil {
		return &transport.RequestVoteResp{}
	}
	return &transport.RequestVoteResp{
		Term:        resp.Term,
		VoteGranted: resp.VoteGranted,
		Reason:      resp.Reason,
	}
}

func (h *groupHandlerWrapper) HandleInstallSnapshot(req *transport.InstallSnapshotReq) *transport.InstallSnapshotResp {
	if h.raftNode == nil {
		return &transport.InstallSnapshotResp{Term: 0}
	}
	rn, ok := h.raftNode.(interface {
		HandleInstallSnapshotRPC(*raft.InstallSnapshotRequest) *raft.InstallSnapshotResponse
	})
	if !ok {
		return &transport.InstallSnapshotResp{Term: 0}
	}
	raftReq := &raft.InstallSnapshotRequest{
		Term:              req.Term,
		LeaderID:          raft.ServerID(req.LeaderID),
		LastIncludedIndex: req.LastIncludedIndex,
		LastIncludedTerm:  req.LastIncludedTerm,
		Offset:            req.Offset,
		Data:              req.Data,
		Done:              req.Done,
	}
	resp := rn.HandleInstallSnapshotRPC(raftReq)
	if resp == nil {
		return &transport.InstallSnapshotResp{}
	}
	return &transport.InstallSnapshotResp{Term: resp.Term}
}

func (h *groupHandlerWrapper) HandleTimeoutNow(_ *transport.TimeoutNowReq) *transport.TimeoutNowResp {
	if h.raftNode == nil {
		return &transport.TimeoutNowResp{}
	}
	type timeoutNowHandler interface {
		HandleTimeoutNowRPC()
	}
	if handler, ok := h.raftNode.(timeoutNowHandler); ok {
		handler.HandleTimeoutNowRPC()
	}
	return &transport.TimeoutNowResp{}
}

// newGroupState constructs (but does not start) a GroupState for the given
// GroupConfig.  baseDataDir is used to derive a default DataDir when
// cfg.DataDir is empty.
func newGroupState(cfg GroupConfig, baseDataDir string, logger *zap.Logger) (*GroupState, error) {
	dataDir := cfg.DataDir
	if dataDir == "" {
		dataDir = filepath.Join(baseDataDir, "group-"+strconv.FormatUint(cfg.ID, 10))
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir %q: %w", dataDir, err)
	}

	// --- WAL ---
	wal, err := storage.NewWAL(filepath.Join(dataDir, "wal"), nil)
	if err != nil {
		return nil, fmt.Errorf("group %d: WAL: %w", cfg.ID, err)
	}

	// --- StableStore ---
	stable, err := storage.NewStableStore(filepath.Join(dataDir, "stable.db"))
	if err != nil {
		wal.Close() //nolint:errcheck
		return nil, fmt.Errorf("group %d: StableStore: %w", cfg.ID, err)
	}

	// --- SnapshotStore ---
	snapshotStore, err := storage.NewFileSnapshotStore(dataDir, 2)
	if err != nil {
		wal.Close()    //nolint:errcheck
		stable.Close() //nolint:errcheck
		return nil, fmt.Errorf("group %d: SnapshotStore: %w", cfg.ID, err)
	}

	// --- FSM + WatchManager ---
	kvStore := fsm.NewKVStore()
	watchMgr := fsm.NewWatchManager(kvStore)

	// --- Build InitialConfiguration from Members if not already set ---
	raftCfg := cfg.RaftConfig
	if len(raftCfg.InitialConfiguration.Servers) == 0 && len(cfg.Members) > 0 {
		raftCfg.InitialConfiguration = raft.Configuration{Servers: cfg.Members}
	}

	// --- Transport + Raft (three-step ordering to avoid init cycle) ---
	listenAddr := raftCfg.InitialConfiguration.GetServer(raftCfg.LocalID)
	if listenAddr == nil {
		// Fall back to the listen address stored on the RaftConfig (if any custom
		// field were present); for now we require LocalID to be in Members.
		return nil, fmt.Errorf("group %d: local node %q not found in members", cfg.ID, raftCfg.LocalID)
	}
	localAddr := string(listenAddr.Address)

	// Step 1: create wrapper (raftNode is nil initially).
	wrapper := &groupHandlerWrapper{}

	// Step 2: create transport.
	tcpTrans, err := transport.NewTCPTransportWithConfig(
		localAddr,
		wrapper,
		transport.TCPTransportConfig{
			Timeout:         10 * time.Second,
			Logger:          logger.Named(fmt.Sprintf("group-%d-transport", cfg.ID)),
			BinaryTransport: true,
		},
	)
	if err != nil {
		wal.Close()    //nolint:errcheck
		stable.Close() //nolint:errcheck
		return nil, fmt.Errorf("group %d: TCP transport: %w", cfg.ID, err)
	}

	// Add peers (all members except self).
	for _, member := range cfg.Members {
		if member.ID == raftCfg.LocalID {
			continue
		}
		if aerr := tcpTrans.AddPeer(member.ID, member.Address); aerr != nil {
			logger.Warn("group: failed to add peer",
				zap.Uint64("group_id", cfg.ID),
				zap.String("peer", string(member.ID)),
				zap.Error(aerr))
		}
	}

	// Step 3: create Raft node.
	raftNode, err := raft.NewRaft(
		&raftCfg,
		raftCfg.LocalID,
		wal,
		stable,
		snapshotStore,
		kvStore,
		tcpTrans,
	)
	if err != nil {
		tcpTrans.Close() //nolint:errcheck
		wal.Close()      //nolint:errcheck
		stable.Close()   //nolint:errcheck
		return nil, fmt.Errorf("group %d: NewRaft: %w", cfg.ID, err)
	}

	// Wire the raft node into the handler wrapper.
	wrapper.raftNode = raftNode

	return &GroupState{
		id:       cfg.ID,
		cfg:      cfg,
		raftNode: raftNode,
		kvStore:  kvStore,
		watchMgr: watchMgr,
		tp:       tcpTrans,
		logger:   logger.Named(fmt.Sprintf("group-%d", cfg.ID)),
	}, nil
}

// Start starts the Raft node and the WatchManager for this group.
func (g *GroupState) Start() error {
	watchCtx, watchCancel := context.WithCancel(context.Background())
	g.watchCtxCancel = watchCancel
	g.watchMgr.Start(watchCtx)

	if err := g.raftNode.Start(); err != nil {
		watchCancel()
		return fmt.Errorf("group %d: raft Start: %w", g.id, err)
	}
	g.logger.Info("group started", zap.Uint64("group_id", g.id))
	return nil
}

// Stop shuts down the Raft node, WatchManager, and TCP transport for this group.
func (g *GroupState) Stop() error {
	if g.watchCtxCancel != nil {
		g.watchCtxCancel()
	}
	if err := g.raftNode.Shutdown(); err != nil {
		g.logger.Warn("group: raft Shutdown error",
			zap.Uint64("group_id", g.id), zap.Error(err))
	}
	if err := g.tp.Close(); err != nil {
		g.logger.Warn("group: transport Close error",
			zap.Uint64("group_id", g.id), zap.Error(err))
	}
	return nil
}

// Info returns the current status of this group as a GroupInfo struct.
func (g *GroupState) Info() GroupInfo {
	return GroupInfo{
		ID:            g.id,
		KeyRangeStart: g.cfg.KeyRangeStart,
		KeyRangeEnd:   g.cfg.KeyRangeEnd,
		IsLeader:      g.raftNode.State() == raft.StateLeader,
		LeaderID:      g.raftNode.Leader(),
		Term:          g.raftNode.Term(),
		AppliedIndex:  g.raftNode.AppliedIndex(),
	}
}

// AppliedIndex returns the last applied log index for this group.
func (g *GroupState) AppliedIndex() uint64 {
	return g.raftNode.AppliedIndex()
}
