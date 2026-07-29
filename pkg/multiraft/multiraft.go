// Package multiraft implements horizontal key-range partitioning on top of the
// existing Raft consensus implementation. Each shard (group) runs an
// independent Raft cluster with its own WAL, stable store, snapshot store, FSM
// (KVStore), WatchManager, and TCP transport listener.
//
// The MultiRaft type is the entry point. Construct it with New, call Start to
// bring up all groups, use Apply/Get/Range/Watch for data operations, and call
// Stop to tear down.
package multiraft

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/sanskarpan/raft-consensus/pkg/fsm"
	"go.uber.org/zap"
)

// ErrNoGroup is returned by Apply/Get/Range when the key falls outside all
// configured ranges. Callers should treat it as a 404.
var ErrNoGroup = errors.New("multiraft: no group configured for key")

// MultiRaft manages a collection of Raft shards and a RangeRouter that maps
// keys to the owning shard.
type MultiRaft struct {
	groups map[GroupID]*GroupState
	router *RangeRouter
	logger *zap.Logger
}

// New creates a MultiRaft with one GroupState per GroupConfig. All groups are
// wired (WAL, transport, Raft node) but NOT yet started — call Start() to
// bring them up.
func New(cfgs []GroupConfig, baseDataDir string, logger *zap.Logger) (*MultiRaft, error) {
	router, err := NewRangeRouter(cfgs)
	if err != nil {
		return nil, fmt.Errorf("multiraft: build router: %w", err)
	}

	groups := make(map[GroupID]*GroupState, len(cfgs))
	for _, cfg := range cfgs {
		gs, err := newGroupState(cfg, baseDataDir, logger)
		if err != nil {
			// Best-effort cleanup: stop already-created groups.
			for _, existing := range groups {
				existing.Stop() //nolint:errcheck
			}
			return nil, fmt.Errorf("multiraft: group %d: %w", cfg.ID, err)
		}
		groups[cfg.ID] = gs
	}

	return &MultiRaft{
		groups: groups,
		router: router,
		logger: logger,
	}, nil
}

// Start starts all configured Raft groups (Raft node + WatchManager).
// If any group fails to start, all previously started groups are stopped
// before the error is returned.
func (m *MultiRaft) Start() error {
	started := make([]*GroupState, 0, len(m.groups))
	for _, g := range m.groups {
		if err := g.Start(); err != nil {
			for _, s := range started {
				s.Stop() //nolint:errcheck
			}
			return err
		}
		started = append(started, g)
	}
	return nil
}

// Stop stops all Raft groups concurrently, logging but not propagating errors.
func (m *MultiRaft) Stop() {
	var wg sync.WaitGroup
	for _, g := range m.groups {
		wg.Add(1)
		g := g
		go func() {
			defer wg.Done()
			if err := g.Stop(); err != nil {
				m.logger.Warn("multiraft: error stopping group",
					zap.Uint64("group_id", g.id),
					zap.Error(err))
			}
		}()
	}
	wg.Wait()
}

// GroupFor returns the GroupState responsible for key, or ErrNoGroup if no
// configured shard covers it.
func (m *MultiRaft) GroupFor(key string) (*GroupState, error) {
	id, ok := m.router.GroupIDFor(key)
	if !ok {
		return nil, fmt.Errorf("%w: key=%q", ErrNoGroup, key)
	}
	g, ok := m.groups[id]
	if !ok {
		return nil, fmt.Errorf("multiraft: group %d not found in registry", id)
	}
	return g, nil
}

// Apply routes a command to the shard that owns key and calls raft.Apply on
// the leader of that shard.
func (m *MultiRaft) Apply(ctx context.Context, key string, cmd []byte) ([]byte, error) {
	g, err := m.GroupFor(key)
	if err != nil {
		return nil, err
	}
	return g.raftNode.Apply(ctx, cmd)
}

// Get returns the KeyValue for key from the owning shard's FSM (stale read).
// Returns (nil, false, nil) when the key does not exist.
// Returns (nil, false, ErrNoGroup) when no shard covers the key.
func (m *MultiRaft) Get(key string) (*fsm.KeyValue, bool, error) {
	g, err := m.GroupFor(key)
	if err != nil {
		return nil, false, err
	}
	kv, err := g.kvStore.Get(key)
	if err != nil {
		return nil, false, err
	}
	return kv, kv != nil, nil
}

// Range fans out a prefix scan to all shards whose key range overlaps prefix,
// merges the results, and returns them sorted by key. An error from any shard
// is returned immediately (no partial result).
func (m *MultiRaft) Range(prefix string) ([]*fsm.KeyValue, error) {
	groupIDs := m.router.GroupsForPrefix(prefix)

	type shardResult struct {
		kvs []*fsm.KeyValue
		err error
	}

	results := make(chan shardResult, len(groupIDs))
	for _, id := range groupIDs {
		g, ok := m.groups[id]
		if !ok {
			continue
		}
		go func(gs *GroupState) {
			kvs, err := gs.kvStore.Range(prefix)
			results <- shardResult{kvs: kvs, err: err}
		}(g)
	}

	var merged []*fsm.KeyValue
	for i := 0; i < len(groupIDs); i++ {
		r := <-results
		if r.err != nil {
			return nil, r.err
		}
		merged = append(merged, r.kvs...)
	}

	// Sort the merged slice by key for a consistent, deterministic response.
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].Key < merged[j].Key
	})

	return merged, nil
}

// Watch subscribes to key-change events on the shard that owns key.
// Returns (nil, 0, ErrNoGroup) when no shard covers the key.
func (m *MultiRaft) Watch(ctx context.Context, key string, sinceRevision int64) (<-chan fsm.WatchEvent, fsm.WatchID, error) {
	g, err := m.GroupFor(key)
	if err != nil {
		return nil, 0, err
	}
	ch, id := g.watchMgr.Watch(ctx, key, sinceRevision)
	return ch, id, nil
}

// Groups returns a snapshot of the current status of all shards.
func (m *MultiRaft) Groups() []GroupInfo {
	infos := make([]GroupInfo, 0, len(m.groups))
	for _, g := range m.groups {
		infos = append(infos, g.Info())
	}
	// Return in a stable order (sorted by GroupID) for deterministic JSON output.
	sort.Slice(infos, func(i, j int) bool {
		return infos[i].ID < infos[j].ID
	})
	return infos
}
