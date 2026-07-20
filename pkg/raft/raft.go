package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"time"

	"github.com/sanskarpan/raft-consensus/pkg/metrics"
	"github.com/sanskarpan/raft-consensus/pkg/tracing"
	"go.uber.org/zap"
)

var (
	defaultElectionTick  = 10
	defaultHeartbeatTick = 1
	defaultMaxSizePerMsg = mathMaxInt64
	defaultMaxInflight   = 256
)

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// proposalFuture carries a client command through the proposal pipeline.
type proposalFuture struct {
	data   []byte
	future *ApplyFuture
	ctx    context.Context // caller's trace context, for write-path spans (#213)
}

type raft struct {
	localID ServerID

	config *Config

	state    RaftState
	term     uint64
	votedFor ServerID

	log      LogStore
	stable   StableStore
	snapshot SnapshotStore
	fsm      FSM

	transport Transport
	logger    *zap.Logger

	lastIndex uint64
	lastTerm  uint64

	commitIndex uint64
	applyIndex  uint64

	configuration Configuration

	// jointConfig is non-nil when we are in the joint-consensus phase of a
	// membership change. It holds both the old and new configurations.
	jointConfig *JointConfiguration

	// pendingConfigIndex is the log index of the most recently appended
	// configuration-change entry. A new membership change is refused until this
	// entry commits (pendingConfigIndex <= commitIndex), enforcing the
	// at-most-one-outstanding-config-change rule (C7).
	pendingConfigIndex uint64

	leaderID ServerID

	votes     map[ServerID]bool
	voteCount int

	// inPreVote is true when we are running a pre-vote phase (not yet
	// incrementing term or updating votedFor).
	inPreVote    bool
	preVotes     map[ServerID]bool
	preVoteCount int

	leadershipTransfer *leadershipTransfer

	// nextIndex and matchIndex track per-follower replication progress when this
	// node is leader.
	nextIndex  map[ServerID]uint64
	matchIndex map[ServerID]uint64

	// inflightReplication guards against unbounded goroutine growth: at most one
	// replicateTo goroutine may be in flight per follower at a time (H1). Without
	// this, a slow/partitioned follower would accumulate one new goroutine every
	// heartbeat tick. Access is guarded by r.mu.
	inflightReplication map[ServerID]bool

	// inflightWindows tracks the per-follower flow-control window: how many
	// AppendEntries batches have been sent but not yet acknowledged. Bounded by
	// config.MaxInflight. Guarded by r.mu.
	inflightWindows map[ServerID]*inflightWindow

	// followerStates implements the Probe/Replicate state machine per follower.
	// stateProbe: send one batch at a time (initial or after rejection).
	// stateReplicate: pipeline up to MaxInflight batches. Guarded by r.mu.
	followerStates map[ServerID]followerState

	// heartbeatAcks records the last time each follower acknowledged an
	// AppendEntries RPC at the current term.  Used to compute leader-lease
	// validity for ReadIndex.
	heartbeatAcks map[ServerID]time.Time

	// pendingFutures maps log index -> ApplyFuture waiting for commit+apply.
	pendingFutures map[uint64]*ApplyFuture

	// electionTicks counts ticks without hearing from a leader (followers/candidates).
	electionTicks int

	// clock is the injectable time source. Defaults to realClock (wall clock).
	// Set from Config.Clock in newRaft(); simulation tests inject a simClock.
	clock Clock
	// newTicker is the injectable ticker factory. Defaults to newRealTicker.
	// Set from Config.NewTicker in newRaft(); simulation tests inject simClock.NewTicker.
	newTicker func(d time.Duration) Ticker

	ticker         Ticker
	stopCh         chan struct{}

	snapshotTicker Ticker
	snapshotIndex  uint64

	// snapReasm reassembles the Offset-ordered chunks of an incoming
	// InstallSnapshot transfer. The leader sends a snapshot larger than
	// snapshotChunkSize as a sequence of chunks (one RPC each, Done only on the
	// last), so the receiver must accumulate them before restoring the FSM.
	// Guarded by r.mu (all writes happen inside handleInstallSnapshot).
	snapReasm snapshotReassembly

	// proposalCh carries client Apply() requests into the run() goroutine.
	proposalCh chan *proposalFuture

	// commitNotifyCh is used by replication goroutines to wake run() when
	// commitIndex may have advanced.
	commitNotifyCh chan struct{}

	// persistCommitCh signals the run() goroutine that commitIndex changed and
	// should be flushed to the stable store OUTSIDE the r.mu critical section
	// (H1: never hold r.mu across a disk write/sync). It is a coalescing signal
	// (buffered size 1); run() reads the current commitIndex under a short lock
	// and then performs the stable.Set with the lock released.
	persistCommitCh chan struct{}

	// heartbeatTrigger asks the run() loop to send an immediate heartbeat round
	// (coalescing, buffered size 1). Used by heartbeat-confirmed ReadIndex (M4)
	// to obtain fresh leadership confirmation without waiting for the next tick.
	heartbeatTrigger chan struct{}

	fsmSnapshotCh chan *reqSnapshotFuture

	userSnapshotCh chan *reqSnapshotFuture

	restoreCh chan *restoreFuture

	// doneCh is closed at the very end of run() so Shutdown() can block until the
	// final drain/flush has completed (H-R1). It is recreated in Start().
	doneCh chan struct{}

	// fatalErr, when non-nil, records a persistent storage-write failure
	// (H-R2) or an unrecoverable FSM apply panic (H-R3) that has forced the node
	// to stop. A node with a non-nil fatalErr is unhealthy and must not count
	// toward quorum. Guarded by r.mu.
	fatalErr error

	mu sync.RWMutex

	// applyCond broadcasts whenever applyIndex advances so that WaitApplied
	// callers can block for a specific applied index without busy-polling (L9).
	// It is guarded by applyMu (an independent lock, NOT r.mu, so the apply path
	// never has to acquire two locks in a fixed order and waiters never contend
	// on r.mu). appliedForWait mirrors r.applyIndex under applyMu.
	applyMu sync.Mutex
	// applyWaitCh is closed+replaced by notifyApplied on each apply-index
	// advance; WaitApplied selects on it instead of spawning a per-call watcher
	// goroutine (L2). appliedForWait mirrors r.applyIndex under applyMu.
	applyWaitCh    chan struct{}
	appliedForWait uint64
}

// followerState is the per-follower replication mode for pipelining (#200.4).
type followerState uint8

const (
	// stateProbe means we send at most one batch in-flight at a time while
	// discovering the follower's log position (initial state or after rejection).
	stateProbe followerState = iota
	// stateReplicate means we may pipeline up to MaxInflight batches before
	// waiting for acknowledgement (entered after first successful replication).
	stateReplicate
)

type leadershipTransfer struct {
	target   ServerID
	complete chan struct{}
	err      error

	// timeoutNowSent records the tick at which we sent TimeoutNow so the
	// transfer can be abandoned with a timeout if leadership does not change
	// within a bounded number of ticks (M1).
	sent           bool
	deadline       time.Time
	completeClosed bool
}

// transferring reports whether a leadership transfer is in progress. While true,
// handleProposal rejects new proposals (M1) so the log does not grow past the
// point the target is being asked to catch up to. Caller must hold r.mu.
func (r *raft) transferring() bool {
	return r.leadershipTransfer != nil
}

// finishTransfer resolves the pending leadership transfer future exactly once.
// Caller must hold r.mu (write).
func (r *raft) finishTransfer(err error) {
	lt := r.leadershipTransfer
	if lt == nil {
		return
	}
	if !lt.completeClosed {
		lt.err = err
		lt.completeClosed = true
		close(lt.complete)
	}
	r.leadershipTransfer = nil
}

type restoreFuture struct {
	snapshot io.Reader
	index    uint64
	term     uint64
	future   *ApplyFuture
}

type reqSnapshotFuture struct {
	done chan struct{}
	err  error
}

func newRaft(config *Config, localID ServerID, log LogStore, stable StableStore, snapshot SnapshotStore, fsm FSM, transport Transport) (*raft, error) {
	// Apply defaults before validation.
	if config.ElectionTick == 0 {
		config.ElectionTick = defaultElectionTick
	}
	if config.HeartbeatTick == 0 {
		config.HeartbeatTick = defaultHeartbeatTick
	}
	if config.MaxSizePerMsg == 0 {
		config.MaxSizePerMsg = defaultMaxSizePerMsg
	}
	if config.MaxInflight == 0 {
		config.MaxInflight = defaultMaxInflight
	}

	if err := config.Validate(); err != nil {
		return nil, err
	}

	r := &raft{
		localID:   localID,
		config:    config,
		log:       log,
		stable:    stable,
		snapshot:  snapshot,
		fsm:       fsm,
		transport: transport,
		logger:    zap.NewNop(),

		state:    StateShutdown,
		term:     0,
		votedFor: "",

		votes:               make(map[ServerID]bool),
		pendingFutures:      make(map[uint64]*ApplyFuture),
		nextIndex:           make(map[ServerID]uint64),
		matchIndex:          make(map[ServerID]uint64),
		inflightReplication: make(map[ServerID]bool),
		inflightWindows:     make(map[ServerID]*inflightWindow),
		followerStates:      make(map[ServerID]followerState),
		heartbeatAcks:       make(map[ServerID]time.Time),

		proposalCh:       make(chan *proposalFuture, 256),
		commitNotifyCh:   make(chan struct{}, 1),
		persistCommitCh:  make(chan struct{}, 1),
		heartbeatTrigger: make(chan struct{}, 1),
		fsmSnapshotCh:    make(chan *reqSnapshotFuture, 1),

		userSnapshotCh: make(chan *reqSnapshotFuture, 1),
		restoreCh:      make(chan *restoreFuture, 1),

		stopCh: make(chan struct{}),
	}

	r.applyWaitCh = make(chan struct{})

	// Initialize injectable clock and ticker factory from config; fall back to
	// real-wall-clock implementations when not set (production path).
	if config.Clock != nil {
		r.clock = config.Clock
	} else {
		r.clock = realClock{}
	}
	if config.NewTicker != nil {
		r.newTicker = config.NewTicker
	} else {
		r.newTicker = newRealTicker
	}

	if transport != nil {
		transport.SetLocalID(localID)
	}

	return r, nil
}

func (r *raft) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != StateShutdown {
		return ErrAlreadyStarted
	}

	if err := r.loadConfiguration(); err != nil {
		return err
	}

	if err := r.loadTerm(); err != nil {
		return err
	}

	if err := r.loadLastIndex(); err != nil {
		return err
	}

	if err := r.loadCommitIndex(); err != nil {
		return err
	}

	// C5: if the log was compacted after a snapshot, initialize state from the
	// latest snapshot so the node knows its true lastIndex/lastTerm (and does not
	// re-apply already-snapshotted entries or fail the election restriction).
	if err := r.loadSnapshotState(); err != nil {
		return err
	}

	// Safety: if the WAL was truncated in a previous run and the node crashed
	// before the reduced commitIndex was persisted, cap it here so that
	// applyCommitted never tries to read entries that no longer exist.
	if r.commitIndex > r.lastIndex {
		r.commitIndex = r.lastIndex
	}

	if r.config.StartAsLearner {
		r.state = StateLearner
	} else {
		r.state = StateFollower
	}
	r.leaderID = ""
	r.electionTicks = r.randomElectionTickCount()
	r.ticker = r.newTicker(r.heartbeatInterval())
	r.stopCh = make(chan struct{})
	r.doneCh = make(chan struct{})
	r.fatalErr = nil

	if r.config.SnapshotInterval > 0 {
		r.snapshotTicker = r.newTicker(r.config.SnapshotInterval)
		r.snapshotIndex = r.lastIndex
	}

	go r.run()

	r.logger.Info("raft started", zap.String("id", r.localID.String()), zap.Uint64("term", r.term))

	return nil
}

func (r *raft) Shutdown() error {
	r.mu.Lock()

	if r.state == StateShutdown {
		// If a fatal error already stopped run() (H-R2/H-R3), the loop closed
		// doneCh itself; nothing more to wait on.
		r.mu.Unlock()
		return nil
	}

	r.state = StateShutdown
	r.ticker.Stop()
	if r.snapshotTicker != nil {
		r.snapshotTicker.Stop()
	}
	// Guard against a double-close: a fatal-error stop may have already closed
	// stopCh (see stopFatal).
	select {
	case <-r.stopCh:
	default:
		close(r.stopCh)
	}
	doneCh := r.doneCh

	r.logger.Info("raft stopping", zap.String("id", r.localID.String()))
	r.mu.Unlock()

	// H-R1: block until run() finishes its final drain/flush so no committed
	// write is lost by a premature process exit. Bounded so a stuck FSM/disk
	// cannot hang Shutdown forever.
	if doneCh != nil {
		select {
		case <-doneCh:
		case <-time.After(r.shutdownTimeout()):
			r.logger.Warn("raft shutdown drain timed out", zap.String("id", r.localID.String()))
		}
	}

	r.logger.Info("raft stopped", zap.String("id", r.localID.String()))
	return nil
}

// shutdownTimeout bounds how long Shutdown() waits for run() to finish its final
// drain. Scaled off the election timeout with a sane floor.
func (r *raft) shutdownTimeout() time.Duration {
	d := 10 * r.electionTimeout()
	if d < 5*time.Second {
		d = 5 * time.Second
	}
	return d
}

// stopFatal marks the node as permanently stopped due to an unrecoverable
// storage-write failure (H-R2) or FSM apply panic (H-R3). The node transitions
// to StateShutdown, records fatalErr so health checks can observe it, and closes
// stopCh so run() exits. Caller must hold r.mu (write).
func (r *raft) stopFatal(err error) {
	if r.fatalErr == nil {
		r.fatalErr = err
	}
	if r.state == StateShutdown {
		return
	}
	r.state = StateShutdown
	r.leaderID = ""
	if r.ticker != nil {
		r.ticker.Stop()
	}
	if r.snapshotTicker != nil {
		r.snapshotTicker.Stop()
	}
	metrics.RecordLeaderID(false)
	r.logger.Error("raft node halted due to fatal error", zap.String("id", r.localID.String()), zap.Error(err))
	select {
	case <-r.stopCh:
	default:
		close(r.stopCh)
	}
}

// FatalError returns the fatal error that stopped the node, or nil if healthy.
// A non-nil result means the node has halted (H-R2/H-R3) and must not be counted
// toward quorum.
func (r *raft) FatalError() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.fatalErr
}

// Healthy reports whether the node is running without a fatal storage/FSM error.
func (r *raft) Healthy() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.fatalErr == nil && r.state != StateShutdown
}

func (r *raft) run() {
	// H-R1: signal Shutdown() that the final drain/flush has completed.
	defer close(r.doneCh)

	var snapshotCh <-chan time.Time
	if r.snapshotTicker != nil {
		snapshotCh = r.snapshotTicker.C()
	}

	for {
		select {
		case <-r.ticker.C():
			r.tick()

		case proposal := <-r.proposalCh:
			r.handleProposalBatch(proposal)

		case req := <-r.userSnapshotCh:
			r.processSnapshot(req)

		case req := <-r.restoreCh:
			r.processRestore(req)

		case <-r.commitNotifyCh:
			// Replication goroutine notified us that commitIndex advanced.

		case <-r.persistCommitCh:
			// H1: flush commitIndex to disk here, OUTSIDE any r.mu critical
			// section, so we never hold the lock across a stable-store write.
			r.flushCommitIndex()

		case <-r.heartbeatTrigger:
			// M4: heartbeat-confirmed ReadIndex asked for an immediate heartbeat
			// round so a fresh leadership confirmation arrives promptly.
			r.mu.Lock()
			if r.state == StateLeader {
				r.sendHeartbeat()
			}
			r.mu.Unlock()

		case <-snapshotCh:
			r.triggerSnapshot()

		case <-r.stopCh:
			r.drainFuturesOnShutdown()
			return
		}

		// Apply committed entries to the FSM after every event.
		r.applyCommitted()

		// H-R2/H-R3: a fatal storage-write failure or FSM apply panic sets
		// fatalErr and closes stopCh. Exit the loop promptly (still draining
		// futures) rather than spin as a zombie member.
		r.mu.RLock()
		fatal := r.fatalErr != nil
		r.mu.RUnlock()
		if fatal {
			r.drainFuturesOnShutdown()
			return
		}
	}
}

// tick is called on every heartbeat-interval tick.
func (r *raft) tick() {
	r.mu.Lock()
	defer r.mu.Unlock()

	switch r.state {
	case StateFollower:
		r.tickFollower()
	case StateCandidate:
		r.tickCandidate()
	case StateLeader:
		r.tickLeader()
	case StateLearner:
		// Learners never time out or start elections; do nothing.
	}
}

// tickFollower counts ticks and starts an election after the randomized
// election timeout has elapsed without hearing from a leader.
func (r *raft) tickFollower() {
	r.electionTicks--
	if r.electionTicks <= 0 {
		r.electionTicks = r.randomElectionTickCount()
		r.startElection()
	}
}

// tickCandidate restarts the election after a timeout (e.g. split vote).
func (r *raft) tickCandidate() {
	r.electionTicks--
	if r.electionTicks <= 0 {
		r.electionTicks = r.randomElectionTickCount()
		r.startElection()
	}
}

// checkQuorum steps the leader down if it has not received acknowledgements from
// a quorum of voters within the last election timeout. This bounds how long a
// partitioned-minority leader keeps leadership (and keeps failing/forwarding
// writes to nowhere) before a majority-side election can succeed. Opt-in via
// Config.CheckQuorum. Caller must hold r.mu (write); called from tickLeader.
func (r *raft) checkQuorum() {
	if !r.config.CheckQuorum || r.state != StateLeader {
		return
	}
	quorum := r.configuration.QuorumSize()
	if quorum <= 1 {
		return // single-voter cluster always has quorum with itself
	}

	cutoff := r.clock.Now().Add(-r.electionTimeout())
	count := 1 // self
	for id, ackTime := range r.heartbeatAcks {
		if ackTime.After(cutoff) && r.configuration.IsVoter(id) {
			count++
		}
	}
	if count >= quorum {
		return
	}

	r.logger.Warn("CheckQuorum: lost contact with a quorum of voters; stepping down",
		zap.Int("acked_voters", count), zap.Int("quorum", quorum), zap.Uint64("term", r.term))
	metrics.RecordRejection("check_quorum")

	// Same-term step-down: keep term/votedFor (we did not observe a higher term),
	// but relinquish leadership so a majority-side node can win an election.
	r.state = StateFollower
	r.leaderID = ""
	r.electionTicks = r.randomElectionTickCount()
	r.heartbeatAcks = make(map[ServerID]time.Time)
	metrics.RecordLeaderID(false)
	r.failPendingFutures()
	if r.leadershipTransfer != nil {
		r.finishTransfer(ErrNotLeader)
	}
}

func (r *raft) tickLeader() {
	r.checkQuorum()
	// checkQuorum may have stepped us down; only continue leader work if we are
	// still the leader.
	if r.state != StateLeader {
		return
	}
	r.sendHeartbeat()

	// If a leadership transfer is pending, check whether the target is caught
	// up and, if so, send TimeoutNow.
	if r.leadershipTransfer != nil {
		r.doLeadershipTransfer()
	}
}

// doLeadershipTransfer selects the best transfer target (or uses the one
// specified in leadershipTransfer.target), checks whether it is caught up,
// and sends a TimeoutNow RPC to force an immediate election on the target.
// Must be called with r.mu held (write).
func (r *raft) doLeadershipTransfer() {
	lt := r.leadershipTransfer

	// M1: give up with a timeout if leadership has not changed in time. We are
	// still leader here (called from tickLeader), so if the deadline has passed
	// the target never took over — report failure rather than false success.
	if lt.sent && !lt.deadline.IsZero() && r.clock.Now().After(lt.deadline) {
		r.logger.Warn("leadership transfer: timed out waiting for target to take over",
			zap.String("target", lt.target.String()),
		)
		r.finishTransfer(ErrTimeout)
		return
	}

	target := lt.target
	if target == "" {
		// Pick the follower with the highest matchIndex (non-learner).
		var bestMatch uint64
		for id, match := range r.matchIndex {
			s := r.configuration.GetServer(id)
			if s == nil || s.Learner {
				continue
			}
			if match > bestMatch {
				bestMatch = match
				target = id
			}
		}
		lt.target = target
	}

	if target == "" {
		r.logger.Warn("leadership transfer: no suitable target found")
		r.finishTransfer(ErrEmptyCluster)
		return
	}

	// Only proceed once the target has caught up to our last log index.
	if r.matchIndex[target] < r.lastIndex {
		// Not yet caught up; keep waiting (will retry on next tick).
		return
	}

	// Send TimeoutNow at most once and arm the timeout. Do NOT close the future
	// here (M1): success is reported only once leadership actually changes (this
	// node steps down — see finishTransfer calls on the step-down paths) or the
	// deadline fires above.
	if !lt.sent {
		r.logger.Info("leadership transfer: sending TimeoutNow",
			zap.String("target", target.String()),
		)
		lt.sent = true
		lt.deadline = r.clock.Now().Add(r.electionTimeout())
		go func() {
			// M-C1: bounded, cancel-on-shutdown context.
			ctx, cancel := r.rpcContext()
			defer cancel()
			_ = r.transport.TimeoutNow(ctx, target)
		}()
	}
}

func (r *raft) startElection() {
	r.startElectionWith(false, false)
}

// startElectionWith runs a real election. alreadyPreVoted must be true when the
// caller has just completed a successful pre-vote round; passing it explicitly
// (L2) avoids relying on the shared r.inPreVote flag to decide whether to
// recurse into startPreVote, which was racy across concurrent pre-vote replies.
func (r *raft) startElectionWith(alreadyPreVoted, leaderTransfer bool) {
	// If pre-vote is enabled and we have NOT already gathered a pre-vote quorum,
	// run the pre-vote phase first. startPreVote calls startElectionWith(true)
	// once a quorum of pre-votes is gathered. Leadership-transfer campaigns skip
	// pre-vote entirely (immediate election, as the target is already caught up).
	if r.config.PreVote && !alreadyPreVoted && !leaderTransfer {
		r.startPreVote()
		return
	}

	// Reset the inPreVote flag so subsequent elections are gated again.
	r.inPreVote = false

	r.state = StateCandidate
	r.term++
	r.votedFor = r.localID
	r.leaderID = ""
	r.votes = make(map[ServerID]bool)
	r.voteCount = 1

	r.persistTermAndVotedForLogged()

	metrics.RecordElection()
	metrics.RecordTerm(r.term)

	r.logger.Info("starting election",
		zap.String("id", r.localID.String()),
		zap.Uint64("term", r.term),
	)

	lastIndex, lastTerm := r.lastIndex, r.lastTerm
	r.votes[r.localID] = true

	// Single-node cluster: win immediately.
	if r.configuration.QuorumSize() == 1 {
		r.becomeLeader()
		return
	}

	for _, server := range r.configuration.Servers {
		serverID := server.ID
		if serverID == r.localID || server.Learner {
			continue
		}

		req := &RequestVoteRequest{
			Term:           r.term,
			CandidateID:    r.localID,
			LastLogIndex:   lastIndex,
			LastLogTerm:    lastTerm,
			PreVote:        false,
			LeaderTransfer: leaderTransfer,
		}

		go r.sendVoteRequest(serverID, req)
	}
}

// startPreVote runs phase-1 of the pre-vote protocol.  It sends RequestVote
// RPCs with PreVote=true and Term=r.term+1 WITHOUT modifying persistent state.
// If a quorum of pre-votes is received, startElection is called with
// inPreVote=true so that the real election begins immediately.
func (r *raft) startPreVote() {
	r.state = StateCandidate
	r.inPreVote = true
	r.preVotes = make(map[ServerID]bool)
	r.preVoteCount = 1 // count ourselves
	r.preVotes[r.localID] = true

	r.logger.Info("starting pre-vote",
		zap.String("id", r.localID.String()),
		zap.Uint64("proposed_term", r.term+1),
	)

	// Single-node cluster: skip to the real election immediately.
	if r.configuration.QuorumSize() == 1 {
		r.startElectionWith(true, false)
		return
	}

	lastIndex, lastTerm := r.lastIndex, r.lastTerm

	for _, server := range r.configuration.Servers {
		serverID := server.ID
		if serverID == r.localID || server.Learner {
			continue
		}

		req := &RequestVoteRequest{
			Term:         r.term + 1, // proposed next term
			CandidateID:  r.localID,
			LastLogIndex: lastIndex,
			LastLogTerm:  lastTerm,
			PreVote:      true,
		}

		go r.sendPreVoteRequest(serverID, req)
	}
}

// sendPreVoteRequest sends a single pre-vote RPC and processes the result.
func (r *raft) sendPreVoteRequest(serverID ServerID, req *RequestVoteRequest) {
	// M-C1: bounded, cancel-on-shutdown context. H-O2: trace span. C-O1: latency.
	ctx, cancel := r.rpcContext()
	defer cancel()
	spanCtx, span := tracing.SpanRequestVote(ctx, serverID.String(), req.Term)
	start := r.clock.Now()
	resp, err := r.transport.RequestVote(spanCtx, serverID, req)
	metrics.RecordRequestVoteLatency(r.clock.Now().Sub(start).Seconds())
	if err != nil {
		tracing.RecordError(span, err)
	}
	span.End()
	if err != nil {
		r.logger.Warn("failed to send pre-vote request",
			zap.String("target", serverID.String()),
			zap.Error(err),
		)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// If we are no longer in the pre-vote phase, discard the response.
	if !r.inPreVote || r.state != StateCandidate {
		return
	}

	if resp.Term > r.term {
		// Someone has a higher term; abort pre-vote and step down.
		r.term = resp.Term
		r.state = StateFollower
		r.votedFor = ""
		r.inPreVote = false
		r.persistTermAndVotedForLogged()
		return
	}

	if resp.VoteGranted {
		r.preVotes[serverID] = true
		r.preVoteCount = 0
		for _, granted := range r.preVotes {
			if granted {
				r.preVoteCount++
			}
		}

		// C6: joint-consensus-aware quorum for pre-vote as well.
		if r.hasVoteQuorum(r.preVotes) {
			// Pre-vote quorum reached: run the real election. Pass the flag
			// explicitly (L2) so we don't recurse back into startPreVote.
			r.startElectionWith(true, false)
		}
	}
}

func (r *raft) sendVoteRequest(serverID ServerID, req *RequestVoteRequest) {
	// M-C1: bounded, cancel-on-shutdown context. H-O2: trace span. C-O1: latency.
	ctx, cancel := r.rpcContext()
	defer cancel()
	spanCtx, span := tracing.SpanRequestVote(ctx, serverID.String(), req.Term)
	start := r.clock.Now()
	resp, err := r.transport.RequestVote(spanCtx, serverID, req)
	metrics.RecordRequestVoteLatency(r.clock.Now().Sub(start).Seconds())
	if err != nil {
		tracing.RecordError(span, err)
	}
	span.End()
	if err != nil {
		r.logger.Warn("failed to send vote request",
			zap.String("target", serverID.String()),
			zap.Error(err),
		)
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != StateCandidate {
		return
	}

	if resp.Term > r.term {
		r.term = resp.Term
		r.state = StateFollower
		r.votedFor = ""
		r.persistTermAndVotedForLogged()
		return
	}

	if resp.Term != r.term {
		return
	}

	if resp.VoteGranted {
		r.votes[serverID] = true
		metrics.RecordVoteGranted()

		r.voteCount = 0
		for _, voted := range r.votes {
			if voted {
				r.voteCount++
			}
		}

		// C6: during joint consensus this requires a majority in BOTH configs.
		if r.hasVoteQuorum(r.votes) {
			r.becomeLeader()
		}
	}
}

// configChangePending reports whether a configuration change is already
// outstanding — either we are mid joint-transition, or the last appended config
// entry has not yet committed. Raft permits at most one at a time (C7). Caller
// must hold r.mu.
func (r *raft) configChangePending() bool {
	return r.jointConfig != nil || r.pendingConfigIndex > r.commitIndex
}

// hasVoteQuorum reports whether the granted votes form a winning quorum. During
// joint consensus (C6) it requires a majority in BOTH the old and new
// configurations; otherwise a majority of the single active configuration. Only
// votes from voting members are counted (non-voters/learners are ignored).
func (r *raft) hasVoteQuorum(votes map[ServerID]bool) bool {
	countVoters := func(cfg Configuration) int {
		n := 0
		for id, granted := range votes {
			if granted && cfg.IsVoter(id) {
				n++
			}
		}
		return n
	}
	if r.jointConfig != nil {
		return countVoters(r.jointConfig.OldConfig) >= r.jointConfig.OldConfig.QuorumSize() &&
			countVoters(r.jointConfig.NewConfig) >= r.jointConfig.NewConfig.QuorumSize()
	}
	return countVoters(r.configuration) >= r.configuration.QuorumSize()
}

func (r *raft) becomeLeader() {
	r.state = StateLeader
	metrics.RecordLeaderChange() // this node observes itself as the new leader
	r.leaderID = r.localID
	r.leadershipTransfer = nil
	// A freshly elected leader must re-confirm quorum before serving
	// linearizable reads.  Clear stale acks from the previous term.
	r.heartbeatAcks = make(map[ServerID]time.Time)

	metrics.RecordLeaderID(true)

	r.logger.Info("became leader",
		zap.String("id", r.localID.String()),
		zap.Uint64("term", r.term),
	)

	// Initialize per-follower replication trackers for ALL non-local servers
	// (including learners so they receive log entries and can catch up).
	r.nextIndex = make(map[ServerID]uint64)
	r.matchIndex = make(map[ServerID]uint64)
	r.inflightWindows = make(map[ServerID]*inflightWindow)
	r.followerStates = make(map[ServerID]followerState)

	allServers := r.configuration.Servers
	if r.jointConfig != nil {
		allServers = r.jointConfig.AllServers()
	}
	for _, server := range allServers {
		if server.ID != r.localID {
			r.nextIndex[server.ID] = r.lastIndex + 1
			r.matchIndex[server.ID] = 0
			r.inflightWindows[server.ID] = newInflightWindow(r.config.MaxInflight)
			r.followerStates[server.ID] = stateProbe
		}
	}

	// Append a no-op entry so the leader can discover the commit point.
	noop := &LogEntry{
		Term:  r.term,
		Index: r.lastIndex + 1,
		Type:  EntryNormal,
		Data:  nil,
	}
	if err := r.persistLog([]*LogEntry{noop}); err != nil {
		r.logger.Error("failed to append no-op entry", zap.Error(err))
	}

	r.sendHeartbeat()

	// Single-node cluster: the leader is its own quorum. Advance commitIndex
	// immediately so that the no-op (and all pre-existing log entries) become
	// committed and are applied to the FSM. Without this, a restarted single-
	// node leader would never advance commitIndex because there are no followers
	// to acknowledge the no-op via replicateTo/advanceCommitIndex.
	if r.configuration.QuorumSize() == 1 {
		r.advanceCommitIndex()
	}
}

// sendHeartbeat triggers replication/heartbeat to all non-local servers,
// including learners (so they stay caught up) and, during joint consensus,
// all servers in both old and new configurations.
// Must be called with r.mu held.
func (r *raft) sendHeartbeat() {
	allServers := r.configuration.Servers
	if r.jointConfig != nil {
		allServers = r.jointConfig.AllServers()
	}
	for _, server := range allServers {
		if server.ID == r.localID {
			continue
		}
		r.spawnReplication(server.ID)
	}
}

// spawnReplication launches a single replicateTo goroutine for the follower,
// but only if one is not already in flight (H1). This bounds the number of
// concurrent replication goroutines to one per follower, so a slow follower can
// no longer cause a per-tick goroutine explosion. Must be called with r.mu held
// (read or write) — it takes the in-flight slot under the caller's lock.
func (r *raft) spawnReplication(serverID ServerID) {
	if r.inflightReplication[serverID] {
		return
	}
	r.inflightReplication[serverID] = true
	go r.replicateTo(serverID)
}

// replicateTo sends the appropriate AppendEntries RPC to a follower.
// It may carry new log entries or act as a heartbeat with no entries.
// replicateTo drains outstanding entries to a follower. M-P1/M-P2: instead of
// sending a single AppendEntries and re-spawning a goroutine on the next tick,
// one goroutine keeps sending (replicateOnce) as long as the follower is still
// behind or a rejection must be retried — continuous replication that amortizes
// RTT over a backlog and avoids per-tick goroutine churn during replication.
// The single-in-flight-per-follower invariant (H1) is preserved by the caller's
// inflightReplication guard; a heartbeat to a caught-up follower is a single
// pass. Safety of matchIndex/nextIndex updates is unchanged (see replicateOnce).
func (r *raft) replicateTo(serverID ServerID) {
	// H1: release the in-flight slot when this goroutine exits so the next tick
	// (or proposal) can start a fresh replication attempt for this follower.
	defer func() {
		r.mu.Lock()
		delete(r.inflightReplication, serverID)
		r.mu.Unlock()
	}()

	for r.replicateOnce(serverID) {
		// Follower still behind (or a reject needs an immediate retry from the
		// backed-off nextIndex): keep draining without waiting for the next tick.
	}
}

// replicateOnce sends one AppendEntries (or InstallSnapshot) to a follower and
// applies the response. It returns true if the caller should immediately send
// again — i.e. the follower is still behind after a successful batch, a
// rejection was recorded (retry from the backed-off nextIndex), or a snapshot
// was streamed — and false when the follower is caught up, we are no longer the
// leader for this request's term, the RPC failed, or the inflight window is full.
//
// #200.3/#200.4: before sending, the inflight window is checked. In stateProbe
// the window cap is effectively 1 (at most one batch in flight). In stateReplicate
// the full MaxInflight cap is honored. If the window is full we return false to
// let the replicateTo loop yield until an ack arrives.
func (r *raft) replicateOnce(serverID ServerID) bool {
	r.mu.RLock()
	if r.state != StateLeader {
		r.mu.RUnlock()
		return false
	}

	// #200.3/#200.4: enforce the per-follower flow-control window.
	// inflightWindows is initialized in becomeLeader() for every known follower.
	// If the window is absent (e.g. during test set-up that bypasses becomeLeader)
	// we skip flow-control for this iteration so replication still makes progress.
	win := r.inflightWindows[serverID]
	if win != nil {
		fstate := r.followerStates[serverID]
		// In probe mode treat the window cap as 1 (one batch in-flight at a time).
		windowFull := win.full() || (fstate == stateProbe && win.count() >= 1)
		if windowFull {
			r.mu.RUnlock()
			return false
		}
	}

	nextIdx, ok := r.nextIndex[serverID]
	if !ok {
		nextIdx = r.lastIndex + 1
	}

	// C-A1: if the entry the follower needs (nextIdx) has been compacted away
	// (below the log's FirstIndex), we can no longer replicate it via
	// AppendEntries. Stream the latest snapshot instead so the follower can
	// recover, then resume normal replication from LastIncludedIndex+1.
	firstIdx, _ := r.log.FirstIndex()
	if firstIdx > 0 && nextIdx < firstIdx {
		r.mu.RUnlock()
		// Reset the window on snapshot: we are starting fresh.
		r.mu.Lock()
		if w := r.inflightWindows[serverID]; w != nil {
			w.reset()
		}
		r.followerStates[serverID] = stateProbe
		r.mu.Unlock()
		r.sendSnapshotTo(serverID)
		return true // continue replicating post-snapshot entries
	}

	prevLogIndex := uint64(0)
	prevLogTerm := uint64(0)
	if nextIdx > 1 {
		prevLogIndex = nextIdx - 1
		if entry, err := r.log.Get(prevLogIndex); err == nil {
			prevLogTerm = entry.Term
		} else if firstIdx > 0 && prevLogIndex < firstIdx {
			// C-A1: the prevLog entry itself was compacted; we cannot prove log
			// consistency via AppendEntries. Fall back to a snapshot.
			r.mu.RUnlock()
			r.mu.Lock()
			if w := r.inflightWindows[serverID]; w != nil {
				w.reset()
			}
			r.followerStates[serverID] = stateProbe
			r.mu.Unlock()
			r.sendSnapshotTo(serverID)
			return true
		}
	}

	// #200.1: collect at most maxBatch entries, but also honor MaxSizePerMsg.
	// At least one entry is always included to avoid starvation on oversized entries.
	const maxBatch = 100
	var entries []*LogEntry
	var totalBytes uint64
	maxSize := r.config.MaxSizePerMsg
	for idx := nextIdx; idx <= r.lastIndex && len(entries) < maxBatch; idx++ {
		entry, err := r.log.Get(idx)
		if err != nil {
			break
		}
		entrySize := uint64(len(entry.Data))
		// Always include the first entry (no starvation on large entries).
		if len(entries) > 0 && totalBytes+entrySize > maxSize {
			break
		}
		entries = append(entries, entry)
		totalBytes += entrySize
	}

	// H2: capture the term this request is being sent under so that, after the
	// RPC round-trip, we only act on the response if we are still leader in the
	// SAME term. Otherwise the matchIndex/nextIndex we would update is stale.
	reqTerm := r.term
	req := &AppendEntriesRequest{
		Term:         reqTerm,
		LeaderID:     r.localID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: r.commitIndex,
	}

	// #200.3: record this batch in the inflight window before releasing the lock.
	// For heartbeats (no entries) we skip the window add since there is nothing
	// to ack from a flow-control perspective. win may be nil for tests that
	// bypass becomeLeader(); guard before calling methods on it.
	var lastSentIdx uint64
	if len(entries) > 0 {
		lastSentIdx = entries[len(entries)-1].Index
		if win != nil {
			win.add(lastSentIdx)
		}
	}
	r.mu.RUnlock()

	// M-C1: bounded, cancel-on-shutdown context. H-O2: wrap the send in a span.
	ctx, cancel := r.rpcContext()
	defer cancel()
	spanCtx, span := tracing.SpanAppendEntries(ctx, serverID.String(), reqTerm, len(entries))

	// C-O1: measure AppendEntries RPC latency and count sends by outcome.
	start := r.clock.Now()
	resp, err := r.transport.AppendEntries(spanCtx, serverID, req)
	metrics.RecordAppendEntriesLatency(serverID.String(), r.clock.Now().Sub(start).Seconds())
	if err != nil {
		metrics.RecordAppendEntriesSent(serverID.String(), false)
		tracing.RecordError(span, err)
		span.End()
		r.logger.Debug("AppendEntries failed",
			zap.String("target", serverID.String()),
			zap.Error(err),
		)
		// On transport error reset window so the next attempt starts clean.
		if lastSentIdx > 0 {
			r.mu.Lock()
			if w := r.inflightWindows[serverID]; w != nil {
				w.reset()
			}
			r.followerStates[serverID] = stateProbe
			r.mu.Unlock()
		}
		return false // transport error: stop draining, retry on the next tick
	}
	metrics.RecordAppendEntriesSent(serverID.String(), resp.Success)
	span.End()

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != StateLeader {
		return false
	}

	if resp.Term > r.term {
		r.term = resp.Term
		r.failPendingFutures()
		r.state = StateFollower
		r.votedFor = ""
		r.leaderID = ""
		r.heartbeatAcks = make(map[ServerID]time.Time) // stale acks from old term
		metrics.RecordLeaderID(false)
		r.persistTermAndVotedForLogged()
		return false
	}

	if resp.Term != r.term {
		return false
	}

	// H2: the request was sent under reqTerm; if our term has advanced since
	// then (e.g. we stepped down and were re-elected) the response describes a
	// stale request. matchIndex/nextIndex derived from a stale slice must not be
	// applied. resp.Term == r.term guarantees no gap only if reqTerm also
	// matches — a leader only mutates term via step-down, so verify explicitly.
	if reqTerm != r.term {
		return false
	}

	if resp.Success {
		// Record the ack time for leader-lease / ReadIndex.
		r.heartbeatAcks[serverID] = r.clock.Now()

		if len(entries) > 0 {
			if lastSentIdx > r.matchIndex[serverID] {
				r.matchIndex[serverID] = lastSentIdx
			}
			r.nextIndex[serverID] = r.matchIndex[serverID] + 1

			// #200.3: ack the window up to the acknowledged matchIndex.
			if w := r.inflightWindows[serverID]; w != nil {
				w.ack(r.matchIndex[serverID])
			}

			// #200.4: transition to stateReplicate on first successful ack.
			if r.followerStates[serverID] == stateProbe {
				r.followerStates[serverID] = stateReplicate
			}
		}
		// C-O1: report replication lag (leader lastIndex - follower matchIndex).
		if r.lastIndex >= r.matchIndex[serverID] {
			metrics.RecordReplicationLag(serverID.String(), r.lastIndex-r.matchIndex[serverID])
		}
		r.advanceCommitIndex()
		// M-P1: if the follower is still behind, keep draining immediately.
		return r.nextIndex[serverID] <= r.lastIndex
	} else {
		// Follower rejected. Back off nextIndex, using the conflict term for a
		// fast term-skip when available (M7), otherwise the conflict index hint.
		// #200.3/#200.4: reset window and revert to probe mode on rejection.
		if w := r.inflightWindows[serverID]; w != nil {
			w.reset()
		}
		r.followerStates[serverID] = stateProbe

		next := r.nextIndex[serverID]
		if resp.ConflictTerm > 0 {
			if last := r.lastIndexOfTerm(resp.ConflictTerm); last > 0 {
				next = last + 1
			} else if resp.Index > 0 {
				next = resp.Index
			}
		} else if resp.Index > 0 {
			next = resp.Index
		} else if next > 1 {
			next--
		}
		if next < 1 {
			next = 1
		}
		if next < r.nextIndex[serverID] {
			r.nextIndex[serverID] = next
		} else if r.nextIndex[serverID] > 1 {
			r.nextIndex[serverID]--
		}
		// Retry immediately from the backed-off nextIndex (fast convergence).
		return true
	}
}

// snapshotChunkSize bounds how much snapshot data is carried per InstallSnapshot
// RPC when streaming to a lagging follower (C-A1).
const snapshotChunkSize = 1 << 20 // 1 MiB

// maxReassembledSnapshotBytes bounds the receiver-side reassembly buffer so a
// buggy or malicious (but authenticated) leader cannot exhaust memory by
// streaming an unbounded snapshot. The transports enforce their own caps too;
// this is a defense-in-depth ceiling at the consensus layer.
const maxReassembledSnapshotBytes = 1 << 30 // 1 GiB

// snapshotReassembly accumulates the Offset-ordered chunks of a single
// InstallSnapshot transfer keyed by its LastIncludedIndex. A chunk at Offset 0
// starts (or restarts) a transfer; subsequent chunks must be contiguous.
type snapshotReassembly struct {
	index uint64 // LastIncludedIndex of the in-progress transfer (0 = none)
	buf   []byte
}

func (s *snapshotReassembly) reset() {
	s.index = 0
	s.buf = nil
}

// sendSnapshotTo streams the latest local snapshot to a follower whose required
// log entry has been compacted away (C-A1). It reads the newest snapshot from
// the SnapshotStore and sends it in chunks via Transport.InstallSnapshot,
// honoring Offset/Done. On success it advances the follower's matchIndex /
// nextIndex to LastIncludedIndex+1 so normal AppendEntries replication resumes.
func (r *raft) sendSnapshotTo(serverID ServerID) {
	r.mu.RLock()
	if r.state != StateLeader {
		r.mu.RUnlock()
		return
	}
	reqTerm := r.term
	r.mu.RUnlock()

	// Locate the newest snapshot.
	metas, err := r.snapshot.List()
	if err != nil || len(metas) == 0 {
		r.logger.Warn("cannot send InstallSnapshot: no snapshot available",
			zap.String("target", serverID.String()), zap.Error(err))
		return
	}
	latest := metas[0]
	for _, m := range metas {
		if m != nil && m.Index > latest.Index {
			latest = m
		}
	}
	if latest == nil {
		return
	}

	snap, meta, err := r.snapshot.Open(latest.ID)
	if err != nil {
		r.logger.Warn("cannot open snapshot for InstallSnapshot",
			zap.String("target", serverID.String()), zap.Error(err))
		return
	}
	reader := snap.Reader()
	defer reader.Close()

	lastIncludedIndex := meta.Index
	lastIncludedTerm := meta.Term

	// M-C1 / H-O2: bounded, cancel-on-shutdown context wrapped in a span.
	ctx, cancel := r.rpcContext()
	defer cancel()
	spanCtx, span := tracing.SpanSnapshot(ctx, serverID.String(), lastIncludedIndex)
	defer span.End()

	snapStart := r.clock.Now()

	buf := make([]byte, snapshotChunkSize)
	var offset uint64
	for {
		n, readErr := reader.Read(buf)
		done := false
		if readErr == io.EOF {
			done = true
		} else if readErr != nil {
			tracing.RecordError(span, readErr)
			r.logger.Warn("failed reading snapshot chunk", zap.Error(readErr))
			return
		}

		req := &InstallSnapshotRequest{
			Term:              reqTerm,
			LeaderID:          r.localID,
			LastIncludedIndex: lastIncludedIndex,
			LastIncludedTerm:  lastIncludedTerm,
			Offset:            offset,
			Data:              buf[:n],
			Done:              done,
		}

		resp, sErr := r.transport.InstallSnapshot(spanCtx, serverID, req)
		if sErr != nil {
			tracing.RecordError(span, sErr)
			r.logger.Debug("InstallSnapshot failed",
				zap.String("target", serverID.String()), zap.Error(sErr))
			return
		}

		// Step down if the follower reports a higher term.
		r.mu.Lock()
		if resp.Term > r.term {
			r.term = resp.Term
			r.failPendingFutures()
			r.state = StateFollower
			r.votedFor = ""
			r.leaderID = ""
			r.heartbeatAcks = make(map[ServerID]time.Time)
			metrics.RecordLeaderID(false)
			r.persistTermAndVotedForLogged()
			r.mu.Unlock()
			return
		}
		stillLeader := r.state == StateLeader && r.term == reqTerm
		r.mu.Unlock()
		if !stillLeader {
			return
		}

		offset += uint64(n)
		if done {
			break
		}
	}

	// Success: the follower now holds state through lastIncludedIndex.
	// offset is the total bytes streamed (the snapshot size).
	metrics.RecordInstallSnapshotLatency(r.clock.Now().Sub(snapStart).Seconds())
	metrics.RecordSnapshotSize(int(offset))

	r.mu.Lock()
	if r.state == StateLeader && r.term == reqTerm {
		if lastIncludedIndex > r.matchIndex[serverID] {
			r.matchIndex[serverID] = lastIncludedIndex
		}
		r.nextIndex[serverID] = r.matchIndex[serverID] + 1
		r.heartbeatAcks[serverID] = r.clock.Now()
		if r.lastIndex >= r.matchIndex[serverID] {
			metrics.RecordReplicationLag(serverID.String(), r.lastIndex-r.matchIndex[serverID])
		}
		r.advanceCommitIndex()
	}
	r.mu.Unlock()

	r.logger.Info("sent snapshot to follower",
		zap.String("target", serverID.String()),
		zap.Uint64("last_included_index", lastIncludedIndex),
	)
}

// lastIndexOfTerm returns the highest index in the local log whose entry has the
// given term, or 0 if none. Log terms are monotonic, so the scan stops once it
// passes below the target term. Caller must hold r.mu.
func (r *raft) lastIndexOfTerm(term uint64) uint64 {
	for idx := r.lastIndex; idx >= 1; idx-- {
		e, err := r.log.Get(idx)
		if err != nil {
			return 0
		}
		if e.Term == term {
			return idx
		}
		if e.Term < term {
			return 0
		}
	}
	return 0
}

// advanceCommitIndex checks whether a new index can be committed (replicated to
// a quorum) and advances commitIndex accordingly.  During joint consensus, BOTH
// the old and new configuration quorums must be satisfied before an entry is
// considered committed.
// Must be called with r.mu held (write).
func (r *raft) advanceCommitIndex() {
	for idx := r.commitIndex + 1; idx <= r.lastIndex; idx++ {
		entry, err := r.log.Get(idx)
		if err != nil {
			break
		}

		// The Raft safety rule: only commit entries from the current term.
		if entry.Term != r.term {
			continue
		}

		if r.jointConfig != nil {
			// Joint consensus: require quorum from BOTH old and new configs.
			oldCount := 0
			newCount := 0

			if r.jointConfig.OldConfig.Contains(r.localID) {
				oldCount++
			}
			if r.jointConfig.NewConfig.Contains(r.localID) {
				newCount++
			}

			for id, matchIdx := range r.matchIndex {
				if matchIdx >= idx {
					if r.jointConfig.OldConfig.Contains(id) {
						oldCount++
					}
					if r.jointConfig.NewConfig.Contains(id) {
						newCount++
					}
				}
			}

			if oldCount >= r.jointConfig.OldConfig.QuorumSize() &&
				newCount >= r.jointConfig.NewConfig.QuorumSize() {
				r.commitIndex = idx
				metrics.RecordCommitIndex(r.commitIndex)
			}
			continue
		}

		// Normal (non-joint) quorum.
		quorum := r.configuration.QuorumSize()

		// Count how many nodes (including self, only non-learner voters) have
		// this entry. H-C1: the leader may have removed itself from the
		// configuration; GetServer(localID) is then nil, so guard against a nil
		// dereference and treat an absent self as a non-voter (don't count it).
		count := 0
		if self := r.configuration.GetServer(r.localID); self != nil && !self.Learner {
			count = 1
		}
		for id, matchIdx := range r.matchIndex {
			if matchIdx >= idx {
				s := r.configuration.GetServer(id)
				if s != nil && !s.Learner {
					count++
				}
			}
		}

		if count >= quorum {
			r.commitIndex = idx
			metrics.RecordCommitIndex(r.commitIndex)
		}
	}

	// Persist the (possibly updated) commitIndex so it survives a restart.
	r.persistCommitIndex()

	// Notify the run() goroutine that there may be committed entries to apply.
	select {
	case r.commitNotifyCh <- struct{}{}:
	default:
	}
}

// maxProposalBatch bounds how many proposals are coalesced into a single
// persistLog/WAL.Append (one fsync) per group-commit round (H-P1).
const maxProposalBatch = 256

// handleProposalBatch processes one proposal plus any additional ready
// proposals drained non-blocking from proposalCh, appending the whole batch in a
// SINGLE persistLog call (one WAL.Append / one fsync) before resolving any
// future (H-P1). Ordering is preserved: entries are appended in arrival order
// and the fsync inside persistLog completes before commit/replication is
// triggered, so no future can resolve before its entry is durable.
func (r *raft) handleProposalBatch(first *proposalFuture) {
	batch := make([]*proposalFuture, 0, maxProposalBatch)
	batch = append(batch, first)

	// Non-blocking drain of additional ready proposals.
	for len(batch) < maxProposalBatch {
		select {
		case p := <-r.proposalCh:
			batch = append(batch, p)
		default:
			r.handleProposal(batch)
			return
		}
	}
	r.handleProposal(batch)
}

// handleProposal appends a batch of client proposals in one WAL append (H-P1).
func (r *raft) handleProposal(batch []*proposalFuture) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != StateLeader {
		for _, p := range batch {
			p.future.respond(ErrNotLeader, 0, 0, nil)
		}
		return
	}

	// M1: while a leadership transfer is in progress, reject new proposals so we
	// do not extend the log past the point the target is catching up to. The
	// caller should retry against the new leader.
	if r.transferring() {
		for _, p := range batch {
			p.future.respond(ErrLeadershipLost, 0, 0, nil)
		}
		return
	}

	// M-R5: bound outstanding pending futures. If the apply loop has stalled and
	// too many futures are already in flight, reject the overflow with a busy
	// error rather than let pendingFutures grow without limit.
	limit := r.pendingFutureCap()
	accepted := batch
	if len(r.pendingFutures)+len(batch) > limit {
		room := limit - len(r.pendingFutures)
		if room < 0 {
			room = 0
		}
		if room > len(batch) {
			room = len(batch)
		}
		for _, p := range batch[room:] {
			p.future.respond(ErrNodeBusy, 0, 0, nil)
		}
		accepted = batch[:room]
	}
	if len(accepted) == 0 {
		return
	}

	entries := make([]*LogEntry, 0, len(accepted))
	nextIdx := r.lastIndex + 1
	for i, p := range accepted {
		entries = append(entries, &LogEntry{
			Term:  r.term,
			Index: nextIdx + uint64(i),
			Type:  EntryNormal,
			Data:  p.data,
		})
	}

	// H-P1: one persistLog => one WAL.Append => one fsync for the whole batch.
	if err := r.persistLog(entries); err != nil {
		// H-R2: a persistent storage-write failure makes this node a silent
		// zombie if it keeps running. Fail the batch and halt the node.
		for _, p := range accepted {
			p.future.respond(err, 0, 0, nil)
		}
		r.stopFatal(fmt.Errorf("persist proposal batch: %w", err))
		return
	}

	for i, p := range accepted {
		r.pendingFutures[entries[i].Index] = p.future
		// #213: start a write-path span (propose→applied) as a child of the
		// caller's trace context, ended when the entry is applied. This makes a
		// client write's trace span the internal WAL-append→commit→apply latency.
		if p.ctx != nil {
			_, span := tracing.StartSpan(p.ctx, "raft", "raft.commit_apply")
			p.future.span = span
		}
	}

	lastEntryIndex := entries[len(entries)-1].Index

	// Single-node cluster: commit immediately.
	if r.configuration.QuorumSize() == 1 {
		r.commitIndex = lastEntryIndex
		metrics.RecordCommitIndex(r.commitIndex)
		r.persistCommitIndex()
		return
	}

	// Trigger replication to all non-local servers (including learners and
	// any extra servers from the joint configuration when applicable).
	allServers := r.configuration.Servers
	if r.jointConfig != nil {
		allServers = r.jointConfig.AllServers()
	}
	for _, server := range allServers {
		if server.ID != r.localID {
			r.spawnReplication(server.ID)
		}
	}
}

// pendingFutureCap returns the maximum number of outstanding pending futures
// (M-R5). Derived from MaxInflight with a sane floor.
func (r *raft) pendingFutureCap() int {
	c := r.config.MaxInflight * 16
	if c < 1024 {
		c = 1024
	}
	return c
}

// applyCommitted applies all log entries between applyIndex and commitIndex to
// the FSM, resolving any waiting ApplyFutures.
func (r *raft) applyCommitted() {
	r.mu.RLock()
	commitIndex := r.commitIndex
	applyIndex := r.applyIndex
	fatal := r.fatalErr != nil
	r.mu.RUnlock()

	// H-R3: once the node has halted on an FSM apply panic (or fatal storage
	// error), it must NOT keep applying committed entries — doing so would
	// silently advance applyIndex past the entry it could not apply and diverge
	// this replica. The drain path also calls applyCommitted, so this guard is
	// what keeps a halted node from re-applying during shutdown.
	if fatal {
		return
	}

	for applyIndex < commitIndex {
		nextApply := applyIndex + 1

		entry, err := r.log.Get(nextApply)
		if err != nil {
			r.logger.Error("failed to get entry for FSM apply",
				zap.Uint64("index", nextApply),
				zap.Error(err),
			)
			break
		}

		var result []byte
		var applyErr error
		var fsmPanicked bool

		switch entry.Type {
		case EntryNormal:
			if len(entry.Data) > 0 {
				func() {
					defer func() {
						if rec := recover(); rec != nil {
							r.logger.Error("FSM Apply panicked",
								zap.Uint64("index", entry.Index),
								zap.Any("panic", rec),
							)
							applyErr = fmt.Errorf("FSM panic at index %d: %v", entry.Index, rec)
							fsmPanicked = true
						}
					}()
					// C-O1: record FSM apply latency around the Apply call.
					start := r.clock.Now()
					result, applyErr = r.fsm.Apply(entry.Data)
					metrics.RecordFSMApplyLatency(r.clock.Now().Sub(start).Seconds())
				}()
			}
		case EntryConfiguration:
			r.applyConfigurationEntry(entry)
		}

		// H-R3: an FSM Apply panic means this replica cannot safely apply a
		// committed entry. Advancing applyIndex past it would silently diverge
		// this replica's state machine from the rest of the cluster. Do NOT
		// advance applyIndex; halt the node so the divergence is loud, not silent.
		if fsmPanicked {
			r.mu.Lock()
			if f, ok := r.pendingFutures[entry.Index]; ok {
				f.respond(applyErr, entry.Index, entry.Term, nil)
				delete(r.pendingFutures, entry.Index)
			}
			r.stopFatal(fmt.Errorf("FSM apply panic at index %d halted node: %w", entry.Index, applyErr))
			r.mu.Unlock()
			return
		}

		r.mu.Lock()
		r.applyIndex = nextApply
		applyIndex = nextApply
		metrics.RecordAppliedIndex(r.applyIndex)
		metrics.RecordApplyLag(r.commitIndex, r.applyIndex) // M-O1

		if f, ok := r.pendingFutures[entry.Index]; ok {
			if !f.created.IsZero() {
				metrics.RecordProposalCommitLatency(r.clock.Now().Sub(f.created).Seconds())
			}
			f.respond(applyErr, entry.Index, entry.Term, result) // ends the #213 span
			delete(r.pendingFutures, entry.Index)
		}
		r.mu.Unlock()

		// L9: wake any WaitApplied callers now that applyIndex advanced. Done
		// after releasing r.mu so we never hold r.mu and applyMu together.
		r.notifyApplied(nextApply)
	}
}

// notifyApplied records that the FSM has applied through idx and wakes any
// WaitApplied callers. It is safe to call with a stale/lower idx (it only ever
// raises appliedForWait). Must NOT be called with r.mu or applyMu held.
func (r *raft) notifyApplied(idx uint64) {
	r.applyMu.Lock()
	if idx > r.appliedForWait {
		r.appliedForWait = idx
	}
	// L2: broadcast by closing the current wait channel and installing a fresh
	// one, so WaitApplied callers wake without a per-call watcher goroutine.
	close(r.applyWaitCh)
	r.applyWaitCh = make(chan struct{})
	r.applyMu.Unlock()
}

// applyConfigurationEntry processes a committed configuration change entry.
// Must NOT be called with r.mu held.
func (r *raft) applyConfigurationEntry(entry *LogEntry) {
	if len(entry.Data) == 0 {
		return
	}

	// Peek at the change type byte.
	changeType := ConfigurationChangeType(entry.Data[0])

	// Handle joint-consensus special cases before the standard decode path.
	if changeType == ChangeJoint {
		newConfig, err := decodeJointConfigChange(entry.Data)
		if err != nil {
			r.logger.Error("failed to decode joint config change", zap.Error(err))
			return
		}

		r.mu.Lock()
		oldConfig := r.configuration.Copy()
		jc := &JointConfiguration{OldConfig: oldConfig, NewConfig: newConfig}
		r.jointConfig = jc

		r.logger.Info("entered joint consensus",
			zap.Int("old_voters", oldConfig.VoteCount()),
			zap.Int("new_voters", newConfig.VoteCount()),
		)

		isLeader := r.state == StateLeader
		// Ensure all servers in the joint config have replication trackers.
		if isLeader {
			for _, server := range jc.AllServers() {
				if server.ID != r.localID {
					if _, ok := r.nextIndex[server.ID]; !ok {
						r.nextIndex[server.ID] = r.lastIndex + 1
						r.matchIndex[server.ID] = 0
						r.inflightWindows[server.ID] = newInflightWindow(r.config.MaxInflight)
						r.followerStates[server.ID] = stateProbe
					}
				}
			}
		}
		r.mu.Unlock()

		// If we are leader, append the ChangeCommitJoint entry to finalize the
		// transition once the joint entry is committed.
		if isLeader {
			r.mu.Lock()
			commitEntry := &LogEntry{
				Term:  r.term,
				Index: r.lastIndex + 1,
				Type:  EntryConfiguration,
				Data:  encodeCommitJointChange(),
			}
			if err := r.persistLog([]*LogEntry{commitEntry}); err != nil {
				r.logger.Error("failed to append ChangeCommitJoint entry", zap.Error(err))
			}
			r.mu.Unlock()
		}
		return
	}

	if changeType == ChangeCommitJoint {
		r.mu.Lock()
		defer r.mu.Unlock()

		if r.jointConfig == nil {
			r.logger.Warn("received ChangeCommitJoint but not in joint mode")
			return
		}

		r.configuration = r.jointConfig.NewConfig
		r.jointConfig = nil

		r.logger.Info("committed joint consensus, new config active",
			zap.Int("voters", r.configuration.VoteCount()),
		)

		// Clean up trackers for servers that are no longer in the config.
		if r.state == StateLeader {
			for id := range r.nextIndex {
				if !r.configuration.Contains(id) {
					delete(r.nextIndex, id)
					delete(r.matchIndex, id)
				}
			}
		}
		// H-C1: if this leader is no longer a voter in the newly-committed config
		// (e.g. it removed itself), it must step down and stop proposing.
		r.stepDownIfNotVoter()
		return
	}

	// Standard single-step change.
	change := decodeConfigurationChange(entry.Data)

	r.mu.Lock()
	defer r.mu.Unlock()

	switch change.ChangeType {
	case ChangeAddNode:
		r.configuration = r.configuration.AddServer(change.ServerID, change.ServerAddr, false)
	case ChangeRemoveNode:
		r.configuration = r.configuration.RemoveServer(change.ServerID)
	case ChangeAddLearner:
		r.configuration = r.configuration.AddServer(change.ServerID, change.ServerAddr, true)
	case ChangePromoteLearner:
		for i, s := range r.configuration.Servers {
			if s.ID == change.ServerID {
				r.configuration.Servers[i].Learner = false
				break
			}
		}
	}

	r.logger.Info("AUDIT membership change applied",
		zap.String("node_id", string(r.localID)),
		zap.Uint64("term", r.term),
		zap.Uint64("index", entry.Index),
		zap.String("change_type", change.ChangeType.String()),
		zap.String("server_id", string(change.ServerID)),
		zap.String("server_addr", string(change.ServerAddr)),
	)

	// Update replication trackers if we are leader and got a new server.
	if r.state == StateLeader {
		for _, server := range r.configuration.Servers {
			if server.ID != r.localID {
				if _, ok := r.nextIndex[server.ID]; !ok {
					r.nextIndex[server.ID] = r.lastIndex + 1
					r.matchIndex[server.ID] = 0
					r.inflightWindows[server.ID] = newInflightWindow(r.config.MaxInflight)
					r.followerStates[server.ID] = stateProbe
				}
			}
		}
	}

	// H-C1: a single-step change (e.g. RemoveNode of ourselves) may leave this
	// leader as a non-voter. Step down if so.
	r.stepDownIfNotVoter()
}

// stepDownIfNotVoter makes the node relinquish leadership when it is no longer a
// voting member of the committed configuration (H-C1). A leader that has removed
// itself must stop proposing and stepping on quorum decisions. Caller must hold
// r.mu (write).
func (r *raft) stepDownIfNotVoter() {
	if r.state != StateLeader {
		return
	}
	if r.configuration.IsVoter(r.localID) {
		return
	}
	r.logger.Info("stepping down: local node is no longer a voter in the committed configuration",
		zap.String("id", r.localID.String()),
	)
	// Fail any uncommitted proposals; a non-voter leader cannot commit them.
	r.failPendingFutures()
	r.finishTransfer(ErrLeadershipLost)
	r.state = StateFollower
	r.leaderID = ""
	r.votedFor = ""
	r.heartbeatAcks = make(map[ServerID]time.Time)
	r.electionTicks = r.randomElectionTickCount()
	metrics.RecordLeaderID(false)
}

// failPendingFutures cancels pending Apply futures for log entries that have
// NOT yet been committed (index > commitIndex).  It must be called with r.mu
// held (write) whenever this node loses leadership.
//
// Entries at or below commitIndex are intentionally left alone: they will be
// resolved by the next applyCommitted() call (either on this node as a follower
// or on the new leader), so canceling them would produce spurious
// ErrLeadershipLost errors for already-committed writes.
func (r *raft) failPendingFutures() {
	for idx, f := range r.pendingFutures {
		if idx > r.commitIndex {
			f.respond(ErrLeadershipLost, 0, 0, nil) // ends the #213 span
			delete(r.pendingFutures, idx)
		}
	}
}

// drainFuturesOnShutdown resolves all pending Apply futures when the node is
// shutting down (M2). Futures for indices that have already been committed must
// NOT be failed with ErrNotStarted — those writes are durable and will be
// applied by this node (as follower) or the new leader; failing them would tell
// a client its committed write was lost. They are resolved as success. Only the
// genuinely uncommitted futures (index > commitIndex) are failed.
func (r *raft) drainFuturesOnShutdown() {
	// Flush any committed-but-unapplied entries to the FSM first so committed
	// futures observe a consistent applied state (and are resolved with their
	// real results by applyCommitted itself).
	r.applyCommitted()

	r.mu.Lock()
	defer r.mu.Unlock()
	commitIndex := r.commitIndex
	for idx, f := range r.pendingFutures {
		if idx <= commitIndex {
			var term uint64
			if e, err := r.log.Get(idx); err == nil {
				term = e.Term
			}
			f.respond(nil, idx, term, nil)
		} else {
			f.respond(ErrNotStarted, 0, 0, nil)
		}
	}
	r.pendingFutures = make(map[uint64]*ApplyFuture)
}

// ReadIndex returns the committed log index up to which a linearizable read is
// safe to serve, without writing to the Raft log.
//
// The implementation is heartbeat-confirmed (Raft §6.4), NOT a clock lease: it
// captures the current commit index as the read index, then confirms it is
// still the leader by requiring a quorum of voters to acknowledge a heartbeat
// that the leader processes AFTER this call began. The reference instant
// (`start`) and the recorded ack times are BOTH taken from the leader's own
// monotonic clock in the AppendEntries response handler, so the confirmation
// makes no assumption about inter-node clock synchronization — it purely proves
// "a majority still recognized me as leader after I started this read", which is
// exactly what linearizability requires.
//
// Callers should wait until AppliedIndex() >= the returned value before reading
// from the FSM (see Server.waitApplied in the HTTP layer).
func (r *raft) ReadIndex(ctx context.Context) (uint64, error) {
	r.mu.RLock()
	state := r.state
	readIndex := r.commitIndex
	quorum := r.configuration.QuorumSize()
	r.mu.RUnlock()

	if state != StateLeader {
		return 0, ErrNotLeader
	}

	// Single-node cluster: we are always the quorum — return immediately.
	if quorum == 1 {
		return readIndex, nil
	}

	// Reference instant on the leader's monotonic clock; only acks recorded
	// strictly after this point count as confirmation for this read.
	start := r.clock.Now()

	// Nudge an immediate heartbeat round so confirmation arrives within ~one
	// heartbeat interval rather than waiting for the next periodic tick.
	r.triggerHeartbeat()

	// L1: reuse a single Timer across retry iterations instead of allocating a
	// fresh time.After channel (and leaking its underlying timer) each loop.
	timer := time.NewTimer(r.heartbeatInterval() / 2)
	defer timer.Stop()

	for {
		r.mu.RLock()
		if r.state != StateLeader {
			r.mu.RUnlock()
			return 0, ErrNotLeader
		}
		// Count self plus voters that acknowledged a heartbeat AFTER `start`.
		count := 1 // self
		for id, ackTime := range r.heartbeatAcks {
			if ackTime.After(start) && r.configuration.IsVoter(id) {
				count++
			}
		}
		r.mu.RUnlock()

		if count >= quorum {
			// Leadership confirmed as of `start`; readIndex (the commit index at
			// that instant) is committed and safe to serve once applied.
			return readIndex, nil
		}

		// Not yet confirmed — wait for the next heartbeat round's acks. Bounded
		// only by the caller's context so a generous deadline can span several
		// heartbeat rounds during transient follower slowness.
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-r.stopCh:
			return 0, ErrNotStarted
		case <-timer.C:
			r.triggerHeartbeat()
			timer.Reset(r.heartbeatInterval() / 2)
		}
	}
}

// triggerHeartbeat asks the run loop to send an immediate heartbeat round. The
// signal is coalescing and never blocks (M4).
func (r *raft) triggerHeartbeat() {
	select {
	case r.heartbeatTrigger <- struct{}{}:
	default:
	}
}

func (r *raft) Apply(ctx context.Context, data []byte) ([]byte, error) {
	r.mu.RLock()
	state := r.state
	r.mu.RUnlock()

	if state != StateLeader {
		return nil, ErrNotLeader
	}

	future := &ApplyFuture{
		ch:      make(chan struct{}),
		data:    data,
		created: r.clock.Now(),
	}

	proposal := &proposalFuture{
		data:   data,
		future: future,
		ctx:    ctx,
	}

	select {
	case r.proposalCh <- proposal:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.stopCh:
		return nil, ErrNotStarted
	}

	// Wait for the future to be resolved while still honoring the caller's
	// context and a node shutdown.  This ensures Apply() never hangs
	// indefinitely if the leader steps down after the proposal was sent.
	select {
	case <-future.ch:
		if err := future.Error(); err != nil {
			return nil, err
		}
		return future.Result(), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-r.stopCh:
		return nil, ErrNotStarted
	}
}

func (r *raft) State() RaftState {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.state
}

func (r *raft) Leader() ServerID {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.leaderID
}

func (r *raft) Term() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.term
}

func (r *raft) LastIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastIndex
}

func (r *raft) LastTerm() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastTerm
}

func (r *raft) AppliedIndex() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.applyIndex
}

// WaitApplied blocks until the FSM has applied through idx (applyIndex >= idx)
// or ctx is done, in which case it returns ctx.Err(). It does not busy-poll: it
// waits on a condition variable that is broadcast whenever applyIndex advances
// (L9). If idx has already been applied it returns immediately.
func (r *raft) WaitApplied(ctx context.Context, idx uint64) error {
	// Fast path: already applied. Read the authoritative applyIndex under r.mu.
	if r.AppliedIndex() >= idx {
		return nil
	}

	// L2: block on a broadcast channel + ctx.Done() directly — no per-call
	// watcher goroutine. We capture the current wait channel under applyMu AFTER
	// re-checking the predicate; notifyApplied closes exactly that channel under
	// the same lock, so a wake that races the select cannot be missed.
	for {
		r.applyMu.Lock()
		if r.appliedForWait >= idx {
			r.applyMu.Unlock()
			return nil
		}
		ch := r.applyWaitCh
		r.applyMu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
			// applyIndex advanced (or a broadcast fired); re-check the predicate.
		}
	}
}

func (r *raft) Configuration() Configuration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.configuration.Copy()
}

func (r *raft) Demote(ctx context.Context, target ServerID) error {
	return r.RemoveServer(ctx, target)
}

func (r *raft) RemoveServer(ctx context.Context, target ServerID) error {
	return r.removeServer(ctx, target)
}

func (r *raft) removeServer(ctx context.Context, target ServerID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != StateLeader {
		return ErrNotLeader
	}
	if !r.configuration.Contains(target) {
		return ErrConfiguration
	}
	if r.configChangePending() {
		return ErrConfigChangeInProgress
	}

	// Build the new configuration that would result from removing target.
	newConfig := r.configuration.RemoveServer(target)

	data, err := encodeJointConfigChange(newConfig)
	if err != nil {
		return err
	}

	entry := &LogEntry{
		Term:  r.term,
		Index: r.lastIndex + 1,
		Type:  EntryConfiguration,
		Data:  data,
	}

	if err := r.persistLog([]*LogEntry{entry}); err != nil {
		return err
	}
	r.pendingConfigIndex = entry.Index

	r.logger.Info("removing server via joint consensus",
		zap.String("server_id", string(target)),
	)

	return nil
}

func (r *raft) ReplaceServer(ctx context.Context, oldID, newID ServerID, newAddr ServerAddress) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != StateLeader {
		return ErrNotLeader
	}
	if r.configChangePending() {
		return ErrConfigChangeInProgress
	}

	// Compute the target config: remove oldID, add newID.
	// RemoveServer and AddServer return Configuration values; take address to
	// chain the call.
	intermediate := r.configuration.RemoveServer(oldID)
	newConfig := (&intermediate).AddServer(newID, newAddr, false)

	// Encode as a joint entry directly to the new config
	data, err := encodeJointConfigChange(newConfig)
	if err != nil {
		return err
	}

	entry := &LogEntry{
		Term:  r.term,
		Index: r.lastIndex + 1,
		Type:  EntryConfiguration,
		Data:  data,
	}

	if err := r.persistLog([]*LogEntry{entry}); err != nil {
		return err
	}
	r.pendingConfigIndex = entry.Index

	r.logger.Info("replacing server",
		zap.String("old_server_id", string(oldID)),
		zap.String("new_server_id", string(newID)),
		zap.String("new_address", string(newAddr)),
	)

	return nil
}

func (r *raft) AddServer(ctx context.Context, target ServerID, addr ServerAddress) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != StateLeader {
		return ErrNotLeader
	}
	if r.configChangePending() {
		return ErrConfigChangeInProgress
	}

	// Build the new configuration that would result from adding the server.
	newConfig := r.configuration.AddServer(target, addr, false)

	data, err := encodeJointConfigChange(newConfig)
	if err != nil {
		return err
	}

	entry := &LogEntry{
		Term:  r.term,
		Index: r.lastIndex + 1,
		Type:  EntryConfiguration,
		Data:  data,
	}

	if err := r.persistLog([]*LogEntry{entry}); err != nil {
		return err
	}
	r.pendingConfigIndex = entry.Index

	r.logger.Info("adding server via joint consensus",
		zap.String("server_id", string(target)),
		zap.String("address", string(addr)),
	)

	return nil
}

func (r *raft) createConfigurationEntry(changeType ConfigurationChangeType, serverID ServerID, addr ServerAddress, learner bool) *LogEntry {
	change := ConfigurationChange{
		ChangeType: changeType,
		ServerID:   serverID,
		ServerAddr: addr,
		Index:      r.lastIndex + 1,
		Term:       r.term,
	}

	data, _ := encodeConfigurationChange(change)

	return &LogEntry{
		Term:  r.term,
		Index: r.lastIndex + 1,
		Type:  EntryConfiguration,
		Data:  data,
	}
}

func encodeConfigurationChange(change ConfigurationChange) ([]byte, error) {
	// Format: [1 byte type][n bytes serverID][1 byte null separator][m bytes address]
	sep := []byte{0}
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(change.ChangeType))
	buf.WriteString(string(change.ServerID))
	buf.Write(sep)
	buf.WriteString(string(change.ServerAddr))
	return buf.Bytes(), nil
}

// encodeJointConfigChange encodes a joint-consensus configuration change entry.
// Format: [1 byte ChangeJoint][JSON of newConfig]
func encodeJointConfigChange(newConfig Configuration) ([]byte, error) {
	jsonBytes, err := json.Marshal(newConfig)
	if err != nil {
		return nil, err
	}
	buf := &bytes.Buffer{}
	buf.WriteByte(byte(ChangeJoint))
	buf.Write(jsonBytes)
	return buf.Bytes(), nil
}

// encodeCommitJointChange encodes a commit-joint entry (no payload needed).
// Format: [1 byte ChangeCommitJoint]
func encodeCommitJointChange() []byte {
	return []byte{byte(ChangeCommitJoint)}
}

// decodeJointConfigChange decodes a joint-consensus configuration change entry.
func decodeJointConfigChange(data []byte) (Configuration, error) {
	if len(data) < 2 {
		return Configuration{}, nil
	}
	var cfg Configuration
	if err := json.Unmarshal(data[1:], &cfg); err != nil {
		return Configuration{}, err
	}
	return cfg, nil
}

func decodeConfigurationChange(data []byte) ConfigurationChange {
	if len(data) < 2 {
		return ConfigurationChange{}
	}

	changeType := ConfigurationChangeType(data[0])
	rest := data[1:]

	// Find the null separator between server ID and address.
	sepIdx := -1
	for i, b := range rest {
		if b == 0 {
			sepIdx = i
			break
		}
	}

	var serverID ServerID
	var serverAddr ServerAddress
	if sepIdx >= 0 {
		serverID = ServerID(rest[:sepIdx])
		serverAddr = ServerAddress(rest[sepIdx+1:])
	} else {
		serverID = ServerID(rest)
	}

	return ConfigurationChange{
		ChangeType: changeType,
		ServerID:   serverID,
		ServerAddr: serverAddr,
	}
}

func (r *raft) AddLearner(ctx context.Context, target ServerID, addr ServerAddress) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != StateLeader {
		return ErrNotLeader
	}
	if r.configChangePending() {
		return ErrConfigChangeInProgress
	}

	entry := r.createConfigurationEntry(ChangeAddLearner, target, addr, true)
	entry.Index = r.lastIndex + 1

	if err := r.persistLog([]*LogEntry{entry}); err != nil {
		return err
	}
	r.pendingConfigIndex = entry.Index

	r.logger.Info("learner added",
		zap.String("server_id", string(target)),
	)

	return nil
}

func (r *raft) PromoteLearner(ctx context.Context, target ServerID) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.state != StateLeader {
		return ErrNotLeader
	}

	// Safety check: ensure the learner is sufficiently caught up before
	// promoting it to a full voter.  This prevents a stale learner from
	// disrupting quorum.
	trailingLogs := r.config.TrailingLogs
	if trailingLogs == 0 {
		trailingLogs = 1000 // sensible default
	}
	matchIdx, ok := r.matchIndex[target]
	if !ok || (r.lastIndex > trailingLogs && matchIdx < r.lastIndex-trailingLogs) {
		return ErrLearnerNotReady
	}

	if r.configChangePending() {
		return ErrConfigChangeInProgress
	}

	entry := r.createConfigurationEntry(ChangePromoteLearner, target, "", false)
	entry.Index = r.lastIndex + 1

	if err := r.persistLog([]*LogEntry{entry}); err != nil {
		return err
	}
	r.pendingConfigIndex = entry.Index

	r.logger.Info("learner promoted",
		zap.String("server_id", string(target)),
	)

	return nil
}

func (r *raft) RequestLeadership(ctx context.Context) error {
	r.mu.Lock()

	if r.state != StateLeader {
		r.mu.Unlock()
		return ErrNotLeader
	}

	lt := &leadershipTransfer{
		target:   "",
		complete: make(chan struct{}),
	}
	r.leadershipTransfer = lt
	r.mu.Unlock()

	// Wait for the transfer to complete or for the context to expire.
	select {
	case <-lt.complete:
		return lt.err
	case <-ctx.Done():
		r.mu.Lock()
		// Cancel the pending transfer if still set.
		if r.leadershipTransfer == lt {
			r.leadershipTransfer = nil
		}
		r.mu.Unlock()
		return ctx.Err()
	case <-r.stopCh:
		return ErrNotStarted
	}
}

// HandleTimeoutNowRPC forces the receiving node to start an election
// immediately by resetting its election timer to zero.
func (r *raft) HandleTimeoutNowRPC() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.logger.Info("received TimeoutNow, starting immediate election",
		zap.String("id", r.localID.String()),
	)

	// Start an immediate real election marked as a leadership transfer: skip
	// pre-vote (the target is already caught up) and set LeaderTransfer so peers
	// bypass the disruptive-server rejection even though they just heard from the
	// (transferring) leader.
	r.electionTicks = r.randomElectionTickCount()
	r.startElectionWith(true, true)
}

func (r *raft) Snapshot() error {
	future := &reqSnapshotFuture{
		done: make(chan struct{}),
	}

	select {
	case r.userSnapshotCh <- future:
	case <-r.stopCh:
		return ErrNotStarted
	}

	<-future.done
	return future.err
}

func (r *raft) triggerSnapshot() {
	r.mu.RLock()
	threshold := r.config.SnapshotThreshold
	// M3: the snapshot is taken at applyIndex (see processSnapshot), so gate on
	// progress since the last snapshot measured against applyIndex, and set
	// snapshotIndex to the index the snapshot will actually cover — not
	// lastIndex, which may be far ahead of what has been applied.
	applyIndex := r.applyIndex
	sinceLastSnap := applyIndex - r.snapshotIndex
	r.mu.RUnlock()

	if threshold == 0 || applyIndex <= r.snapshotIndex || sinceLastSnap < threshold {
		return
	}

	// M3: do NOT set snapshotIndex here to lastIndex. processSnapshot captures
	// applyIndex and the FSM snapshot atomically and updates snapshotIndex to the
	// real index the snapshot covers.
	go func() {
		if err := r.Snapshot(); err != nil {
			r.logger.Warn("background snapshot failed", zap.Error(err))
		}
	}()
}

// processSnapshot handles a user-initiated snapshot request from the run() goroutine.
func (r *raft) processSnapshot(req *reqSnapshotFuture) {
	// M3: capture applyIndex and the FSM snapshot together under the read lock
	// so the recorded snapshot index matches the FSM state that is snapshotted.
	// The FSM snapshot must be taken while the lock is held to prevent applied
	// entries from advancing between reading index and snapshotting.
	r.mu.RLock()
	index := r.applyIndex
	term := r.lastTerm
	config := r.configuration.Copy()
	snap, err := r.fsm.Snapshot()
	r.mu.RUnlock()

	if err != nil {
		req.err = err
		close(req.done)
		return
	}

	sink, err := r.snapshot.Create(SnapshotVersionMax, index, term, config)
	if err != nil {
		req.err = err
		close(req.done)
		return
	}

	reader := snap.Reader()
	defer reader.Close()

	if _, err := io.Copy(sink, reader); err != nil {
		_ = sink.Cancel() // best-effort cleanup; original copy error is what we report
		req.err = err
		close(req.done)
		return
	}

	if err := sink.Close(); err != nil {
		req.err = err
		close(req.done)
		return
	}

	// Compact the WAL up to the snapshot index.
	if compactor, ok := r.log.(interface{ Compact(uint64) error }); ok {
		if err := compactor.Compact(index); err != nil {
			r.logger.Warn("failed to compact WAL after snapshot", zap.Error(err))
		}
	}

	// M3: record the real snapshot index now that the snapshot is durable, so a
	// subsequent triggerSnapshot measures progress from the actual covered index.
	r.mu.Lock()
	if index > r.snapshotIndex {
		r.snapshotIndex = index
	}
	r.mu.Unlock()

	metrics.RecordSnapshot()

	r.logger.Info("snapshot taken",
		zap.Uint64("index", index),
		zap.Uint64("term", term),
	)

	close(req.done)

	r.logger.Info("AUDIT snapshot created",
		zap.String("node_id", string(r.localID)),
		zap.Uint64("term", term),
		zap.Uint64("snapshot_index", index),
		zap.Uint64("snapshot_term", term),
	)
}

// processRestore handles a user-initiated restore request from the run() goroutine.
func (r *raft) processRestore(req *restoreFuture) {
	if err := r.fsm.Restore(req.snapshot); err != nil {
		req.future.respond(err, 0, 0, nil)
		return
	}

	r.mu.Lock()
	r.applyIndex = req.index
	r.commitIndex = req.index
	r.lastIndex = req.index
	r.lastTerm = req.term
	r.persistCommitIndex()
	r.mu.Unlock()

	// L9: restore jumps applyIndex forward; wake WaitApplied callers.
	r.notifyApplied(req.index)

	req.future.respond(nil, req.index, req.term, nil)
}

func (r *raft) Restore(ctx context.Context, reader io.Reader) error {
	future := &restoreFuture{
		snapshot: reader,
		future:   &ApplyFuture{ch: make(chan struct{})},
	}

	select {
	case r.restoreCh <- future:
	case <-ctx.Done():
		return ctx.Err()
	case <-r.stopCh:
		return ErrNotStarted
	}

	return future.future.Await()
}

func (r *raft) loadTerm() error {
	term, err := r.stable.Get([]byte(KeyTerm))
	if err != nil {
		return err
	}
	if term != nil {
		r.term = bytesToUint64(term)
	}

	votedFor, err := r.stable.Get([]byte(KeyVotedFor))
	if err != nil {
		return err
	}
	if votedFor != nil {
		r.votedFor = ServerID(votedFor)
	}

	return nil
}

func (r *raft) loadConfiguration() error {
	if len(r.config.InitialConfiguration.Servers) > 0 {
		r.configuration = r.config.InitialConfiguration
	}
	return nil
}

func (r *raft) loadLastIndex() error {
	lastIndex, err := r.log.LastIndex()
	if err != nil {
		return err
	}
	r.lastIndex = lastIndex

	if lastIndex > 0 {
		entry, err := r.log.Get(lastIndex)
		if err != nil {
			return err
		}
		r.lastTerm = entry.Term
	}

	return nil
}

// loadSnapshotState raises lastIndex/lastTerm/applyIndex/commitIndex to reflect
// the latest on-disk snapshot. It only ever moves state forward, so it is safe
// to call after loadLastIndex/loadCommitIndex. This is what lets a node that
// compacted its log recover its true position on restart (C5).
func (r *raft) loadSnapshotState() error {
	snaps, err := r.snapshot.List()
	if err != nil {
		return err
	}
	if len(snaps) == 0 {
		return nil
	}
	latest := snaps[0]
	for _, s := range snaps {
		if s != nil && s.Index > latest.Index {
			latest = s
		}
	}
	if latest == nil {
		return nil
	}
	if latest.Index > r.lastIndex {
		r.lastIndex = latest.Index
		r.lastTerm = latest.Term
	}
	if latest.Index > r.applyIndex {
		r.applyIndex = latest.Index
	}
	if latest.Index > r.commitIndex {
		r.commitIndex = latest.Index
	}
	return nil
}

func (r *raft) persistTermAndVotedFor() error {
	if err := r.stable.Set([]byte(KeyTerm), uint64ToBytes(r.term)); err != nil {
		return err
	}
	if err := r.stable.Set([]byte(KeyVotedFor), []byte(r.votedFor)); err != nil {
		return err
	}
	return r.stable.Sync()
}

// persistTermAndVotedForLogged persists term/votedFor on a best-effort basis and
// logs (rather than propagates) any error. It is used on step-down and term-bump
// paths where votedFor is being cleared: the caller cannot meaningfully act on a
// persistence failure, and Raft's term-comparison recovers correctness on restart
// even if the write was lost. The safety-critical vote-*grant* path persists via
// persistTermAndVotedFor and checks the error directly.
func (r *raft) persistTermAndVotedForLogged() {
	if err := r.persistTermAndVotedFor(); err != nil {
		r.logger.Error("failed to persist term/votedFor", zap.Error(err))
	}
}

// persistCommitIndex requests that the current commitIndex be flushed to the
// stable store so it survives restarts. It is called whenever commitIndex
// advances, typically while r.mu is held.
//
// H1: it must NOT perform the disk write inline, because that would hold r.mu
// across a stable-store write/sync. Instead it fires a coalescing signal and
// the run() goroutine performs the actual write via flushCommitIndex() with the
// lock released. The write is best-effort: a failure is logged but not fatal —
// the node can always re-derive its commit point from the leader's
// AppendEntries LeaderCommit field.
func (r *raft) persistCommitIndex() {
	select {
	case r.persistCommitCh <- struct{}{}:
	default:
	}
}

// flushCommitIndex writes the current commitIndex to the stable store. It reads
// commitIndex under a short read lock and then performs the write with the lock
// released, so the disk write never happens while r.mu is held (H1).
func (r *raft) flushCommitIndex() {
	r.mu.RLock()
	commitIndex := r.commitIndex
	r.mu.RUnlock()

	if err := r.stable.Set([]byte(KeyCommitIndex), uint64ToBytes(commitIndex)); err != nil {
		r.logger.Warn("failed to persist commit index", zap.Uint64("index", commitIndex), zap.Error(err))
	}
}

// loadCommitIndex reads the persisted commitIndex from the stable store on
// startup.  If no value exists (fresh node) it returns 0.
func (r *raft) loadCommitIndex() error {
	raw, err := r.stable.Get([]byte(KeyCommitIndex))
	if err != nil {
		return err
	}
	if raw != nil {
		r.commitIndex = bytesToUint64(raw)
	}
	return nil
}

func (r *raft) persistLog(entries []*LogEntry) error {
	// H3: reject a non-contiguous append. Every batch must start exactly at
	// lastIndex+1 and each entry's index must be one greater than the previous,
	// otherwise a gap (or overlap) in the log would silently corrupt the log
	// invariants and let applyCommitted/replicateTo read holes.
	expected := r.lastIndex + 1
	for _, entry := range entries {
		if entry.Index != expected {
			return fmt.Errorf("non-contiguous log append: entry index %d, expected %d",
				entry.Index, expected)
		}
		expected++
	}

	if err := r.log.Append(entries); err != nil {
		return err
	}

	for _, entry := range entries {
		r.lastIndex = entry.Index
		r.lastTerm = entry.Term
	}

	return nil
}

func (r *raft) truncateLog(index uint64) error {
	if err := r.log.DeleteRange(index+1, r.lastIndex+1); err != nil {
		return err
	}
	r.lastIndex = index
	if index > 0 {
		entry, err := r.log.Get(index)
		if err != nil {
			return err
		}
		r.lastTerm = entry.Term
	} else {
		r.lastTerm = 0
	}
	// Cap commitIndex to the new lastIndex.  Entries beyond lastIndex were
	// removed; allowing applyCommitted to advance into that gap would cause
	// "not found" errors against the log store.  Persist the reduced value so
	// that a crash/restart does not reload a stale (too-high) commitIndex.
	if r.commitIndex > r.lastIndex {
		r.commitIndex = r.lastIndex
		r.persistCommitIndex()
	}
	return nil
}

// rpcContext derives a per-RPC context with a deadline (~one election timeout)
// that is also canceled when the node stops (M-C1). This bounds outbound
// replicate/vote/prevote/timeoutnow/snapshot RPCs so a stuck peer cannot pin an
// in-flight slot indefinitely, and lets a shutdown promptly abort them. The
// caller MUST invoke the returned cancel func.
func (r *raft) rpcContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), r.electionTimeout())
	// Cancel the RPC when the node stops so shutdown does not wait on a hung
	// peer. A single watcher goroutine per RPC, released via done.
	done := make(chan struct{})
	go func() {
		select {
		case <-r.stopCh:
			cancel()
		case <-done:
		}
	}()
	return ctx, func() {
		close(done)
		cancel()
	}
}

func (r *raft) heartbeatInterval() time.Duration {
	return time.Duration(r.config.HeartbeatTick) * 50 * time.Millisecond
}

func (r *raft) electionTimeout() time.Duration {
	return time.Duration(r.config.ElectionTick) * 50 * time.Millisecond
}

// randomElectionTickCount returns a randomized election timeout expressed as a
// number of heartbeat ticks, between ElectionTick and 2*ElectionTick.
func (r *raft) randomElectionTickCount() int {
	jitter := rand.Intn(r.config.ElectionTick + 1)
	return r.config.ElectionTick + jitter
}

func uint64ToBytes(v uint64) []byte {
	return []byte{
		byte(v >> 56),
		byte(v >> 48),
		byte(v >> 40),
		byte(v >> 32),
		byte(v >> 24),
		byte(v >> 16),
		byte(v >> 8),
		byte(v),
	}
}

func bytesToUint64(b []byte) uint64 {
	if len(b) < 8 {
		return 0
	}
	return uint64(b[0])<<56 |
		uint64(b[1])<<48 |
		uint64(b[2])<<40 |
		uint64(b[3])<<32 |
		uint64(b[4])<<24 |
		uint64(b[5])<<16 |
		uint64(b[6])<<8 |
		uint64(b[7])
}

var mathMaxInt64 = ^uint64(0)

func (r *raft) handleAppendEntries(req *AppendEntriesRequest) *AppendEntriesResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &AppendEntriesResponse{
		Term:    r.term,
		Success: false,
	}

	if req.Term < r.term {
		return resp
	}

	if req.Term > r.term {
		r.term = req.Term
		r.failPendingFutures()
		r.state = StateFollower
		r.votedFor = ""
		// Clear stale acks: a re-elected leader must re-confirm quorum.
		r.heartbeatAcks = make(map[ServerID]time.Time)
		metrics.RecordTerm(r.term)
		r.persistTermAndVotedForLogged()
	}

	// Step down if we were candidate and receive AppendEntries from a valid leader.
	if r.state == StateCandidate && req.Term == r.term {
		r.state = StateFollower
	}

	if req.LeaderID != r.leaderID {
		metrics.RecordLeaderChange() // observed a new/changed leader via AppendEntries
	}
	r.leaderID = req.LeaderID
	// Reset election timer on valid leader contact.
	r.electionTicks = r.randomElectionTickCount()

	// M1: a valid leader other than ourselves is now in charge. If we had a
	// leadership transfer in flight, it has succeeded — resolve it with nil.
	if r.leadershipTransfer != nil && req.LeaderID != r.localID {
		r.finishTransfer(nil)
	}

	// Log consistency check.
	if req.PrevLogIndex > 0 {
		lastIndex, _ := r.log.LastIndex()
		if req.PrevLogIndex > lastIndex {
			resp.Index = lastIndex + 1
			return resp
		}

		prevEntry, err := r.log.Get(req.PrevLogIndex)
		if err != nil {
			resp.Index = req.PrevLogIndex
			return resp
		}

		if prevEntry.Term != req.PrevLogTerm {
			// C3: Consistency check failed. Do NOT truncate here — just reject.
			// The leader will decrement nextIndex and retry with an earlier
			// PrevLogIndex. Truncating on a bare prevLogTerm mismatch can delete
			// entries the request does not supersede (including committed ones).
			// M7: return the conflicting term and the first index we hold for it
			// so the leader can back up past the whole term in one step.
			resp.ConflictTerm = prevEntry.Term
			ci := req.PrevLogIndex
			for ci > 1 {
				e, gerr := r.log.Get(ci - 1)
				if gerr != nil || e.Term != prevEntry.Term {
					break
				}
				ci--
			}
			resp.Index = ci
			return resp
		}
	}

	// Append new entries. Find the first incoming entry that either does not
	// exist yet or conflicts (same index, different term); truncate from the
	// conflict point and append the remaining suffix in one shot. Entries
	// already present with a matching term are skipped (idempotent).
	//
	// H3: track the index of the last entry that this request establishes on our
	// log (either freshly appended or already-present-and-matching). commitIndex
	// must not advance past what this request actually covers, even if our log
	// happens to be longer (e.g. from an earlier, longer leader's suffix).
	lastReqIndex := req.PrevLogIndex
	if n := len(req.Entries); n > 0 {
		lastReqIndex = req.Entries[n-1].Index
	}
	for i, entry := range req.Entries {
		existing, err := r.log.Get(entry.Index)
		if err == nil {
			if existing.Term == entry.Term {
				continue // already present and identical
			}
			// C3: Never overwrite a committed entry. In correct operation a
			// committed entry can never conflict; a conflict here means a
			// stale/duplicate or malicious request, which we reject rather than
			// destroy committed state (State Machine Safety).
			if entry.Index <= r.commitIndex {
				resp.Index = r.commitIndex + 1
				return resp
			}
			// Truncate the conflicting entry and everything after it.
			if err := r.truncateLog(entry.Index - 1); err != nil {
				r.logger.Error("failed to truncate log", zap.Error(err))
				return resp
			}
		}
		// First new/conflicting index: append this entry and all that follow.
		if err := r.persistLog(req.Entries[i:]); err != nil {
			r.logger.Error("failed to persist log entries", zap.Error(err))
			return resp
		}
		break
	}

	if req.LeaderCommit > r.commitIndex {
		// H3: cap at the last index this request actually carries/covers, not
		// r.lastIndex. Committing over a gap (up to a longer local suffix the
		// leader did not include in this request) could commit entries the
		// leader has not yet committed.
		r.commitIndex = min(req.LeaderCommit, lastReqIndex)
		metrics.RecordCommitIndex(r.commitIndex)
		r.persistCommitIndex()
	}

	resp.Term = r.term
	resp.Success = true
	resp.Index = r.lastIndex

	return resp
}

func (r *raft) handleRequestVote(req *RequestVoteRequest) *RequestVoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &RequestVoteResponse{
		Term:        r.term,
		VoteGranted: false,
	}

	// ---- Pre-vote path -------------------------------------------------------
	// Pre-vote RPCs must not modify any persistent state.  We only grant if:
	//   1. The proposed term (req.Term) is >= our current term.
	//   2. We haven't heard from a leader recently (election timer not reset).
	//   3. The candidate's log is at least as up-to-date as ours.
	if req.PreVote {
		if req.Term < r.term {
			resp.Reason = "pre-vote: stale proposed term"
			return resp
		}

		// "Haven't heard from a leader recently" is approximated by checking
		// whether our election timer has been fully depleted (electionTicks <= 0).
		// A fresh reset (electionTicks > 0) means we recently heard from a leader.
		leaderRecentlyHeard := r.electionTicks > 0 && r.leaderID != ""
		if leaderRecentlyHeard {
			resp.Reason = "pre-vote: heard from leader recently"
			return resp
		}

		lastIndex, lastTerm := r.lastIndex, r.lastTerm
		logOK := req.LastLogTerm > lastTerm ||
			(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)
		if !logOK {
			resp.Reason = "pre-vote: candidate log not up-to-date"
			return resp
		}

		resp.Term = r.term
		resp.VoteGranted = true
		return resp
	}
	// ---- End pre-vote path ---------------------------------------------------

	if req.Term < r.term {
		resp.Reason = "stale term"
		return resp
	}

	// Disruptive-server protection (Ongaro §4.2.3): if we have heard from a
	// current leader within the last election timeout, reject a real RequestVote
	// WITHOUT adopting its (higher) term — unless it is a leadership transfer,
	// which is expected to disrupt. This stops a removed/partitioned server from
	// inflating terms and triggering spurious elections against a healthy leader.
	//
	// Gated on CheckQuorum: the rejection is only safe when a leader that loses
	// quorum steps down on its own (checkQuorum), which guarantees this guard
	// cannot permanently block a legitimate election. Pre-vote covers the common
	// case; this adds protection for non-pre-vote term bumps.
	if r.config.CheckQuorum && !req.LeaderTransfer && r.electionTicks > 0 && r.leaderID != "" {
		resp.Reason = "recently heard from leader (disruptive-server protection)"
		metrics.RecordRejection("vote")
		return resp
	}

	if req.Term > r.term {
		r.term = req.Term
		r.failPendingFutures()
		r.state = StateFollower
		r.votedFor = ""
		metrics.RecordTerm(r.term)
		r.persistTermAndVotedForLogged()
	}

	// Grant vote only if we haven't voted for someone else this term.
	if r.votedFor != "" && r.votedFor != req.CandidateID {
		resp.Reason = "already voted"
		return resp
	}

	// Candidate's log must be at least as up-to-date as ours.
	lastIndex, lastTerm := r.lastIndex, r.lastTerm
	logOK := req.LastLogTerm > lastTerm ||
		(req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIndex)
	if !logOK {
		resp.Reason = "candidate log is not up-to-date"
		return resp
	}

	prevVotedFor := r.votedFor
	r.votedFor = req.CandidateID
	// Durability is a precondition for granting a vote: if the vote cannot be
	// persisted, we must NOT tell the candidate it was granted. Otherwise a crash
	// before the write reaches disk could let this node grant its vote again in
	// the same term to a different candidate — electing two leaders in one term.
	// (Step-down/term-bump paths use the best-effort ...Logged variant because a
	// lost write there is recovered by term comparison on restart; a lost *grant*
	// is not.)
	if err := r.persistTermAndVotedFor(); err != nil {
		r.logger.Error("failed to persist vote grant; denying vote", zap.Error(err))
		r.votedFor = prevVotedFor
		resp.Term = r.term
		resp.VoteGranted = false
		return resp
	}
	// Reset election timer only once the vote is durably granted.
	r.electionTicks = r.randomElectionTickCount()
	resp.Term = r.term
	resp.VoteGranted = true

	return resp
}

func (r *raft) handleInstallSnapshot(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	r.mu.Lock()
	defer r.mu.Unlock()

	resp := &InstallSnapshotResponse{
		Term: r.term,
	}

	if req.Term < r.term {
		return resp
	}

	if req.Term > r.term {
		r.term = req.Term
		r.failPendingFutures()
		r.state = StateFollower
		r.votedFor = ""
		// Clear heartbeat acks so that if this node re-becomes leader it
		// must re-confirm quorum rather than using stale acks from the old term.
		r.heartbeatAcks = make(map[ServerID]time.Time)
		metrics.RecordTerm(r.term)
		r.persistTermAndVotedForLogged()
	}

	if req.LeaderID != r.leaderID {
		metrics.RecordLeaderChange() // observed a new/changed leader via InstallSnapshot
	}
	r.leaderID = req.LeaderID
	r.electionTicks = r.randomElectionTickCount()

	// Reassemble Offset-ordered chunks across the per-chunk InstallSnapshot RPCs
	// (H9): the leader streams a snapshot larger than snapshotChunkSize as a
	// sequence of chunks, Done only on the last. Accumulate here and restore the
	// full payload once Done arrives.
	if req.Offset == 0 {
		// Start (or restart) the transfer.
		r.snapReasm.index = req.LastIncludedIndex
		r.snapReasm.buf = append(r.snapReasm.buf[:0], req.Data...)
	} else if r.snapReasm.index == req.LastIncludedIndex &&
		req.Offset == uint64(len(r.snapReasm.buf)) {
		// Contiguous continuation of the in-progress transfer.
		r.snapReasm.buf = append(r.snapReasm.buf, req.Data...)
	} else {
		// Out-of-order/mismatched chunk (a lost chunk, or a different snapshot):
		// drop the partial buffer and wait for the leader to restart at Offset 0.
		r.logger.Warn("discarding out-of-order snapshot chunk",
			zap.Uint64("offset", req.Offset),
			zap.Uint64("have", uint64(len(r.snapReasm.buf))),
			zap.Uint64("index", req.LastIncludedIndex))
		r.snapReasm.reset()
		return resp
	}

	if len(r.snapReasm.buf) > maxReassembledSnapshotBytes {
		r.logger.Error("snapshot exceeds reassembly cap; aborting transfer",
			zap.Int("bytes", len(r.snapReasm.buf)),
			zap.Uint64("index", req.LastIncludedIndex))
		r.snapReasm.reset()
		return resp
	}

	if req.Done {
		// The reassembled payload is what we restore, not this final chunk alone.
		fullData := r.snapReasm.buf
		r.snapReasm.reset()

		// C4: never move state backward on a stale/duplicate snapshot. If we
		// already have this index committed, ignore it (but ack success so the
		// leader stops retrying).
		if req.LastIncludedIndex <= r.commitIndex {
			resp.Term = r.term
			return resp
		}

		req.Data = fullData
		if err := r.restoreSnapshotData(req); err != nil {
			r.logger.Error("failed to restore snapshot", zap.Error(err))
			return resp
		}

		// C5: reconcile the log with the installed snapshot. If we have an entry
		// at LastIncludedIndex whose term matches, the snapshot is a prefix of
		// our log — retain the following entries and compact the prefix.
		// Otherwise the snapshot supersedes our log entirely — discard all
		// entries so the log never contradicts the snapshot.
		existing, gerr := r.log.Get(req.LastIncludedIndex)
		if gerr == nil && existing.Term == req.LastIncludedTerm {
			first, _ := r.log.FirstIndex()
			if first == 0 {
				first = 1
			}
			if req.LastIncludedIndex >= first {
				if derr := r.log.DeleteRange(first, req.LastIncludedIndex); derr != nil {
					r.logger.Error("failed to compact log after snapshot", zap.Error(derr))
				}
			}
			if req.LastIncludedIndex > r.lastIndex {
				r.lastIndex = req.LastIncludedIndex
				r.lastTerm = req.LastIncludedTerm
			}
		} else {
			first, _ := r.log.FirstIndex()
			last, _ := r.log.LastIndex()
			if last > 0 && last >= first {
				if derr := r.log.DeleteRange(first, last); derr != nil {
					r.logger.Error("failed to discard log after snapshot", zap.Error(derr))
				}
			}
			r.lastIndex = req.LastIncludedIndex
			r.lastTerm = req.LastIncludedTerm
		}

		r.commitIndex = req.LastIncludedIndex
		r.applyIndex = req.LastIncludedIndex
		r.persistCommitIndex()
		metrics.RecordSnapshotRestore()

		// L9: snapshot install advances applyIndex; wake WaitApplied callers.
		// notifyApplied only takes applyMu (never r.mu), so calling it while
		// holding r.mu is deadlock-free.
		r.notifyApplied(req.LastIncludedIndex)
	}

	return resp
}

// restoreSnapshotData writes the raw snapshot data from the InstallSnapshot
// request into the snapshot store and restores the FSM from it.
func (r *raft) restoreSnapshotData(req *InstallSnapshotRequest) error {
	sink, err := r.snapshot.Create(
		SnapshotVersionMax,
		req.LastIncludedIndex,
		req.LastIncludedTerm,
		r.configuration,
	)
	if err != nil {
		return err
	}

	metrics.RecordSnapshotSize(len(req.Data))
	if _, err := sink.Write(req.Data); err != nil {
		_ = sink.Cancel() // best-effort cleanup; original write error is what we report
		return err
	}

	if err := sink.Close(); err != nil {
		return err
	}

	snap, _, err := r.snapshot.Open(sink.ID())
	if err != nil {
		return err
	}

	reader := snap.Reader()
	defer reader.Close()

	if err := r.fsm.Restore(reader); err != nil {
		return err
	}

	r.logger.Info("AUDIT snapshot restored",
		zap.String("node_id", string(r.localID)),
		zap.Uint64("last_included_index", req.LastIncludedIndex),
		zap.Uint64("last_included_term", req.LastIncludedTerm),
	)

	return nil
}

func (r *raft) HandleAppendEntriesRPC(req *AppendEntriesRequest) *AppendEntriesResponse {
	resp := r.handleAppendEntries(req)
	// Wake the run() goroutine so it applies any newly committed entries.
	// Do NOT call applyCommitted() here directly: that would race with the
	// run() goroutine's own applyCommitted() call on r.applyIndex.
	select {
	case r.commitNotifyCh <- struct{}{}:
	default:
	}
	return resp
}

func (r *raft) HandleRequestVoteRPC(req *RequestVoteRequest) *RequestVoteResponse {
	return r.handleRequestVote(req)
}

func (r *raft) HandleInstallSnapshotRPC(req *InstallSnapshotRequest) *InstallSnapshotResponse {
	return r.handleInstallSnapshot(req)
}

func (r *raft) SetLogger(logger *zap.Logger) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.logger = logger
}
