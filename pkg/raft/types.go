package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

var (
	ErrNotLeader      = errors.New("not leader")
	ErrLeadershipLost = errors.New("leadership lost")
	ErrNotStarted     = errors.New("raft not started")
	ErrAlreadyStarted = errors.New("raft already started")
	ErrConfiguration  = errors.New("invalid configuration")
	ErrShardInvalid   = errors.New("invalid shard")
	ErrCanceled       = errors.New("canceled")
	ErrTimeout        = errors.New("timeout")
	ErrUnsupported    = errors.New("unsupported")
	ErrEmptyCluster   = errors.New("empty cluster")
	// ErrConfigChangeInProgress is returned when a membership change is
	// requested while a previous configuration change is still uncommitted.
	// Raft permits at most one outstanding configuration change (C7).
	ErrConfigChangeInProgress = errors.New("configuration change already in progress")
	ErrLearnerNotReady        = errors.New("learner not ready")
	// ErrNodeBusy is returned when the leader has too many outstanding
	// un-applied proposals (the apply loop is stalling) and is shedding load
	// (M-R5). Callers should back off and retry.
	ErrNodeBusy = errors.New("node busy: too many outstanding proposals")
)

type ServerID string
type ServerAddress string

func (s ServerID) String() string      { return string(s) }
func (a ServerAddress) String() string { return string(a) }

type Server struct {
	ID      ServerID
	Address ServerAddress
	Learner bool
}

type Configuration struct {
	Servers []Server
}

func (c *Configuration) Copy() Configuration {
	servers := make([]Server, len(c.Servers))
	copy(servers, c.Servers)
	return Configuration{Servers: servers}
}

func (c *Configuration) GetServer(id ServerID) *Server {
	// L3: return a pointer to the real slice element, not to the range-loop
	// copy (which is reused each iteration and escapes with the wrong value).
	for i := range c.Servers {
		if c.Servers[i].ID == id {
			return &c.Servers[i]
		}
	}
	return nil
}

func (c *Configuration) Contains(id ServerID) bool {
	return c.GetServer(id) != nil
}

// IsVoter reports whether id is a voting member (present and not a learner).
func (c *Configuration) IsVoter(id ServerID) bool {
	s := c.GetServer(id)
	return s != nil && !s.Learner
}

func (c *Configuration) VoteCount() int {
	count := 0
	for _, s := range c.Servers {
		if !s.Learner {
			count++
		}
	}
	return count
}

func (c *Configuration) QuorumSize() int {
	voters := c.VoteCount()
	return voters/2 + 1
}

func (c *Configuration) Learners() []*Server {
	// L3: append pointers to the real elements, not to the range-loop copy.
	var learners []*Server
	for i := range c.Servers {
		if c.Servers[i].Learner {
			learners = append(learners, &c.Servers[i])
		}
	}
	return learners
}

func (c *Configuration) Voters() []*Server {
	// L3: append pointers to the real elements, not to the range-loop copy.
	var voters []*Server
	for i := range c.Servers {
		if !c.Servers[i].Learner {
			voters = append(voters, &c.Servers[i])
		}
	}
	return voters
}

func (c *Configuration) AddServer(id ServerID, addr ServerAddress, learner bool) Configuration {
	newConfig := c.Copy()
	newConfig.Servers = append(newConfig.Servers, Server{
		ID:      id,
		Address: addr,
		Learner: learner,
	})
	return newConfig
}

func (c *Configuration) RemoveServer(id ServerID) Configuration {
	newConfig := c.Copy()
	for i, s := range newConfig.Servers {
		if s.ID == id {
			newConfig.Servers = append(newConfig.Servers[:i], newConfig.Servers[i+1:]...)
			break
		}
	}
	return newConfig
}

func (c *Configuration) UpdateServer(id ServerID, addr ServerAddress) Configuration {
	newConfig := c.Copy()
	for i, s := range newConfig.Servers {
		if s.ID == id {
			newConfig.Servers[i].Address = addr
			break
		}
	}
	return newConfig
}

// IsJoint always returns false for a plain Configuration.
// Use JointConfiguration.IsJoint() for joint-consensus checks.
func (c *Configuration) IsJoint() bool {
	return false
}

type RaftState int

const (
	StateFollower RaftState = iota
	StateCandidate
	StateLeader
	StateLearner
	StateShutdown
)

func (s RaftState) String() string {
	switch s {
	case StateFollower:
		return "Follower"
	case StateCandidate:
		return "Candidate"
	case StateLeader:
		return "Leader"
	case StateLearner:
		return "Learner"
	case StateShutdown:
		return "Shutdown"
	default:
		return "Unknown"
	}
}

type EntryType uint8

const (
	EntryNormal EntryType = iota
	EntryConfiguration
	EntrySnapshot
)

type LogEntry struct {
	Term  uint64
	Index uint64
	Type  EntryType
	Data  []byte
}

func (e *LogEntry) Clone() LogEntry {
	data := make([]byte, len(e.Data))
	copy(data, e.Data)
	return LogEntry{
		Term:  e.Term,
		Index: e.Index,
		Type:  e.Type,
		Data:  data,
	}
}

type FSM interface {
	Apply(entry []byte) (result []byte, err error)
	Snapshot() (Snapshot, error)
	Restore(reader io.Reader) error
}

type Snapshot interface {
	Index() uint64
	Term() uint64
	Reader() io.ReadCloser
}

type SnapshotMeta struct {
	Index         uint64
	Term          uint64
	Configuration Configuration
	ID            string
	Version       uint64
}

type SnapshotStore interface {
	Create(version SnapshotVersion, index, term uint64, configuration Configuration) (SnapshotSink, error)
	Open(id string) (Snapshot, *SnapshotMeta, error)
	List() ([]*SnapshotMeta, error)
	Delete(id string) error
}

type SnapshotSink interface {
	io.WriteCloser
	ID() string
	Cancel() error
}

type SnapshotVersion uint64

type SnapshotRequest struct {
	Stop bool
}

const (
	SnapshotVersionMax    SnapshotVersion = 1
	SnapshotVersionLegacy SnapshotVersion = 0
)

type LogStore interface {
	Append(entries []*LogEntry) error
	Get(idx uint64) (*LogEntry, error)
	Iterate(start, stop uint64, f func(*LogEntry) bool) error
	FirstIndex() (uint64, error)
	LastIndex() (uint64, error)
	DeleteRange(min, max uint64) error
	Close() error
}

type StableStore interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Iterate(prefix []byte, f func(key, value []byte) bool) error
	Sync() error
	Close() error
}

const (
	KeyTerm         = "raft_term"
	KeyVotedFor     = "raft_voted_for"
	KeyLastSnapshot = "raft_last_snapshot"
	KeyLastIndex    = "raft_last_index"
	KeyConfig       = "raft_config"
	KeyCommitIndex  = "raft_commit_index"
)

type Config struct {
	LocalID           ServerID
	ElectionTick      int
	HeartbeatTick     int
	MaxSizePerMsg     uint64
	MaxInflight       int
	SnapshotInterval  time.Duration
	SnapshotThreshold uint64
	TrailingLogs      uint64
	FSyncInterval     time.Duration

	PreVote                   bool
	DisableProposalForwarding bool
	LearnerMaxOldLogIndex     uint64

	// CheckQuorum makes a leader step down if it has not heard from a quorum of
	// voters within an election timeout. This bounds how long a
	// partitioned-minority leader retains leadership (and keeps failing writes)
	// before a new leader on the majority side can take over.
	CheckQuorum bool

	InitialConfiguration Configuration

	// StartAsLearner causes the node to start in StateLearner mode instead of
	// StateFollower. A learner never initiates elections and only receives
	// log replication from the leader.
	StartAsLearner bool

	// lastWarning holds a non-fatal advisory produced by the most recent
	// Validate call (L7). Exposed via LastValidateWarning.
	lastWarning string
}

// Sane defaults / bounds for the fields that Validate defaults (M-R8).
const (
	defaultSnapshotThreshold      uint64 = 8192
	defaultTrailingLogs           uint64 = 10240
	defaultConfigMaxSizePerMsg    uint64 = 1024 * 1024
	defaultConfigMaxInflight             = 256
	defaultConfigSnapshotInterval        = 120 * time.Second
	// L7: the conventional Raft ratio is ~10x. We warn (not hard-fail) below 10x
	// so existing 5:1 test configs stay valid, but hard-fail below the 3x floor.
	recommendedElectionRatio = 10
	minElectionRatio         = 3
)

func (c *Config) Validate() error {
	// Defaults are applied in newRaft before Validate is called, so these
	// fields are guaranteed to be positive when this function runs.
	if c.ElectionTick <= 0 {
		return fmt.Errorf("ElectionTick must be positive")
	}
	if c.HeartbeatTick <= 0 {
		return fmt.Errorf("HeartbeatTick must be positive")
	}
	// L1/L7: the randomized election timeout must leave enough headroom over the
	// heartbeat interval that a healthy leader's heartbeats reliably reset the
	// follower's election timer before it fires. Hard-fail below the 3x floor;
	// the recommended ratio is ~10x (surfaced via LastValidateWarning so
	// existing 5:1 test configs stay valid).
	if c.ElectionTick < minElectionRatio*c.HeartbeatTick {
		return fmt.Errorf("ElectionTick (%d) must be at least %dx HeartbeatTick (%d)",
			c.ElectionTick, minElectionRatio, c.HeartbeatTick)
	}
	c.lastWarning = ""
	if c.ElectionTick < recommendedElectionRatio*c.HeartbeatTick {
		c.lastWarning = fmt.Sprintf("ElectionTick (%d) is below the recommended %dx HeartbeatTick (%d); "+
			"GC/network blips may trigger spurious elections",
			c.ElectionTick, recommendedElectionRatio, c.HeartbeatTick)
	}
	if c.LocalID == "" {
		return fmt.Errorf("LocalID is required")
	}

	// M-R8: validate + default the remaining tunables so a mis-set field cannot
	// silently disable snapshotting/batching or produce a degenerate node.
	if c.SnapshotInterval < 0 {
		return fmt.Errorf("SnapshotInterval must not be negative")
	}
	if c.SnapshotInterval == 0 {
		c.SnapshotInterval = defaultConfigSnapshotInterval
	}
	if c.SnapshotThreshold == 0 {
		c.SnapshotThreshold = defaultSnapshotThreshold
	}
	if c.TrailingLogs == 0 {
		c.TrailingLogs = defaultTrailingLogs
	}
	if c.FSyncInterval < 0 {
		return fmt.Errorf("FSyncInterval must not be negative")
	}
	if c.MaxSizePerMsg == 0 {
		c.MaxSizePerMsg = defaultConfigMaxSizePerMsg
	}
	if c.MaxInflight <= 0 {
		c.MaxInflight = defaultConfigMaxInflight
	}
	return nil
}

// LastValidateWarning returns a non-fatal advisory message produced by the most
// recent Validate call (empty if none). Used to surface soft misconfigurations
// (e.g. an election/heartbeat ratio below the recommended 10x — L7).
func (c *Config) LastValidateWarning() string {
	return c.lastWarning
}

type Transport interface {
	AppendEntries(ctx context.Context, target ServerID, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
	RequestVote(ctx context.Context, target ServerID, req *RequestVoteRequest) (*RequestVoteResponse, error)
	InstallSnapshot(ctx context.Context, target ServerID, req *InstallSnapshotRequest) (*InstallSnapshotResponse, error)
	TimeoutNow(ctx context.Context, target ServerID) error
	SetLocalID(id ServerID)
	Close() error
}

type Raft interface {
	State() RaftState
	Leader() ServerID
	Term() uint64
	LastIndex() uint64
	LastTerm() uint64
	AppliedIndex() uint64

	// WaitApplied blocks until the FSM has applied through idx
	// (AppliedIndex() >= idx) or ctx is done (returning ctx.Err()). It does not
	// busy-poll; it is woken by an applied-index notification. Returns
	// immediately if idx is already applied.
	WaitApplied(ctx context.Context, idx uint64) error

	Configuration() Configuration

	// ReadIndex returns the index up to which reads are linearizable.
	// On the leader it confirms quorum (via leader lease) without writing to
	// the Raft log, making it significantly cheaper than Apply for reads.
	// Returns ErrNotLeader when called on a follower.
	ReadIndex(ctx context.Context) (uint64, error)

	Apply(ctx context.Context, data []byte) ([]byte, error)
	Demote(ctx context.Context, target ServerID) error
	RemoveServer(ctx context.Context, target ServerID) error
	ReplaceServer(ctx context.Context, oldID, newID ServerID, newAddr ServerAddress) error
	AddServer(ctx context.Context, target ServerID, addr ServerAddress) error
	AddLearner(ctx context.Context, target ServerID, addr ServerAddress) error
	PromoteLearner(ctx context.Context, target ServerID) error
	RequestLeadership(ctx context.Context) error

	Snapshot() error
	Restore(ctx context.Context, reader io.Reader) error

	Start() error
	Shutdown() error
}

type AppendEntriesRequest struct {
	Term         uint64
	LeaderID     ServerID
	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []*LogEntry
	LeaderCommit uint64
}

type AppendEntriesResponse struct {
	Term    uint64
	Success bool
	Index   uint64
	// ConflictTerm is the term of the follower's conflicting entry at
	// PrevLogIndex (0 if the follower's log was simply too short). Together with
	// Index (the first index the follower holds for ConflictTerm) it lets the
	// leader skip an entire conflicting term in one step instead of decrementing
	// nextIndex one index at a time (M7).
	ConflictTerm uint64
}

type RequestVoteRequest struct {
	Term         uint64
	CandidateID  ServerID
	LastLogIndex uint64
	LastLogTerm  uint64
	PreVote      bool
	// LeaderTransfer marks a campaign started by a TimeoutNow leadership transfer.
	// Recipients honor it even when they heard from a leader recently, bypassing
	// the disruptive-server rejection (§4.2.3).
	LeaderTransfer bool
}

type RequestVoteResponse struct {
	Term        uint64
	VoteGranted bool
	Reason      string
}

type InstallSnapshotRequest struct {
	Term              uint64
	LeaderID          ServerID
	LastIncludedIndex uint64
	LastIncludedTerm  uint64
	Offset            uint64
	Data              []byte
	Done              bool
}

type InstallSnapshotResponse struct {
	Term uint64
}

type ReadConsistency int

const (
	ReadDefault ReadConsistency = iota
	ReadLinearizable
	ReadStale
)

type Future interface {
	Error() error
	Index() uint64
	Term() uint64
}

type ApplyFuture struct {
	data   []byte
	err    error
	index  uint64
	term   uint64
	result []byte
	ch     chan struct{}
}

func (a *ApplyFuture) Error() error   { return a.err }
func (a *ApplyFuture) Index() uint64  { return a.index }
func (a *ApplyFuture) Term() uint64   { return a.term }
func (a *ApplyFuture) Result() []byte { return a.result }

func (a *ApplyFuture) Await() error {
	<-a.ch
	return a.err
}

func (a *ApplyFuture) respond(err error, index, term uint64, result []byte) {
	a.err = err
	a.index = index
	a.term = term
	a.result = result
	close(a.ch)
}

func NewRaft(config *Config, localID ServerID, log LogStore, stable StableStore, snapshot SnapshotStore, fsm FSM, transport Transport) (Raft, error) {
	return newRaft(config, localID, log, stable, snapshot, fsm, transport)
}
