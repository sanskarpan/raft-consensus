package raft

type JointConfiguration struct {
	OldConfig Configuration
	NewConfig Configuration
}

func NewJointConfiguration(oldConfig, newConfig Configuration) JointConfiguration {
	return JointConfiguration{
		OldConfig: oldConfig,
		NewConfig: newConfig,
	}
}

func (j JointConfiguration) IsJoint() bool {
	return len(j.OldConfig.Servers) > 0 && len(j.NewConfig.Servers) > 0
}

func (j JointConfiguration) QuorumSize() int {
	oldVoters := j.OldConfig.VoteCount()
	newVoters := j.NewConfig.VoteCount()

	oldQuorum := oldVoters/2 + 1
	newQuorum := newVoters/2 + 1

	if oldQuorum > newQuorum {
		return oldQuorum
	}
	return newQuorum
}

func (j JointConfiguration) AllServers() []Server {
	seen := make(map[ServerID]bool)
	var servers []Server

	for _, s := range j.OldConfig.Servers {
		if !seen[s.ID] {
			seen[s.ID] = true
			servers = append(servers, s)
		}
	}

	for _, s := range j.NewConfig.Servers {
		if !seen[s.ID] {
			seen[s.ID] = true
			servers = append(servers, s)
		}
	}

	return servers
}

func (j JointConfiguration) Contains(id ServerID) bool {
	return j.OldConfig.Contains(id) || j.NewConfig.Contains(id)
}

func (j JointConfiguration) GetServer(id ServerID) *Server {
	if s := j.OldConfig.GetServer(id); s != nil {
		return s
	}
	return j.NewConfig.GetServer(id)
}

type ConfigurationChange struct {
	ChangeType  ConfigurationChangeType
	ServerID    ServerID
	ServerAddr  ServerAddress
	Index       uint64
	Term        uint64
	JointConfig JointConfiguration
}

type ConfigurationChangeType int

const (
	ChangeAddNode ConfigurationChangeType = iota
	ChangeRemoveNode
	ChangeAddLearner
	ChangePromoteLearner
	ChangeJoint
	ChangeAuto
	ChangeCommitJoint
)

func (c ConfigurationChangeType) String() string {
	switch c {
	case ChangeAddNode:
		return "AddNode"
	case ChangeRemoveNode:
		return "RemoveNode"
	case ChangeAddLearner:
		return "AddLearner"
	case ChangePromoteLearner:
		return "PromoteLearner"
	case ChangeJoint:
		return "Joint"
	case ChangeAuto:
		return "Auto"
	case ChangeCommitJoint:
		return "CommitJoint"
	default:
		return "Unknown"
	}
}

type ConfigurationStore struct {
	pendingConfIndex uint64
	pendingConfTerm  uint64
	jointConfig      *JointConfiguration
}

func NewConfigurationStore() *ConfigurationStore {
	return &ConfigurationStore{}
}

func (cs *ConfigurationStore) BeginJoint(oldConfig, newConfig Configuration, index, term uint64) {
	cs.jointConfig = &JointConfiguration{
		OldConfig: oldConfig,
		NewConfig: newConfig,
	}
	cs.pendingConfIndex = index
	cs.pendingConfTerm = term
}

func (cs *ConfigurationStore) CommitJoint() {
	if cs.jointConfig != nil {
		cs.jointConfig = nil
	}
}

func (cs *ConfigurationStore) GetJointConfig() *JointConfiguration {
	return cs.jointConfig
}

func (cs *ConfigurationStore) HasJointConfig() bool {
	return cs.jointConfig != nil
}

func (cs *ConfigurationStore) AbortJoint() {
	cs.jointConfig = nil
}
