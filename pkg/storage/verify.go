package storage

import (
	"fmt"

	raft "github.com/sanskarpan/raft-consensus/pkg/raft"
)

type LogVerifier struct {
	log raft.LogStore
}

func NewLogVerifier(log raft.LogStore) *LogVerifier {
	return &LogVerifier{
		log: log,
	}
}

func (v *LogVerifier) VerifyConsistency(lastIndex, lastTerm uint64) (bool, uint64, uint64, error) {
	if lastIndex == 0 {
		return true, 0, 0, nil
	}

	entry, err := v.log.Get(lastIndex)
	if err != nil {
		return false, 0, 0, err
	}

	if entry.Term != lastTerm {
		conflictTerm := entry.Term
		conflictIndex, err := v.findFirstIndexOfTerm(conflictTerm)
		if err != nil {
			return false, conflictTerm, 0, err
		}
		return false, conflictTerm, conflictIndex, nil
	}

	return true, 0, 0, nil
}

func (v *LogVerifier) findFirstIndexOfTerm(term uint64) (uint64, error) {
	firstIndex, err := v.log.FirstIndex()
	if err != nil {
		return 0, err
	}

	lastIndex, err := v.log.LastIndex()
	if err != nil {
		return 0, err
	}

	for i := firstIndex; i <= lastIndex; i++ {
		entry, err := v.log.Get(i)
		if err != nil {
			return 0, err
		}
		if entry.Term == term {
			return i, nil
		}
	}

	return lastIndex + 1, nil
}

func (v *LogVerifier) VerifyLog(startIndex uint64, entries []*raft.LogEntry) (bool, error) {
	if len(entries) == 0 {
		return true, nil
	}

	prevIndex := entries[0].Index - 1
	prevTerm := entries[0].Term

	if prevIndex > 0 {
		prevEntry, err := v.log.Get(prevIndex)
		if err != nil {
			return false, err
		}
		if prevEntry.Term != prevTerm {
			return false, fmt.Errorf("previous term mismatch: expected %d, got %d", prevTerm, prevEntry.Term)
		}
	}

	for i, entry := range entries {
		if i > 0 {
			prevEntry := entries[i-1]
			if entry.Index != prevEntry.Index+1 {
				return false, fmt.Errorf("index gap at position %d: expected %d, got %d", i, prevEntry.Index+1, entry.Index)
			}
		}

		existingEntry, err := v.log.Get(entry.Index)
		if err != nil && err != ErrNotFound {
			return false, err
		}

		if existingEntry != nil && existingEntry.Term != entry.Term {
			return false, fmt.Errorf("term mismatch at index %d: expected %d, got %d", entry.Index, entry.Term, existingEntry.Term)
		}
	}

	return true, nil
}
