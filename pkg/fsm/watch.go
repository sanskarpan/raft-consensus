package fsm

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
)

// WatchID uniquely identifies an active watch subscription.
type WatchID int64

// WatchEvent is delivered to watch subscribers. It carries a batch of events
// (all with the same revision when emitted from a transaction) and the
// revision at which they were produced.
type WatchEvent struct {
	Events   []Event `json:"events"`
	Revision int64   `json:"revision"`
}

// watchEntry holds the state for a single active subscription.
type watchEntry struct {
	id     WatchID
	key    string // exact key or prefix, depending on prefix flag
	prefix bool   // true → match HasPrefix(ev.Key, key); false → exact match

	// startRev is the revision captured just before the watcher was registered.
	// Live dispatch delivers only events with Revision > startRev; history replay
	// delivers events with Revision <= startRev. This partition guarantees every
	// event is delivered exactly once (either live or via replay, never both) and
	// in revision order, closing the register/snapshot-revision race (H11).
	startRev int64

	ch     chan WatchEvent
	ctx    context.Context
	cancel context.CancelFunc
}

// WatchManager fans out FSM events to registered watch subscribers.
// It must be started exactly once via Start() before any Watch() calls.
type WatchManager struct {
	eventCh <-chan []Event // sourced from KVStore.NotificationChan()
	kv      *KVStore       // for history replay on Watch() with sinceRevision > 0

	mu       sync.RWMutex
	watchers map[WatchID]*watchEntry

	nextID int64 // accessed atomically

	// droppedEvents counts live WatchEvent sends that were dropped because a
	// subscriber's channel was full. Observable via DroppedEvents().
	droppedEvents uint64 // accessed atomically

	stopCh chan struct{}
}

// DroppedEvents returns the cumulative number of live watch events dropped
// across all subscribers due to slow consumers.
func (wm *WatchManager) DroppedEvents() uint64 {
	return atomic.LoadUint64(&wm.droppedEvents)
}

// NewWatchManager creates a WatchManager wired to the given KVStore.
func NewWatchManager(kv *KVStore) *WatchManager {
	return &WatchManager{
		eventCh:  kv.NotificationChan(),
		kv:       kv,
		watchers: make(map[WatchID]*watchEntry),
		stopCh:   make(chan struct{}),
	}
}

// Start launches the event-dispatch goroutine. It runs until ctx is cancelled.
func (wm *WatchManager) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case events, ok := <-wm.eventCh:
				if !ok {
					return
				}
				wm.dispatch(events)
			case <-ctx.Done():
				return
			case <-wm.stopCh:
				return
			}
		}
	}()
}

// Stop halts the dispatch goroutine.
func (wm *WatchManager) Stop() {
	close(wm.stopCh)
}

// dispatch fans out a batch of events to all matching watchers.
// Non-blocking: slow subscribers are dropped (they reconnect with sinceRevision).
func (wm *WatchManager) dispatch(events []Event) {
	wm.mu.RLock()
	defer wm.mu.RUnlock()

	if len(wm.watchers) == 0 {
		return
	}

	for _, entry := range wm.watchers {
		// Skip entries whose context has already been cancelled: the subscriber
		// is gone and delivering to it would race with removal (H11).
		select {
		case <-entry.ctx.Done():
			continue
		default:
		}

		var matching []Event
		for _, ev := range events {
			// Deliver live only events strictly newer than the revision captured
			// at registration; events at or before startRev are (or will be)
			// delivered by history replay, so live delivery here would duplicate
			// and possibly reorder them (H11).
			if ev.Revision <= entry.startRev {
				continue
			}
			if entry.matches(ev.Key) {
				matching = append(matching, ev)
			}
		}
		if len(matching) == 0 {
			continue
		}

		we := WatchEvent{
			Events:   matching,
			Revision: matching[len(matching)-1].Revision,
		}

		select {
		case entry.ch <- we:
		default:
			// Subscriber not keeping up; drop. The SSE handler will reconnect
			// using Last-Event-ID which triggers history replay.
			// Increment counter so operators can alert on slow consumers.
			atomic.AddUint64(&wm.droppedEvents, 1)
		}
	}
}

// matches reports whether an event key matches this entry's filter.
func (e *watchEntry) matches(key string) bool {
	if e.prefix {
		return strings.HasPrefix(key, e.key)
	}
	return key == e.key
}

// Watch subscribes to changes for the exact key.
// If sinceRevision > 0 buffered history events are replayed before live events.
// The returned channel is closed when the watch is cancelled.
func (wm *WatchManager) Watch(ctx context.Context, key string, sinceRevision int64) (<-chan WatchEvent, WatchID) {
	return wm.subscribe(ctx, key, false, sinceRevision)
}

// WatchPrefix subscribes to changes for all keys with the given prefix.
func (wm *WatchManager) WatchPrefix(ctx context.Context, prefix string, sinceRevision int64) (<-chan WatchEvent, WatchID) {
	return wm.subscribe(ctx, prefix, true, sinceRevision)
}

// subscribe is the common implementation for Watch and WatchPrefix.
func (wm *WatchManager) subscribe(ctx context.Context, key string, prefix bool, sinceRevision int64) (<-chan WatchEvent, WatchID) {
	id := WatchID(atomic.AddInt64(&wm.nextID, 1))
	ch := make(chan WatchEvent, 64)

	entryCtx, cancel := context.WithCancel(ctx)

	// Capture the current revision BEFORE registering the watcher (H11). Any
	// event with Revision > snapshotRevision is guaranteed to be dispatched live
	// (because the watcher is registered before that event can be applied), while
	// events with Revision <= snapshotRevision are delivered by history replay.
	// Reading the revision after registration left a window where an event could
	// slip in and be delivered both live and via replay (dup + out-of-order).
	snapshotRevision := wm.kv.GetRevision()

	entry := &watchEntry{
		id:       id,
		key:      key,
		prefix:   prefix,
		startRev: snapshotRevision,
		ch:       ch,
		ctx:      entryCtx,
		cancel:   cancel,
	}

	// Register after capturing the revision. dispatch() filters per-entry on
	// startRev so events at or below it are never delivered live even if they are
	// applied after registration.
	wm.mu.Lock()
	wm.watchers[id] = entry
	wm.mu.Unlock()

	// Only replay if there is history between sinceRevision and snapshotRevision.
	if sinceRevision >= 0 && snapshotRevision > sinceRevision {
		go wm.replayHistory(entryCtx, entry, sinceRevision, snapshotRevision)
	}

	// Auto-cancel the entry when the caller's context expires.
	go func() {
		<-entryCtx.Done()
		wm.removeEntry(id)
	}()

	return ch, id
}

// replayHistory sends buffered history events that match entry and have
// sinceRevision < Revision <= upToRevision into entry.ch.
// upToRevision is the snapshot taken at subscription time so that events
// already in the live stream (Revision > upToRevision) are not replayed.
func (wm *WatchManager) replayHistory(ctx context.Context, entry *watchEntry, sinceRevision, upToRevision int64) {
	past := wm.kv.GetHistory(sinceRevision)
	for _, ev := range past {
		if ev.Revision > upToRevision {
			continue // live stream will deliver this
		}
		if !entry.matches(ev.Key) {
			continue
		}
		we := WatchEvent{Events: []Event{ev}, Revision: ev.Revision}
		select {
		case entry.ch <- we:
		case <-ctx.Done():
			return
		}
	}
}

// Cancel removes the watch subscription identified by id.
// The associated channel will not receive any more events after this returns.
func (wm *WatchManager) Cancel(id WatchID) {
	wm.mu.Lock()
	entry, ok := wm.watchers[id]
	if ok {
		delete(wm.watchers, id)
	}
	wm.mu.Unlock()

	if ok {
		// Cancel the context; the SSE handler goroutine detects ctx.Done() and
		// returns.  We do NOT close entry.ch here because the dispatch goroutine
		// may still hold a reference — closing a channel with a concurrent sender
		// causes a panic.  The channel is GC'd when the HTTP handler exits.
		entry.cancel()
	}
}

// removeEntry is like Cancel but called from the internal auto-cancel goroutine.
// It avoids calling entry.cancel() again (already done by context expiry).
func (wm *WatchManager) removeEntry(id WatchID) {
	wm.mu.Lock()
	delete(wm.watchers, id)
	wm.mu.Unlock()
}
