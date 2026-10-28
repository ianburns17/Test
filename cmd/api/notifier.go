package main

import (
	"sync"

	"github.com/google/uuid"
)

// JobNotifier manages in-memory event notifications for waiting long-poll observers.
// It implements the Counter + Version synchronization pattern (WAIT-01, VER-03):
// - Safe subscription order prevents race conditions between DB checking and waiting.
// - Broadcast to all active subscribers of the same job ensures every observer wakes (VER-03).
// - Non-blocking channel sends coalesce duplicate notifications safely.
// - Deterministic unsubscribe cleans up channels and resources on exit/timeout/cancellation (WAIT-02).
type JobNotifier struct {
	mu          sync.RWMutex
	subscribers map[uuid.UUID]map[chan struct{}]struct{}
}

// NewJobNotifier creates and initializes a JobNotifier instance.
func NewJobNotifier() *JobNotifier {
	return &JobNotifier{
		subscribers: make(map[uuid.UUID]map[chan struct{}]struct{}),
	}
}

// Subscribe registers a notification channel for the given job identifier (internal UUID or public UUID).
// Returns the buffered channel and an unsubscribe cleanup function.
// Observers MUST subscribe BEFORE checking authoritative database state to eliminate race windows (WAIT-01).
func (n *JobNotifier) Subscribe(jobID uuid.UUID) (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1) // Buffered channel allows 1 coalesced pending signal

	n.mu.Lock()
	if _, exists := n.subscribers[jobID]; !exists {
		n.subscribers[jobID] = make(map[chan struct{}]struct{})
	}
	n.subscribers[jobID][ch] = struct{}{}
	n.mu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			n.mu.Lock()
			defer n.mu.Unlock()
			if subs, exists := n.subscribers[jobID]; exists {
				delete(subs, ch)
				if len(subs) == 0 {
					delete(n.subscribers, jobID)
				}
			}
		})
	}

	return ch, unsubscribe
}

// Notify broadcasts a change wake-up signal to all active subscribers for the given jobID.
// Non-blocking select ensures one slow/hung observer does not delay others, and coalesces duplicate signals (VER-03).
func (n *JobNotifier) Notify(jobID uuid.UUID) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	subs, exists := n.subscribers[jobID]
	if !exists {
		return
	}

	for ch := range subs {
		select {
		case ch <- struct{}{}:
		default:
			// Already has an unconsumed wake-up notification; coalesced (VER-03)
		}
	}
}

// ActiveObserversCount returns the number of active listeners for a job (useful for metrics/diagnostics).
func (n *JobNotifier) ActiveObserversCount(jobID uuid.UUID) int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if subs, exists := n.subscribers[jobID]; exists {
		return len(subs)
	}
	return 0
}
