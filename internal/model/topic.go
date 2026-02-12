package model

import (
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// drainingInfo tracks the state of a queue being drained during scale-in.
type drainingInfo struct {
	DrainStartAt time.Time
}

// Topic manages multiple queues and coordinates message distribution.
type Topic struct {
	mu         sync.RWMutex
	Name       string
	Namespace  string
	Queues     []*Queue
	QueueCount int
	robin      uint64 // for round-robin distribution
	ackTimeout time.Duration

	// Scaling fields
	version          uint64
	draining         map[int]*drainingInfo
	activeQueueCount int32 // atomic; excludes draining queues
}

// NewTopic creates a new topic with the specified number of queues.
func NewTopic(namespace, name string, queueCount int, ackTimeout time.Duration) *Topic {
	if queueCount <= 0 {
		queueCount = 1
	}
	queues := make([]*Queue, queueCount)
	for i := 0; i < queueCount; i++ {
		queues[i] = NewQueue(i, ackTimeout)
	}
	return &Topic{
		Name:             name,
		Namespace:        namespace,
		Queues:           queues,
		QueueCount:       queueCount,
		ackTimeout:       ackTimeout,
		draining:         make(map[int]*drainingInfo),
		activeQueueCount: int32(queueCount),
	}
}

// Version returns the current scaling version of the topic.
func (t *Topic) Version() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.version
}

// IsDraining returns true if any queues are currently being drained.
func (t *Topic) IsDraining() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.draining) > 0
}

// ActiveQueueCount returns the number of non-draining queues.
func (t *Topic) ActiveQueueCount() int {
	return int(atomic.LoadInt32(&t.activeQueueCount))
}

// GetQueue returns the queue with the given ID.
func (t *Topic) GetQueue(id int) (*Queue, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if id < 0 || id >= len(t.Queues) {
		return nil, fmt.Errorf("queue %d not found in topic %s (total: %d)", id, t.Name, len(t.Queues))
	}
	return t.Queues[id], nil
}

// SelectQueue picks a queue for publishing.
// It uses activeQueueCount to skip draining queues.
//   - queueID >= 0 (and not draining): direct routing
//   - routingKey != "": hash-based routing
//   - otherwise: round-robin
func (t *Topic) SelectQueue(routingKey string, queueID int) int {
	activeCount := int(atomic.LoadInt32(&t.activeQueueCount))
	if activeCount <= 0 {
		activeCount = t.QueueCount
	}

	// Direct routing: only if the target is an active (non-draining) queue
	if queueID >= 0 && queueID < activeCount {
		return queueID
	}

	if routingKey != "" {
		h := fnv.New32a()
		h.Write([]byte(routingKey))
		return int(h.Sum32()) % activeCount
	}
	n := atomic.AddUint64(&t.robin, 1)
	return int(n-1) % activeCount
}

// --- Scaling operations ---

// AddQueues adds new queues to the topic for scale-out.
// Returns the new QueueCount and version.
func (t *Topic) AddQueues(count int) (int, uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i := 0; i < count; i++ {
		id := len(t.Queues)
		t.Queues = append(t.Queues, NewQueue(id, t.ackTimeout))
	}
	t.QueueCount = len(t.Queues)
	atomic.StoreInt32(&t.activeQueueCount, int32(t.QueueCount))
	t.version++

	return t.QueueCount, t.version
}

// MarkDraining marks queues from newQueueCount to QueueCount-1 as draining.
// Returns an error if a drain is already in progress or newQueueCount is invalid.
func (t *Topic) MarkDraining(newQueueCount int) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.draining) > 0 {
		return fmt.Errorf("drain already in progress for topic %s", t.Name)
	}
	if newQueueCount <= 0 || newQueueCount >= t.QueueCount {
		return fmt.Errorf("invalid new queue count %d (current: %d)", newQueueCount, t.QueueCount)
	}

	now := time.Now()
	for i := newQueueCount; i < t.QueueCount; i++ {
		t.draining[i] = &drainingInfo{DrainStartAt: now}
	}
	atomic.StoreInt32(&t.activeQueueCount, int32(newQueueCount))
	return nil
}

// DrainStatus returns the IDs and info of queues currently draining.
func (t *Topic) DrainStatus() map[int]*drainingInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make(map[int]*drainingInfo, len(t.draining))
	for k, v := range t.draining {
		result[k] = &drainingInfo{DrainStartAt: v.DrainStartAt}
	}
	return result
}

// ForceRebalanceQueue moves all pending messages from the specified draining queue
// to active queues using round-robin distribution.
// If forceRedeliver is true, unacked messages are also force-redelivered first.
// Returns the moved messages grouped by target queue ID.
func (t *Topic) ForceRebalanceQueue(queueID int, forceRedeliver bool) map[int][]*Message {
	t.mu.Lock()
	defer t.mu.Unlock()

	if queueID < 0 || queueID >= len(t.Queues) {
		return nil
	}

	activeCount := int(atomic.LoadInt32(&t.activeQueueCount))
	if activeCount <= 0 {
		return nil
	}

	srcQueue := t.Queues[queueID]
	srcQueue.Lock()

	if forceRedeliver {
		srcQueue.ForceRedeliverAllLocked()
	}

	msgs := srcQueue.AllPendingLocked()
	srcQueue.ClearPendingLocked()
	srcQueue.Unlock()

	if len(msgs) == 0 {
		return nil
	}

	// Distribute to active queues round-robin
	moved := make(map[int][]*Message)
	for i, msg := range msgs {
		targetID := i % activeCount
		msg.QueueID = targetID
		moved[targetID] = append(moved[targetID], msg)
	}

	// Push to target queues
	for targetID, batch := range moved {
		t.Queues[targetID].PushBatch(batch)
	}

	return moved
}

// FinalizeDrain closes a drained queue and removes it from the draining set.
func (t *Topic) FinalizeDrain(queueID int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, ok := t.draining[queueID]; ok {
		t.Queues[queueID].Close()
		delete(t.draining, queueID)
	}
}

// FinalizeResize trims the Queues slice and updates QueueCount after all
// draining queues have been finalized. Returns the new QueueCount and version.
func (t *Topic) FinalizeResize() (int, uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.draining) > 0 {
		return t.QueueCount, t.version
	}

	newCount := int(atomic.LoadInt32(&t.activeQueueCount))
	if newCount < t.QueueCount {
		t.Queues = t.Queues[:newCount]
		t.QueueCount = newCount
	}
	t.version++
	return t.QueueCount, t.version
}

// Rebalance redistributes all pending messages across all active queues
// using round-robin. Returns a map of messageID -> newQueueID for AOF logging.
func (t *Topic) Rebalance() map[string]int {
	t.mu.Lock()
	defer t.mu.Unlock()

	activeCount := int(atomic.LoadInt32(&t.activeQueueCount))
	if activeCount <= 1 {
		return nil
	}

	// Lock all active queues in order to prevent concurrent access
	for i := 0; i < activeCount; i++ {
		t.Queues[i].Lock()
	}
	defer func() {
		for i := 0; i < activeCount; i++ {
			t.Queues[i].Unlock()
		}
	}()

	// Collect all pending messages from active queues
	var allMsgs []*Message
	for i := 0; i < activeCount; i++ {
		msgs := t.Queues[i].AllPendingLocked()
		t.Queues[i].ClearPendingLocked()
		allMsgs = append(allMsgs, msgs...)
	}

	if len(allMsgs) == 0 {
		return nil
	}

	// Redistribute round-robin
	moves := make(map[string]int, len(allMsgs))
	buckets := make([][]*Message, activeCount)
	for i, msg := range allMsgs {
		targetID := i % activeCount
		msg.QueueID = targetID
		moves[msg.ID] = targetID
		buckets[targetID] = append(buckets[targetID], msg)
	}

	// Push to queues (all still locked)
	for i := 0; i < activeCount; i++ {
		if len(buckets[i]) > 0 {
			t.Queues[i].PushBatchLocked(buckets[i])
		}
	}

	return moves
}

// FinalizeResizeTo directly sets the queue count to newCount by removing tail queues.
// This is used during AOF recovery where draining has already completed.
func (t *Topic) FinalizeResizeTo(newCount int) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if newCount <= 0 || newCount >= t.QueueCount {
		return
	}
	// Close tail queues
	for i := newCount; i < t.QueueCount; i++ {
		t.Queues[i].Close()
	}
	t.Queues = t.Queues[:newCount]
	t.QueueCount = newCount
	atomic.StoreInt32(&t.activeQueueCount, int32(newCount))
}

// --- Work stealing ---

// StealFor attempts work stealing for the target queue.
// It finds the queue with the most messages and steals half from its tail.
// Returns: stolen messages, source queue ID, error.
func (t *Topic) StealFor(targetQueueID int) ([]*Message, int, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if targetQueueID < 0 || targetQueueID >= len(t.Queues) {
		return nil, -1, fmt.Errorf("queue %d not found", targetQueueID)
	}

	target := t.Queues[targetQueueID]

	// Find the queue with the most messages
	var source *Queue
	maxLen := 0
	for _, q := range t.Queues {
		if q.ID == targetQueueID {
			continue
		}
		l := q.Len()
		if l > maxLen {
			maxLen = l
			source = q
		}
	}

	if source == nil || maxLen < 2 {
		return nil, -1, nil
	}

	// Lock both queues in ID order to prevent deadlock
	first, second := source, target
	if first.ID > second.ID {
		first, second = second, first
	}
	first.Lock()
	second.Lock()
	defer first.Unlock()
	defer second.Unlock()

	// Re-check after acquiring locks
	if source.LenLocked() < 2 {
		return nil, source.ID, nil
	}

	stolen := source.StealHalfLocked()
	if len(stolen) == 0 {
		return nil, source.ID, nil
	}

	// Update queue IDs for stolen messages
	for _, msg := range stolen {
		msg.QueueID = targetQueueID
	}

	target.PushBatchLocked(stolen)
	return stolen, source.ID, nil
}

// Close stops all queues in this topic.
func (t *Topic) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, q := range t.Queues {
		q.Close()
	}
}
