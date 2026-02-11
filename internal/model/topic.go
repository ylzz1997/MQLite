package model

import (
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"
)

// Topic manages multiple queues and coordinates message distribution.
type Topic struct {
	mu         sync.RWMutex
	Name       string
	Namespace  string
	Queues     []*Queue
	QueueCount int
	robin      uint64 // for round-robin distribution
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
		Name:       name,
		Namespace:  namespace,
		Queues:     queues,
		QueueCount: queueCount,
	}
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
//   - queueID >= 0: direct routing
//   - routingKey != "": hash-based routing
//   - otherwise: round-robin
func (t *Topic) SelectQueue(routingKey string, queueID int) int {
	if queueID >= 0 && queueID < t.QueueCount {
		return queueID
	}
	if routingKey != "" {
		h := fnv.New32a()
		h.Write([]byte(routingKey))
		return int(h.Sum32()) % t.QueueCount
	}
	n := atomic.AddUint64(&t.robin, 1)
	return int(n-1) % t.QueueCount
}

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
