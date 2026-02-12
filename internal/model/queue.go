package model

import (
	"sync"
	"time"
)

// unackedMessage tracks a consumed but unacknowledged message.
type unackedMessage struct {
	msg        *Message
	consumedAt time.Time
}

// Queue is a thread-safe message queue with ack tracking and redelivery.
type Queue struct {
	mu         sync.Mutex
	ID         int
	pending    []*Message
	unacked    map[string]*unackedMessage
	ackTimeout time.Duration
	stopCh     chan struct{}
}

// NewQueue creates a new queue with the given ID and ack timeout.
// It starts a background goroutine for redelivering timed-out messages.
func NewQueue(id int, ackTimeout time.Duration) *Queue {
	q := &Queue{
		ID:         id,
		pending:    make([]*Message, 0, 256),
		unacked:    make(map[string]*unackedMessage),
		ackTimeout: ackTimeout,
		stopCh:     make(chan struct{}),
	}
	go q.redeliveryLoop()
	return q
}

// Push appends a message to the end of the queue.
func (q *Queue) Push(msg *Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, msg)
}

// PushBatch appends multiple messages to the end of the queue.
func (q *Queue) PushBatch(msgs []*Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, msgs...)
}

// Pop removes and returns the first message from the queue.
// If autoAck is false, the message is tracked as unacked.
func (q *Queue) Pop(autoAck bool) *Message {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.pending) == 0 {
		return nil
	}
	msg := q.pending[0]
	q.pending = q.pending[1:]

	if !autoAck {
		q.unacked[msg.ID] = &unackedMessage{
			msg:        msg,
			consumedAt: time.Now(),
		}
	}
	return msg
}

// PopBatch removes and returns up to count messages from the front of the queue.
func (q *Queue) PopBatch(count int, autoAck bool) []*Message {
	q.mu.Lock()
	defer q.mu.Unlock()

	if len(q.pending) == 0 {
		return nil
	}

	if count <= 0 {
		count = 1
	}
	if count > len(q.pending) {
		count = len(q.pending)
	}

	msgs := make([]*Message, count)
	copy(msgs, q.pending[:count])
	q.pending = q.pending[count:]

	if !autoAck {
		now := time.Now()
		for _, msg := range msgs {
			q.unacked[msg.ID] = &unackedMessage{
				msg:        msg,
				consumedAt: now,
			}
		}
	}

	return msgs
}

// Ack acknowledges messages by their IDs, removing them from the unacked set.
func (q *Queue) Ack(messageIDs []string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, id := range messageIDs {
		delete(q.unacked, id)
	}
}

// Len returns the number of pending messages.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

// UnackedLen returns the number of unacknowledged messages.
func (q *Queue) UnackedLen() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.unacked)
}

// LenLocked returns the number of pending messages. Caller must hold the lock.
func (q *Queue) LenLocked() int {
	return len(q.pending)
}

// StealHalfLocked removes and returns half of the pending messages from the tail.
// Caller must hold the lock.
func (q *Queue) StealHalfLocked() []*Message {
	n := len(q.pending)
	if n < 2 {
		return nil
	}
	half := n / 2
	stolen := make([]*Message, half)
	copy(stolen, q.pending[n-half:])
	q.pending = q.pending[:n-half]
	return stolen
}

// PushBatchLocked appends messages without acquiring the lock. Caller must hold the lock.
func (q *Queue) PushBatchLocked(msgs []*Message) {
	q.pending = append(q.pending, msgs...)
}

// Lock acquires the queue's mutex (for work stealing coordination).
func (q *Queue) Lock() {
	q.mu.Lock()
}

// Unlock releases the queue's mutex.
func (q *Queue) Unlock() {
	q.mu.Unlock()
}

// AllPendingLocked returns a copy of all pending messages. Caller must hold the lock.
func (q *Queue) AllPendingLocked() []*Message {
	result := make([]*Message, len(q.pending))
	copy(result, q.pending)
	return result
}

// ClearPendingLocked removes all pending messages. Caller must hold the lock.
func (q *Queue) ClearPendingLocked() {
	q.pending = q.pending[:0]
}

// AllUnackedLocked returns all unacked messages. Caller must hold the lock.
func (q *Queue) AllUnackedLocked() []*Message {
	result := make([]*Message, 0, len(q.unacked))
	for _, um := range q.unacked {
		result = append(result, um.msg)
	}
	return result
}

// ForceRedeliverAllLocked moves all unacked messages back to pending immediately.
// Caller must hold the lock.
func (q *Queue) ForceRedeliverAllLocked() {
	for id, um := range q.unacked {
		q.pending = append(q.pending, um.msg)
		delete(q.unacked, id)
	}
}

// redeliveryLoop periodically checks for timed-out unacked messages and requeues them.
func (q *Queue) redeliveryLoop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-q.stopCh:
			return
		case <-ticker.C:
			q.redeliverExpired()
		}
	}
}

func (q *Queue) redeliverExpired() {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	for id, um := range q.unacked {
		if now.Sub(um.consumedAt) > q.ackTimeout {
			q.pending = append([]*Message{um.msg}, q.pending...)
			delete(q.unacked, id)
		}
	}
}

// Close stops the redelivery loop.
func (q *Queue) Close() {
	select {
	case <-q.stopCh:
		// already closed
	default:
		close(q.stopCh)
	}
}
