package model

import (
	"time"

	"github.com/google/uuid"
)

// Message represents a single message in the queue.
type Message struct {
	ID        string            `json:"id"`
	Namespace string            `json:"namespace"`
	Topic     string            `json:"topic"`
	QueueID   int               `json:"queue_id"`
	Payload   []byte            `json:"payload"`
	Timestamp int64             `json:"timestamp"`
	Headers   map[string]string `json:"headers,omitempty"`
}

// NewMessage creates a new message with a generated UUID and current timestamp.
func NewMessage(namespace, topic string, queueID int, payload []byte, headers map[string]string) *Message {
	return &Message{
		ID:        uuid.New().String(),
		Namespace: namespace,
		Topic:     topic,
		QueueID:   queueID,
		Payload:   payload,
		Timestamp: time.Now().UnixNano(),
		Headers:   headers,
	}
}

// Clone returns a deep copy of the message.
func (m *Message) Clone() *Message {
	headers := make(map[string]string, len(m.Headers))
	for k, v := range m.Headers {
		headers[k] = v
	}
	payload := make([]byte, len(m.Payload))
	copy(payload, m.Payload)

	return &Message{
		ID:        m.ID,
		Namespace: m.Namespace,
		Topic:     m.Topic,
		QueueID:   m.QueueID,
		Payload:   payload,
		Timestamp: m.Timestamp,
		Headers:   headers,
	}
}
