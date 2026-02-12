package model

import (
	"fmt"
	"sync"
	"time"
)

// TopicInfo provides summary information about a topic.
type TopicInfo struct {
	Name       string `json:"name"`
	QueueCount int    `json:"queue_count"`
	Version    uint64 `json:"version"`
}

// Namespace is the top-level isolation domain containing multiple topics.
type Namespace struct {
	mu     sync.RWMutex
	Name   string
	Topics map[string]*Topic
}

// NewNamespace creates a new namespace.
func NewNamespace(name string) *Namespace {
	return &Namespace{
		Name:   name,
		Topics: make(map[string]*Topic),
	}
}

// CreateTopic creates a new topic within this namespace.
func (ns *Namespace) CreateTopic(name string, queueCount int, ackTimeout time.Duration) (*Topic, error) {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	if _, exists := ns.Topics[name]; exists {
		return nil, fmt.Errorf("topic %s already exists in namespace %s", name, ns.Name)
	}

	topic := NewTopic(ns.Name, name, queueCount, ackTimeout)
	ns.Topics[name] = topic
	return topic, nil
}

// GetTopic returns a topic by name.
func (ns *Namespace) GetTopic(name string) (*Topic, error) {
	ns.mu.RLock()
	defer ns.mu.RUnlock()

	topic, exists := ns.Topics[name]
	if !exists {
		return nil, fmt.Errorf("topic %s not found in namespace %s", name, ns.Name)
	}
	return topic, nil
}

// DeleteTopic removes a topic from this namespace.
func (ns *Namespace) DeleteTopic(name string) error {
	ns.mu.Lock()
	defer ns.mu.Unlock()

	topic, exists := ns.Topics[name]
	if !exists {
		return fmt.Errorf("topic %s not found in namespace %s", name, ns.Name)
	}
	topic.Close()
	delete(ns.Topics, name)
	return nil
}

// ListTopics returns summary info for all topics in this namespace.
func (ns *Namespace) ListTopics() []TopicInfo {
	ns.mu.RLock()
	defer ns.mu.RUnlock()

	infos := make([]TopicInfo, 0, len(ns.Topics))
	for _, topic := range ns.Topics {
		infos = append(infos, TopicInfo{
			Name:       topic.Name,
			QueueCount: topic.QueueCount,
			Version:    topic.Version(),
		})
	}
	return infos
}

// Close stops all topics in this namespace.
func (ns *Namespace) Close() {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	for _, topic := range ns.Topics {
		topic.Close()
	}
}
