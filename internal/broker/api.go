package broker

import (
	"context"

	"mqlite/internal/model"
)

// API defines the unified interface for all protocol servers.
type API interface {
	// Namespace operations
	CreateNamespace(ctx context.Context, name string) error
	DeleteNamespace(ctx context.Context, name string) error
	ListNamespaces(ctx context.Context) []string

	// Topic operations
	CreateTopic(ctx context.Context, namespace, name string, queueCount int) error
	DeleteTopic(ctx context.Context, namespace, name string) error
	ListTopics(ctx context.Context, namespace string) ([]model.TopicInfo, error)

	// Message operations
	Publish(ctx context.Context, req *PublishRequest) (*PublishResponse, error)
	Consume(ctx context.Context, req *ConsumeRequest) ([]*model.Message, error)
	Subscribe(ctx context.Context, req *SubscribeRequest, msgCh chan<- *model.Message) error
	Ack(ctx context.Context, namespace, topic string, queueID int, messageIDs []string) error

	// Lifecycle
	Close() error
}

// PublishRequest contains parameters for publishing a message.
type PublishRequest struct {
	Namespace  string
	Topic      string
	Payload    []byte
	Headers    map[string]string
	RoutingKey string
	QueueID    int // -1 for auto routing
}

// PublishResponse contains the result of a publish operation.
type PublishResponse struct {
	MessageID string
	QueueID   int
}

// ConsumeRequest contains parameters for consuming messages.
type ConsumeRequest struct {
	Namespace string
	Topic     string
	QueueID   int
	BatchSize int
	AutoAck   bool
}

// SubscribeRequest contains parameters for subscribing to messages.
type SubscribeRequest struct {
	Namespace string
	Topic     string
	QueueID   int
	AutoAck   bool
}
