package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"
	"mqlite/internal/model"
	"mqlite/internal/persistence"
)

// Broker is the core message broker that manages namespaces, topics, and queues.
type Broker struct {
	mu         sync.RWMutex
	namespaces map[string]*model.Namespace
	aof        *persistence.AOFWriter
	rewriter   *persistence.Rewriter
	logger     *zap.Logger
	ackTimeout time.Duration
}

// New creates a new Broker instance.
func New(logger *zap.Logger, aof *persistence.AOFWriter, rewriter *persistence.Rewriter, ackTimeout time.Duration) *Broker {
	return &Broker{
		namespaces: make(map[string]*model.Namespace),
		aof:        aof,
		rewriter:   rewriter,
		logger:     logger,
		ackTimeout: ackTimeout,
	}
}

// LoadState loads a recovered state into the broker (used at startup).
func (b *Broker) LoadState(state *persistence.RecoveryState) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for name, ns := range state.Namespaces {
		b.namespaces[name] = ns
	}
	b.logger.Info("broker state loaded", zap.Int("namespaces", len(state.Namespaces)))
}

// --- Namespace operations ---

func (b *Broker) CreateNamespace(ctx context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.namespaces[name]; exists {
		return fmt.Errorf("namespace %q already exists", name)
	}

	b.namespaces[name] = model.NewNamespace(name)

	if b.aof != nil {
		if err := b.aof.WriteCreateNamespace(name); err != nil {
			b.logger.Error("AOF write failed", zap.Error(err))
		}
	}

	b.logger.Info("namespace created", zap.String("namespace", name))
	return nil
}

func (b *Broker) DeleteNamespace(ctx context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	ns, exists := b.namespaces[name]
	if !exists {
		return fmt.Errorf("namespace %q not found", name)
	}

	ns.Close()
	delete(b.namespaces, name)

	if b.aof != nil {
		if err := b.aof.WriteDeleteNamespace(name); err != nil {
			b.logger.Error("AOF write failed", zap.Error(err))
		}
	}

	b.logger.Info("namespace deleted", zap.String("namespace", name))
	return nil
}

func (b *Broker) ListNamespaces(ctx context.Context) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.namespaces))
	for name := range b.namespaces {
		names = append(names, name)
	}
	return names
}

func (b *Broker) getNamespace(name string) (*model.Namespace, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	ns, ok := b.namespaces[name]
	if !ok {
		return nil, fmt.Errorf("namespace %q not found", name)
	}
	return ns, nil
}

// --- Topic operations ---

func (b *Broker) CreateTopic(ctx context.Context, namespace, name string, queueCount int) error {
	ns, err := b.getNamespace(namespace)
	if err != nil {
		return err
	}

	if _, err := ns.CreateTopic(name, queueCount, b.ackTimeout); err != nil {
		return err
	}

	if b.aof != nil {
		if err := b.aof.WriteCreateTopic(namespace, name, queueCount); err != nil {
			b.logger.Error("AOF write failed", zap.Error(err))
		}
	}

	b.logger.Info("topic created",
		zap.String("namespace", namespace),
		zap.String("topic", name),
		zap.Int("queues", queueCount))
	return nil
}

func (b *Broker) DeleteTopic(ctx context.Context, namespace, name string) error {
	ns, err := b.getNamespace(namespace)
	if err != nil {
		return err
	}

	if err := ns.DeleteTopic(name); err != nil {
		return err
	}

	if b.aof != nil {
		if err := b.aof.WriteDeleteTopic(namespace, name); err != nil {
			b.logger.Error("AOF write failed", zap.Error(err))
		}
	}

	b.logger.Info("topic deleted",
		zap.String("namespace", namespace),
		zap.String("topic", name))
	return nil
}

func (b *Broker) ListTopics(ctx context.Context, namespace string) ([]model.TopicInfo, error) {
	ns, err := b.getNamespace(namespace)
	if err != nil {
		return nil, err
	}
	return ns.ListTopics(), nil
}

// --- Message operations ---

func (b *Broker) Publish(ctx context.Context, req *PublishRequest) (*PublishResponse, error) {
	ns, err := b.getNamespace(req.Namespace)
	if err != nil {
		return nil, err
	}

	topic, err := ns.GetTopic(req.Topic)
	if err != nil {
		return nil, err
	}

	queueID := topic.SelectQueue(req.RoutingKey, req.QueueID)
	msg := model.NewMessage(req.Namespace, req.Topic, queueID, req.Payload, req.Headers)

	queue, err := topic.GetQueue(queueID)
	if err != nil {
		return nil, err
	}

	queue.Push(msg)

	if b.aof != nil {
		if err := b.aof.WritePublish(msg); err != nil {
			b.logger.Error("AOF write failed", zap.Error(err))
		}
	}

	return &PublishResponse{
		MessageID: msg.ID,
		QueueID:   queueID,
	}, nil
}

func (b *Broker) Consume(ctx context.Context, req *ConsumeRequest) ([]*model.Message, error) {
	ns, err := b.getNamespace(req.Namespace)
	if err != nil {
		return nil, err
	}

	topic, err := ns.GetTopic(req.Topic)
	if err != nil {
		return nil, err
	}

	queue, err := topic.GetQueue(req.QueueID)
	if err != nil {
		return nil, err
	}

	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}

	msgs := queue.PopBatch(batchSize, req.AutoAck)

	// Work stealing: if queue is empty, try to steal
	if len(msgs) == 0 {
		stolen, sourceID, stealErr := topic.StealFor(req.QueueID)
		if stealErr != nil {
			b.logger.Warn("work stealing failed", zap.Error(stealErr))
		} else if len(stolen) > 0 {
			b.logger.Info("work stealing succeeded",
				zap.String("topic", req.Topic),
				zap.Int("source", sourceID),
				zap.Int("target", req.QueueID),
				zap.Int("stolen", len(stolen)))

			if b.aof != nil {
				ids := make([]string, len(stolen))
				for i, m := range stolen {
					ids[i] = m.ID
				}
				if err := b.aof.WriteSteal(req.Namespace, req.Topic, sourceID, req.QueueID, ids); err != nil {
					b.logger.Error("AOF write steal failed", zap.Error(err))
				}
			}

			// Retry consume after stealing
			msgs = queue.PopBatch(batchSize, req.AutoAck)
		}
	}

	// Record auto-ack in AOF
	if req.AutoAck && b.aof != nil && len(msgs) > 0 {
		ids := make([]string, len(msgs))
		for i, m := range msgs {
			ids[i] = m.ID
		}
		if err := b.aof.WriteAck(req.Namespace, req.Topic, req.QueueID, ids); err != nil {
			b.logger.Error("AOF write ack failed", zap.Error(err))
		}
	}

	return msgs, nil
}

func (b *Broker) Subscribe(ctx context.Context, req *SubscribeRequest, msgCh chan<- *model.Message) error {
	ns, err := b.getNamespace(req.Namespace)
	if err != nil {
		return err
	}

	topic, err := ns.GetTopic(req.Topic)
	if err != nil {
		return err
	}

	queue, err := topic.GetQueue(req.QueueID)
	if err != nil {
		return err
	}

	go func() {
		defer close(msgCh)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				msg := queue.Pop(req.AutoAck)
				if msg == nil {
					// Try work stealing
					stolen, sourceID, stealErr := topic.StealFor(req.QueueID)
					if stealErr == nil && len(stolen) > 0 {
						b.logger.Debug("subscription work stealing",
							zap.Int("source", sourceID),
							zap.Int("target", req.QueueID),
							zap.Int("stolen", len(stolen)))

						if b.aof != nil {
							ids := make([]string, len(stolen))
							for i, m := range stolen {
								ids[i] = m.ID
							}
							_ = b.aof.WriteSteal(req.Namespace, req.Topic, sourceID, req.QueueID, ids)
						}
						msg = queue.Pop(req.AutoAck)
					}
				}
				if msg != nil {
					if req.AutoAck && b.aof != nil {
						_ = b.aof.WriteAck(req.Namespace, req.Topic, req.QueueID, []string{msg.ID})
					}
					select {
					case msgCh <- msg:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}()

	return nil
}

func (b *Broker) Ack(ctx context.Context, namespace, topic string, queueID int, messageIDs []string) error {
	ns, err := b.getNamespace(namespace)
	if err != nil {
		return err
	}

	t, err := ns.GetTopic(topic)
	if err != nil {
		return err
	}

	q, err := t.GetQueue(queueID)
	if err != nil {
		return err
	}

	q.Ack(messageIDs)

	if b.aof != nil {
		if err := b.aof.WriteAck(namespace, topic, queueID, messageIDs); err != nil {
			b.logger.Error("AOF write ack failed", zap.Error(err))
		}
	}

	return nil
}

// --- Rewrite ---

// TriggerRewrite starts an AOF rewrite if conditions are met.
func (b *Broker) TriggerRewrite() {
	if b.aof == nil || b.rewriter == nil {
		return
	}
	if !b.aof.NeedsRewrite() {
		return
	}

	b.mu.RLock()
	snapshot := make(map[string]*model.Namespace, len(b.namespaces))
	for k, v := range b.namespaces {
		snapshot[k] = v
	}
	b.mu.RUnlock()

	go func() {
		tmpPath, newSize, err := b.rewriter.Rewrite(snapshot)
		if err != nil {
			b.logger.Error("AOF rewrite failed", zap.Error(err))
			return
		}
		if err := b.aof.ReplaceFile(tmpPath); err != nil {
			b.logger.Error("AOF replace failed", zap.Error(err))
			return
		}
		b.aof.MarkRewritten(newSize)
		b.logger.Info("AOF rewrite completed", zap.Int64("new_size", newSize))
	}()
}

// --- Lifecycle ---

func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, ns := range b.namespaces {
		ns.Close()
	}

	if b.aof != nil {
		return b.aof.Close()
	}
	return nil
}
