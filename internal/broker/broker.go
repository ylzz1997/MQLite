package broker

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"
	"mqlite/internal/model"
	"mqlite/internal/persistence"
)

// Broker is the core message broker that manages namespaces, topics, and queues.
type Broker struct {
	mu           sync.RWMutex
	namespaces   map[string]*model.Namespace
	aof          *persistence.AOFWriter
	rewriter     *persistence.Rewriter
	logger       *zap.Logger
	ackTimeout   time.Duration
	drainTimeout time.Duration
}

// New creates a new Broker instance.
func New(logger *zap.Logger, aof *persistence.AOFWriter, rewriter *persistence.Rewriter, ackTimeout, drainTimeout time.Duration) *Broker {
	return &Broker{
		namespaces:   make(map[string]*model.Namespace),
		aof:          aof,
		rewriter:     rewriter,
		logger:       logger,
		ackTimeout:   ackTimeout,
		drainTimeout: drainTimeout,
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

		lastVersion := topic.Version()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				// Check for topic resize events and notify consumer
				currentVersion := topic.Version()
				if currentVersion != lastVersion {
					resizeMsg := &model.Message{
						ID:        fmt.Sprintf("_resize_%d", currentVersion),
						Namespace: req.Namespace,
						Topic:     req.Topic,
						QueueID:   -1,
						Timestamp: time.Now().UnixNano(),
						Headers: map[string]string{
							"_event":       "topic_resized",
							"_queue_count": strconv.Itoa(topic.ActiveQueueCount()),
							"_version":     strconv.FormatUint(currentVersion, 10),
						},
					}
					select {
					case msgCh <- resizeMsg:
					case <-ctx.Done():
						return
					}
					lastVersion = currentVersion
				}

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

// --- Resize / Rebalance ---

func (b *Broker) ResizeTopic(ctx context.Context, namespace, name string, newQueueCount int) (*ResizeTopicResponse, error) {
	ns, err := b.getNamespace(namespace)
	if err != nil {
		return nil, err
	}

	topic, err := ns.GetTopic(name)
	if err != nil {
		return nil, err
	}

	currentCount := topic.QueueCount

	if newQueueCount == currentCount {
		return &ResizeTopicResponse{
			NewQueueCount: currentCount,
			Version:       topic.Version(),
		}, nil
	}

	if newQueueCount <= 0 {
		return nil, fmt.Errorf("new queue count must be > 0, got %d", newQueueCount)
	}

	if newQueueCount > currentCount {
		// Scale out: synchronous
		count := newQueueCount - currentCount
		newCount, version := topic.AddQueues(count)

		if b.aof != nil {
			if err := b.aof.WriteResizeTopic(namespace, name, newCount, version); err != nil {
				b.logger.Error("AOF write resize topic failed", zap.Error(err))
			}
		}

		b.logger.Info("topic scaled out",
			zap.String("namespace", namespace),
			zap.String("topic", name),
			zap.Int("old_queues", currentCount),
			zap.Int("new_queues", newCount),
			zap.Uint64("version", version))

		return &ResizeTopicResponse{
			NewQueueCount: newCount,
			Version:       version,
		}, nil
	}

	// Scale in: mark draining and start async drain loop
	if err := topic.MarkDraining(newQueueCount); err != nil {
		return nil, err
	}

	b.logger.Info("topic scale-in started (draining)",
		zap.String("namespace", namespace),
		zap.String("topic", name),
		zap.Int("old_queues", currentCount),
		zap.Int("target_queues", newQueueCount))

	go b.runDrainLoop(namespace, name, topic)

	return &ResizeTopicResponse{
		NewQueueCount: newQueueCount,
		Version:       topic.Version(),
		Draining:      true,
	}, nil
}

func (b *Broker) RebalanceTopic(ctx context.Context, namespace, name string) error {
	ns, err := b.getNamespace(namespace)
	if err != nil {
		return err
	}

	topic, err := ns.GetTopic(name)
	if err != nil {
		return err
	}

	if topic.IsDraining() {
		return fmt.Errorf("cannot rebalance topic %s while draining is in progress", name)
	}

	moves := topic.Rebalance()
	if len(moves) == 0 {
		b.logger.Info("rebalance: no messages to move",
			zap.String("namespace", namespace),
			zap.String("topic", name))
		return nil
	}

	if b.aof != nil {
		if err := b.aof.WriteRebalance(namespace, name, moves); err != nil {
			b.logger.Error("AOF write rebalance failed", zap.Error(err))
		}
	}

	b.logger.Info("topic rebalanced",
		zap.String("namespace", namespace),
		zap.String("topic", name),
		zap.Int("messages_moved", len(moves)))

	return nil
}

// runDrainLoop periodically checks draining queues and handles force-rebalance.
func (b *Broker) runDrainLoop(namespace, topicName string, topic *model.Topic) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		status := topic.DrainStatus()
		if len(status) == 0 {
			break
		}

		for queueID, info := range status {
			queue, err := topic.GetQueue(queueID)
			if err != nil {
				continue
			}

			pendingLen := queue.Len()
			unackedLen := queue.UnackedLen()
			timedOut := b.drainTimeout > 0 && time.Since(info.DrainStartAt) > b.drainTimeout

			if pendingLen == 0 && unackedLen == 0 {
				// Completely drained
				topic.FinalizeDrain(queueID)
				b.logger.Info("queue drained",
					zap.String("namespace", namespace),
					zap.String("topic", topicName),
					zap.Int("queue", queueID))
				continue
			}

			if timedOut {
				// Timeout: force redeliver unacked + rebalance all
				moved := topic.ForceRebalanceQueue(queueID, true)
				b.logRebalanceMoves(namespace, topicName, queueID, moved)
				topic.FinalizeDrain(queueID)
				b.logger.Info("queue force-rebalanced (timeout)",
					zap.String("namespace", namespace),
					zap.String("topic", topicName),
					zap.Int("queue", queueID),
					zap.Duration("elapsed", time.Since(info.DrainStartAt)))
				continue
			}

			if pendingLen > 0 && pendingLen <= 1 {
				// Single message fast path: force rebalance pending
				moved := topic.ForceRebalanceQueue(queueID, false)
				b.logRebalanceMoves(namespace, topicName, queueID, moved)
				if queue.UnackedLen() == 0 {
					topic.FinalizeDrain(queueID)
					b.logger.Info("queue force-rebalanced (single message)",
						zap.String("namespace", namespace),
						zap.String("topic", topicName),
						zap.Int("queue", queueID))
				}
				continue
			}

			// else: pending > 1 or (pending == 0 && unacked > 0)
			// Wait for natural drain or ack timeout
		}

		// Check if all draining is complete
		if !topic.IsDraining() {
			newCount, version := topic.FinalizeResize()

			if b.aof != nil {
				if err := b.aof.WriteResizeTopic(namespace, topicName, newCount, version); err != nil {
					b.logger.Error("AOF write resize topic failed", zap.Error(err))
				}
			}

			b.logger.Info("topic scale-in completed",
				zap.String("namespace", namespace),
				zap.String("topic", topicName),
				zap.Int("new_queue_count", newCount),
				zap.Uint64("version", version))
			break
		}
	}
}

// logRebalanceMoves logs force-rebalanced message moves to AOF.
func (b *Broker) logRebalanceMoves(namespace, topicName string, srcQueueID int, moved map[int][]*model.Message) {
	if b.aof == nil || len(moved) == 0 {
		return
	}

	for targetID, msgs := range moved {
		ids := make([]string, len(msgs))
		for i, m := range msgs {
			ids[i] = m.ID
		}
		if err := b.aof.WriteSteal(namespace, topicName, srcQueueID, targetID, ids); err != nil {
			b.logger.Error("AOF write steal (drain) failed", zap.Error(err))
		}
	}
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
