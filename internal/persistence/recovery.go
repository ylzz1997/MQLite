package persistence

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"mqlite/internal/model"
)

// RecoveryState holds the reconstructed in-memory state after replaying AOF.
type RecoveryState struct {
	Namespaces map[string]*model.Namespace
}

// Recover replays the AOF file and reconstructs the in-memory state.
func Recover(dir, filename string, ackTimeout time.Duration, logger *zap.Logger) (*RecoveryState, error) {
	path := filepath.Join(dir, filename)

	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			logger.Info("no AOF file found, starting fresh")
			return &RecoveryState{Namespaces: make(map[string]*model.Namespace)}, nil
		}
		return nil, fmt.Errorf("open AOF file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, 64*1024)
	state := &RecoveryState{
		Namespaces: make(map[string]*model.Namespace),
	}

	recordCount := 0
	errorCount := 0

	for {
		// Read length (4 bytes)
		var totalLen uint32
		if err := binary.Read(reader, binary.BigEndian, &totalLen); err != nil {
			if err == io.EOF {
				break
			}
			errorCount++
			logger.Warn("AOF read error at record boundary", zap.Error(err))
			break
		}

		if totalLen < 5 { // minimum: 1 byte op + 4 bytes CRC
			errorCount++
			logger.Warn("AOF record too small", zap.Uint32("length", totalLen))
			break
		}

		// Read frame + CRC
		data := make([]byte, totalLen)
		if _, err := io.ReadFull(reader, data); err != nil {
			errorCount++
			logger.Warn("AOF truncated record", zap.Error(err))
			break
		}

		// Verify CRC32
		frameData := data[:len(data)-4]
		expectedCRC := binary.BigEndian.Uint32(data[len(data)-4:])
		actualCRC := crc32.ChecksumIEEE(frameData)
		if expectedCRC != actualCRC {
			errorCount++
			logger.Warn("AOF CRC mismatch, skipping record",
				zap.Uint32("expected", expectedCRC),
				zap.Uint32("actual", actualCRC))
			continue
		}

		opType := frameData[0]
		payload := frameData[1:]

		if err := applyRecord(state, opType, payload, ackTimeout); err != nil {
			errorCount++
			logger.Warn("AOF apply error", zap.Uint8("op", opType), zap.Error(err))
			continue
		}

		recordCount++
	}

	logger.Info("AOF recovery complete",
		zap.Int("records_applied", recordCount),
		zap.Int("errors", errorCount),
		zap.Int("namespaces", len(state.Namespaces)))

	return state, nil
}

func applyRecord(state *RecoveryState, opType uint8, payload []byte, ackTimeout time.Duration) error {
	switch opType {
	case OpCreateNamespace:
		var op CreateNamespaceOp
		if err := json.Unmarshal(payload, &op); err != nil {
			return err
		}
		if _, exists := state.Namespaces[op.Name]; !exists {
			state.Namespaces[op.Name] = model.NewNamespace(op.Name)
		}

	case OpDeleteNamespace:
		var op DeleteNamespaceOp
		if err := json.Unmarshal(payload, &op); err != nil {
			return err
		}
		if ns, exists := state.Namespaces[op.Name]; exists {
			ns.Close()
			delete(state.Namespaces, op.Name)
		}

	case OpCreateTopic:
		var op CreateTopicOp
		if err := json.Unmarshal(payload, &op); err != nil {
			return err
		}
		ns, exists := state.Namespaces[op.Namespace]
		if !exists {
			ns = model.NewNamespace(op.Namespace)
			state.Namespaces[op.Namespace] = ns
		}
		_, _ = ns.CreateTopic(op.Name, op.QueueCount, ackTimeout)

	case OpDeleteTopic:
		var op DeleteTopicOp
		if err := json.Unmarshal(payload, &op); err != nil {
			return err
		}
		if ns, exists := state.Namespaces[op.Namespace]; exists {
			_ = ns.DeleteTopic(op.Name)
		}

	case OpPublish:
		var op PublishOp
		if err := json.Unmarshal(payload, &op); err != nil {
			return err
		}
		ns, exists := state.Namespaces[op.Namespace]
		if !exists {
			return fmt.Errorf("namespace %s not found for publish", op.Namespace)
		}
		topic, err := ns.GetTopic(op.Topic)
		if err != nil {
			return err
		}
		queue, err := topic.GetQueue(op.QueueID)
		if err != nil {
			return err
		}
		msg := &model.Message{
			ID:        op.ID,
			Namespace: op.Namespace,
			Topic:     op.Topic,
			QueueID:   op.QueueID,
			Payload:   op.Payload,
			Timestamp: op.Timestamp,
			Headers:   op.Headers,
		}
		queue.Push(msg)

	case OpAck:
		var op AckOp
		if err := json.Unmarshal(payload, &op); err != nil {
			return err
		}
		// During recovery, ack means the message was consumed and confirmed.
		// We need to remove it from the queue if it's still there.
		ns, exists := state.Namespaces[op.Namespace]
		if !exists {
			return nil // namespace might have been deleted
		}
		topic, err := ns.GetTopic(op.Topic)
		if err != nil {
			return nil // topic might have been deleted
		}
		queue, err := topic.GetQueue(op.QueueID)
		if err != nil {
			return nil
		}
		// Remove acked messages from pending
		queue.Lock()
		removeByIDs(queue, op.MessageIDs)
		queue.Unlock()

	case OpSteal:
		var op StealOp
		if err := json.Unmarshal(payload, &op); err != nil {
			return err
		}
		// Work stealing is already reflected in publish/queue state during recovery.
		// The stolen messages were moved, so we need to replay the move.
		ns, exists := state.Namespaces[op.Namespace]
		if !exists {
			return nil
		}
		topic, err := ns.GetTopic(op.Topic)
		if err != nil {
			return nil
		}
		srcQueue, err := topic.GetQueue(op.SourceQueueID)
		if err != nil {
			return nil
		}
		tgtQueue, err := topic.GetQueue(op.TargetQueueID)
		if err != nil {
			return nil
		}

		// Move messages by ID from source to target
		idSet := make(map[string]bool, len(op.MessageIDs))
		for _, id := range op.MessageIDs {
			idSet[id] = true
		}

		srcQueue.Lock()
		tgtQueue.Lock()
		moveByIDs(srcQueue, tgtQueue, idSet)
		tgtQueue.Unlock()
		srcQueue.Unlock()

	default:
		return fmt.Errorf("unknown operation type: %d", opType)
	}

	return nil
}

// removeByIDs removes messages with the given IDs from the queue's pending list.
// Caller must hold the queue lock.
func removeByIDs(q *model.Queue, ids []string) {
	if len(ids) == 0 {
		return
	}
	idSet := make(map[string]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	pending := q.AllPendingLocked()
	filtered := make([]*model.Message, 0, len(pending))
	for _, msg := range pending {
		if !idSet[msg.ID] {
			filtered = append(filtered, msg)
		}
	}

	// Replace pending — we need direct access, so we use PushBatchLocked after clearing
	// This is a bit hacky but works for recovery
	clearAndReplace(q, filtered)
}

// moveByIDs moves messages matching idSet from src to tgt.
// Caller must hold both locks.
func moveByIDs(src, tgt *model.Queue, idSet map[string]bool) {
	srcPending := src.AllPendingLocked()
	remaining := make([]*model.Message, 0, len(srcPending))
	moved := make([]*model.Message, 0, len(idSet))

	for _, msg := range srcPending {
		if idSet[msg.ID] {
			msg.QueueID = tgt.ID
			moved = append(moved, msg)
		} else {
			remaining = append(remaining, msg)
		}
	}

	clearAndReplace(src, remaining)
	tgt.PushBatchLocked(moved)
}

// clearAndReplace is a helper to replace the pending messages in a queue during recovery.
func clearAndReplace(q *model.Queue, msgs []*model.Message) {
	q.ClearPendingLocked()
	q.PushBatchLocked(msgs)
}
