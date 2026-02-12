package persistence

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"time"

	"go.uber.org/zap"
	"mqlite/internal/model"
)

// Operation types for AOF records.
const (
	OpPublish         uint8 = 1
	OpAck             uint8 = 2
	OpCreateNamespace uint8 = 3
	OpCreateTopic     uint8 = 4
	OpSteal           uint8 = 5
	OpDeleteNamespace uint8 = 6
	OpDeleteTopic     uint8 = 7
	OpResizeTopic     uint8 = 8
	OpRebalance       uint8 = 9
)

// AOFRecord is the envelope for a single AOF entry.
type AOFRecord struct {
	OpType    uint8       `json:"op"`
	Timestamp int64       `json:"ts"`
	Data      interface{} `json:"data"`
}

// --- Operation payloads ---

type PublishOp struct {
	ID        string            `json:"id"`
	Namespace string            `json:"ns"`
	Topic     string            `json:"topic"`
	QueueID   int               `json:"qid"`
	Payload   []byte            `json:"payload"`
	Timestamp int64             `json:"ts"`
	Headers   map[string]string `json:"headers,omitempty"`
}

type AckOp struct {
	Namespace  string   `json:"ns"`
	Topic      string   `json:"topic"`
	QueueID    int      `json:"qid"`
	MessageIDs []string `json:"ids"`
}

type CreateNamespaceOp struct {
	Name string `json:"name"`
}

type CreateTopicOp struct {
	Namespace  string `json:"ns"`
	Name       string `json:"name"`
	QueueCount int    `json:"qcount"`
}

type StealOp struct {
	Namespace     string   `json:"ns"`
	Topic         string   `json:"topic"`
	SourceQueueID int      `json:"src_qid"`
	TargetQueueID int      `json:"tgt_qid"`
	MessageIDs    []string `json:"ids"`
}

type DeleteNamespaceOp struct {
	Name string `json:"name"`
}

type DeleteTopicOp struct {
	Namespace string `json:"ns"`
	Name      string `json:"name"`
}

type ResizeTopicOp struct {
	Namespace     string `json:"ns"`
	Name          string `json:"name"`
	NewQueueCount int    `json:"new_qcount"`
	Version       uint64 `json:"version"`
}

type RebalanceOp struct {
	Namespace string         `json:"ns"`
	Topic     string         `json:"topic"`
	Moves     map[string]int `json:"moves"` // messageID -> newQueueID
}

// ==========================================================
// AOFWriter
// ==========================================================

// AOFWriter appends operation records to an AOF file.
type AOFWriter struct {
	mu     sync.Mutex
	file   *os.File
	writer *bufio.Writer
	fsync  string // "always" | "everysec" | "no"
	logger *zap.Logger
	stopCh chan struct{}

	// for rewrite triggering
	currentSize  int64
	lastRewriteSize int64
	rewriteMinSize  int64
	rewritePercent  int
}

// NewAOFWriter opens (or creates) the AOF file and returns a writer.
func NewAOFWriter(dir, filename, fsync string, rewriteMinSize int64, rewritePercent int, logger *zap.Logger) (*AOFWriter, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, filename)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	info, _ := file.Stat()
	currentSize := int64(0)
	if info != nil {
		currentSize = info.Size()
	}

	w := &AOFWriter{
		file:            file,
		writer:          bufio.NewWriterSize(file, 64*1024), // 64KB buffer
		fsync:           fsync,
		logger:          logger,
		stopCh:          make(chan struct{}),
		currentSize:     currentSize,
		lastRewriteSize: currentSize,
		rewriteMinSize:  rewriteMinSize,
		rewritePercent:  rewritePercent,
	}

	if fsync == "everysec" {
		go w.periodicSync()
	}

	return w, nil
}

// writeRecord encodes and writes a single AOF record.
// Frame format: [4B length][1B opType][JSON payload][4B CRC32]
func (w *AOFWriter) writeRecord(opType uint8, data interface{}) error {
	payload, err := json.Marshal(data)
	if err != nil {
		return err
	}

	// Build frame: opType(1) + payload(N)
	frame := make([]byte, 1+len(payload))
	frame[0] = opType
	copy(frame[1:], payload)

	// Calculate CRC32
	checksum := crc32.ChecksumIEEE(frame)

	w.mu.Lock()
	defer w.mu.Unlock()

	// Write length (4 bytes, big-endian) — length of frame + CRC
	totalLen := uint32(len(frame) + 4)
	if err := binary.Write(w.writer, binary.BigEndian, totalLen); err != nil {
		return err
	}

	// Write frame
	if _, err := w.writer.Write(frame); err != nil {
		return err
	}

	// Write CRC32
	if err := binary.Write(w.writer, binary.BigEndian, checksum); err != nil {
		return err
	}

	w.currentSize += int64(4 + len(frame) + 4)

	// fsync policy
	if w.fsync == "always" {
		if err := w.writer.Flush(); err != nil {
			return err
		}
		return w.file.Sync()
	}

	return nil
}

func (w *AOFWriter) periodicSync() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.mu.Lock()
			_ = w.writer.Flush()
			_ = w.file.Sync()
			w.mu.Unlock()
		}
	}
}

// --- Public write methods ---

func (w *AOFWriter) WritePublish(msg *model.Message) error {
	return w.writeRecord(OpPublish, &PublishOp{
		ID:        msg.ID,
		Namespace: msg.Namespace,
		Topic:     msg.Topic,
		QueueID:   msg.QueueID,
		Payload:   msg.Payload,
		Timestamp: msg.Timestamp,
		Headers:   msg.Headers,
	})
}

func (w *AOFWriter) WriteAck(namespace, topic string, queueID int, messageIDs []string) error {
	return w.writeRecord(OpAck, &AckOp{
		Namespace:  namespace,
		Topic:      topic,
		QueueID:    queueID,
		MessageIDs: messageIDs,
	})
}

func (w *AOFWriter) WriteCreateNamespace(name string) error {
	return w.writeRecord(OpCreateNamespace, &CreateNamespaceOp{Name: name})
}

func (w *AOFWriter) WriteCreateTopic(namespace, name string, queueCount int) error {
	return w.writeRecord(OpCreateTopic, &CreateTopicOp{
		Namespace:  namespace,
		Name:       name,
		QueueCount: queueCount,
	})
}

func (w *AOFWriter) WriteSteal(namespace, topic string, sourceQueueID, targetQueueID int, messageIDs []string) error {
	return w.writeRecord(OpSteal, &StealOp{
		Namespace:     namespace,
		Topic:         topic,
		SourceQueueID: sourceQueueID,
		TargetQueueID: targetQueueID,
		MessageIDs:    messageIDs,
	})
}

func (w *AOFWriter) WriteDeleteNamespace(name string) error {
	return w.writeRecord(OpDeleteNamespace, &DeleteNamespaceOp{Name: name})
}

func (w *AOFWriter) WriteDeleteTopic(namespace, name string) error {
	return w.writeRecord(OpDeleteTopic, &DeleteTopicOp{
		Namespace: namespace,
		Name:      name,
	})
}

func (w *AOFWriter) WriteResizeTopic(namespace, name string, newQueueCount int, version uint64) error {
	return w.writeRecord(OpResizeTopic, &ResizeTopicOp{
		Namespace:     namespace,
		Name:          name,
		NewQueueCount: newQueueCount,
		Version:       version,
	})
}

func (w *AOFWriter) WriteRebalance(namespace, topic string, moves map[string]int) error {
	return w.writeRecord(OpRebalance, &RebalanceOp{
		Namespace: namespace,
		Topic:     topic,
		Moves:     moves,
	})
}

// NeedsRewrite checks if the AOF file should be rewritten.
func (w *AOFWriter) NeedsRewrite() bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.currentSize < w.rewriteMinSize {
		return false
	}
	if w.lastRewriteSize == 0 {
		return true
	}
	growth := float64(w.currentSize-w.lastRewriteSize) / float64(w.lastRewriteSize) * 100
	return growth >= float64(w.rewritePercent)
}

// MarkRewritten records the current size as the baseline for future rewrite checks.
func (w *AOFWriter) MarkRewritten(newSize int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastRewriteSize = newSize
	w.currentSize = newSize
}

// FilePath returns the full path to the AOF file.
func (w *AOFWriter) FilePath() string {
	return w.file.Name()
}

// ReplaceFile atomically replaces the AOF file with a new one.
func (w *AOFWriter) ReplaceFile(newPath string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	_ = w.writer.Flush()
	oldPath := w.file.Name()
	_ = w.file.Close()

	if err := os.Rename(newPath, oldPath); err != nil {
		return err
	}

	file, err := os.OpenFile(oldPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	w.file = file
	w.writer = bufio.NewWriterSize(file, 64*1024)

	info, _ := file.Stat()
	if info != nil {
		w.currentSize = info.Size()
		w.lastRewriteSize = info.Size()
	}

	return nil
}

// Close flushes and closes the AOF file.
func (w *AOFWriter) Close() error {
	select {
	case <-w.stopCh:
	default:
		close(w.stopCh)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}
