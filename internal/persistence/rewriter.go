package persistence

import (
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"mqlite/internal/model"
)

// Rewriter handles AOF compaction by writing a minimal command set
// that reproduces the current in-memory state.
type Rewriter struct {
	dir      string
	filename string
	logger   *zap.Logger
}

// NewRewriter creates a new AOF rewriter.
func NewRewriter(dir, filename string, logger *zap.Logger) *Rewriter {
	return &Rewriter{
		dir:      dir,
		filename: filename,
		logger:   logger,
	}
}

// Rewrite generates a new minimal AOF file from the current in-memory state.
// It writes to a temporary file, then the caller can atomically replace the old AOF.
func (r *Rewriter) Rewrite(namespaces map[string]*model.Namespace) (string, int64, error) {
	tmpPath := filepath.Join(r.dir, r.filename+".rewrite.tmp")

	// Create a temporary AOF writer
	tmpFile, err := os.Create(tmpPath)
	if err != nil {
		return "", 0, err
	}
	tmpFile.Close()

	tmpWriter, err := NewAOFWriter(r.dir, r.filename+".rewrite.tmp", "always", 0, 0, r.logger)
	if err != nil {
		return "", 0, err
	}
	defer tmpWriter.Close()

	recordCount := 0

	for nsName, ns := range namespaces {
		// Write CreateNamespace
		if err := tmpWriter.WriteCreateNamespace(nsName); err != nil {
			r.logger.Error("rewrite: failed to write namespace", zap.String("ns", nsName), zap.Error(err))
			continue
		}
		recordCount++

		topics := ns.ListTopics()
		for _, topicInfo := range topics {
			// Write CreateTopic
			if err := tmpWriter.WriteCreateTopic(nsName, topicInfo.Name, topicInfo.QueueCount); err != nil {
				r.logger.Error("rewrite: failed to write topic", zap.String("topic", topicInfo.Name), zap.Error(err))
				continue
			}
			recordCount++

			// Write all pending messages in each queue
			topic, err := ns.GetTopic(topicInfo.Name)
			if err != nil {
				continue
			}

			for i := 0; i < topicInfo.QueueCount; i++ {
				queue, err := topic.GetQueue(i)
				if err != nil {
					continue
				}

				queue.Lock()
				// Write pending messages
				pending := queue.AllPendingLocked()
				for _, msg := range pending {
					if err := tmpWriter.WritePublish(msg); err != nil {
						r.logger.Error("rewrite: failed to write message", zap.Error(err))
					} else {
						recordCount++
					}
				}
				// Write unacked messages (they should be re-published so they exist on recovery)
				unacked := queue.AllUnackedLocked()
				for _, msg := range unacked {
					if err := tmpWriter.WritePublish(msg); err != nil {
						r.logger.Error("rewrite: failed to write unacked message", zap.Error(err))
					} else {
						recordCount++
					}
				}
				queue.Unlock()
			}
		}
	}

	r.logger.Info("AOF rewrite complete",
		zap.Int("records", recordCount),
		zap.String("tmp_path", tmpPath))

	info, err := os.Stat(tmpPath)
	newSize := int64(0)
	if err == nil {
		newSize = info.Size()
	}

	return tmpPath, newSize, nil
}
