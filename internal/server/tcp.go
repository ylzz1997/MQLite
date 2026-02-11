package server

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"

	"go.uber.org/zap"

	"mqlite/internal/broker"
	"mqlite/internal/model"
)

// TCP frame format:
// [4 bytes: payload length (big-endian)]
// [1 byte: encoding flag - 0=JSON, 1=Protobuf]
// [N bytes: payload]

const (
	EncodingJSON     byte = 0
	EncodingProtobuf byte = 1
)

// TCPCommand is the JSON command structure for TCP protocol.
type TCPCommand struct {
	Action string          `json:"action"`
	Data   json.RawMessage `json:"data"`
}

// TCPResponse is the JSON response structure for TCP protocol.
type TCPResponse struct {
	Status string      `json:"status"` // "ok" or "error"
	Data   interface{} `json:"data,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// --- Command data types ---

type tcpCreateNamespaceData struct {
	Name string `json:"name"`
}

type tcpCreateTopicData struct {
	Namespace  string `json:"namespace"`
	Name       string `json:"name"`
	QueueCount int    `json:"queue_count"`
}

type tcpDeleteNamespaceData struct {
	Name string `json:"name"`
}

type tcpDeleteTopicData struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

type tcpListTopicsData struct {
	Namespace string `json:"namespace"`
}

type tcpPublishData struct {
	Namespace  string            `json:"namespace"`
	Topic      string            `json:"topic"`
	Payload    json.RawMessage   `json:"payload"`
	Headers    map[string]string `json:"headers,omitempty"`
	RoutingKey string            `json:"routing_key,omitempty"`
	QueueID    *int              `json:"queue_id,omitempty"` // nil = auto (-1)
}

type tcpConsumeData struct {
	Namespace string `json:"namespace"`
	Topic     string `json:"topic"`
	QueueID   int    `json:"queue_id"`
	BatchSize int    `json:"batch_size"`
	AutoAck   bool   `json:"auto_ack"`
}

type tcpSubscribeData struct {
	Namespace string `json:"namespace"`
	Topic     string `json:"topic"`
	QueueID   int    `json:"queue_id"`
	AutoAck   bool   `json:"auto_ack"`
}

type tcpAckData struct {
	Namespace  string   `json:"namespace"`
	Topic      string   `json:"topic"`
	QueueID    int      `json:"queue_id"`
	MessageIDs []string `json:"message_ids"`
}

// ==========================================================
// TCPServer
// ==========================================================

// TCPServer provides a raw TCP interface with custom framing.
type TCPServer struct {
	broker   *broker.Broker
	listener net.Listener
	logger   *zap.Logger
	port     int
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewTCPServer creates a new TCP server.
func NewTCPServer(b *broker.Broker, port int, logger *zap.Logger) *TCPServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &TCPServer{
		broker: b,
		logger: logger,
		port:   port,
		ctx:    ctx,
		cancel: cancel,
	}
}

// Start begins listening for TCP connections.
func (s *TCPServer) Start() error {
	var err error
	s.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("TCP listen: %w", err)
	}

	s.logger.Info("TCP server starting", zap.Int("port", s.port))

	go s.acceptLoop()
	return nil
}

// Stop gracefully shuts down the TCP server.
func (s *TCPServer) Stop() {
	s.cancel()
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
}

func (s *TCPServer) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.ctx.Done():
				return
			default:
				s.logger.Error("TCP accept error", zap.Error(err))
				continue
			}
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConnection(conn)
		}()
	}
}

func (s *TCPServer) handleConnection(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReaderSize(conn, 64*1024)
	writer := bufio.NewWriterSize(conn, 64*1024)

	s.logger.Debug("TCP client connected", zap.String("remote", conn.RemoteAddr().String()))

	for {
		select {
		case <-s.ctx.Done():
			return
		default:
		}

		// Read frame
		cmd, encoding, err := readFrame(reader)
		if err != nil {
			if err != io.EOF {
				s.logger.Debug("TCP read error", zap.Error(err))
			}
			return
		}

		if encoding != EncodingJSON {
			// For now, only JSON is supported for TCP commands
			s.writeResponse(writer, encoding, &TCPResponse{
				Status: "error",
				Error:  "only JSON encoding is supported for TCP",
			})
			continue
		}

		// Parse command
		var tcpCmd TCPCommand
		if err := json.Unmarshal(cmd, &tcpCmd); err != nil {
			s.writeResponse(writer, encoding, &TCPResponse{
				Status: "error",
				Error:  "invalid command: " + err.Error(),
			})
			continue
		}

		// Handle command
		resp := s.handleCommand(conn, tcpCmd, encoding, writer)
		if resp != nil {
			s.writeResponse(writer, encoding, resp)
		}
	}
}

func (s *TCPServer) handleCommand(conn net.Conn, cmd TCPCommand, encoding byte, writer *bufio.Writer) *TCPResponse {
	ctx := s.ctx

	switch cmd.Action {
	case "create_namespace":
		var data tcpCreateNamespaceData
		if err := json.Unmarshal(cmd.Data, &data); err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		if err := s.broker.CreateNamespace(ctx, data.Name); err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		return &TCPResponse{Status: "ok", Data: "namespace created"}

	case "delete_namespace":
		var data tcpDeleteNamespaceData
		if err := json.Unmarshal(cmd.Data, &data); err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		if err := s.broker.DeleteNamespace(ctx, data.Name); err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		return &TCPResponse{Status: "ok", Data: "namespace deleted"}

	case "list_namespaces":
		names := s.broker.ListNamespaces(ctx)
		return &TCPResponse{Status: "ok", Data: names}

	case "create_topic":
		var data tcpCreateTopicData
		if err := json.Unmarshal(cmd.Data, &data); err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		if err := s.broker.CreateTopic(ctx, data.Namespace, data.Name, data.QueueCount); err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		return &TCPResponse{Status: "ok", Data: "topic created"}

	case "delete_topic":
		var data tcpDeleteTopicData
		if err := json.Unmarshal(cmd.Data, &data); err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		if err := s.broker.DeleteTopic(ctx, data.Namespace, data.Name); err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		return &TCPResponse{Status: "ok", Data: "topic deleted"}

	case "list_topics":
		var data tcpListTopicsData
		if err := json.Unmarshal(cmd.Data, &data); err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		infos, err := s.broker.ListTopics(ctx, data.Namespace)
		if err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		return &TCPResponse{Status: "ok", Data: infos}

	case "publish":
		var data tcpPublishData
		if err := json.Unmarshal(cmd.Data, &data); err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		queueID := -1
		if data.QueueID != nil {
			queueID = *data.QueueID
		}
		resp, err := s.broker.Publish(ctx, &broker.PublishRequest{
			Namespace:  data.Namespace,
			Topic:      data.Topic,
			Payload:    []byte(data.Payload),
			Headers:    data.Headers,
			RoutingKey: data.RoutingKey,
			QueueID:    queueID,
		})
		if err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		return &TCPResponse{Status: "ok", Data: map[string]interface{}{
			"message_id": resp.MessageID,
			"queue_id":   resp.QueueID,
		}}

	case "consume":
		var data tcpConsumeData
		if err := json.Unmarshal(cmd.Data, &data); err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		msgs, err := s.broker.Consume(ctx, &broker.ConsumeRequest{
			Namespace: data.Namespace,
			Topic:     data.Topic,
			QueueID:   data.QueueID,
			BatchSize: data.BatchSize,
			AutoAck:   data.AutoAck,
		})
		if err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		return &TCPResponse{Status: "ok", Data: msgs}

	case "subscribe":
		var data tcpSubscribeData
		if err := json.Unmarshal(cmd.Data, &data); err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		// Subscribe uses push mode — server sends messages over the connection
		s.handleSubscribe(conn, data, encoding, writer)
		return nil // response handled inside

	case "ack":
		var data tcpAckData
		if err := json.Unmarshal(cmd.Data, &data); err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		if err := s.broker.Ack(ctx, data.Namespace, data.Topic, data.QueueID, data.MessageIDs); err != nil {
			return &TCPResponse{Status: "error", Error: err.Error()}
		}
		return &TCPResponse{Status: "ok", Data: "messages acknowledged"}

	default:
		return &TCPResponse{Status: "error", Error: fmt.Sprintf("unknown action: %s", cmd.Action)}
	}
}

func (s *TCPServer) handleSubscribe(conn net.Conn, data tcpSubscribeData, encoding byte, writer *bufio.Writer) {
	msgCh := make(chan *model.Message, 64)
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	err := s.broker.Subscribe(ctx, &broker.SubscribeRequest{
		Namespace: data.Namespace,
		Topic:     data.Topic,
		QueueID:   data.QueueID,
		AutoAck:   data.AutoAck,
	}, msgCh)
	if err != nil {
		s.writeResponse(writer, encoding, &TCPResponse{
			Status: "error",
			Error:  err.Error(),
		})
		return
	}

	// Send initial OK
	s.writeResponse(writer, encoding, &TCPResponse{
		Status: "ok",
		Data:   "subscribed",
	})

	// Push messages to client
	for msg := range msgCh {
		resp := &TCPResponse{
			Status: "ok",
			Data:   msg,
		}
		if err := s.writeResponse(writer, encoding, resp); err != nil {
			s.logger.Debug("TCP subscribe write error", zap.Error(err))
			return
		}
	}
}

// --- Frame I/O ---

func readFrame(reader *bufio.Reader) ([]byte, byte, error) {
	// Read payload length (4 bytes)
	var payloadLen uint32
	if err := binary.Read(reader, binary.BigEndian, &payloadLen); err != nil {
		return nil, 0, err
	}

	if payloadLen < 1 {
		return nil, 0, fmt.Errorf("invalid frame: payload too small")
	}

	// Read encoding flag (1 byte)
	encoding, err := reader.ReadByte()
	if err != nil {
		return nil, 0, err
	}

	// Read payload
	payload := make([]byte, payloadLen-1)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, 0, err
	}

	return payload, encoding, nil
}

func (s *TCPServer) writeResponse(writer *bufio.Writer, encoding byte, resp *TCPResponse) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}

	// Frame: [4B length][1B encoding][payload]
	totalLen := uint32(1 + len(data))
	if err := binary.Write(writer, binary.BigEndian, totalLen); err != nil {
		return err
	}
	if err := writer.WriteByte(encoding); err != nil {
		return err
	}
	if _, err := writer.Write(data); err != nil {
		return err
	}
	return writer.Flush()
}
