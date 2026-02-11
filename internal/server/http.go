package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"mqlite/internal/broker"
	"mqlite/internal/model"
)

// HTTPServer provides a RESTful HTTP interface.
type HTTPServer struct {
	broker *broker.Broker
	engine *gin.Engine
	server *http.Server
	logger *zap.Logger
	port   int
}

// NewHTTPServer creates a new HTTP/REST server.
func NewHTTPServer(b *broker.Broker, port int, logger *zap.Logger) *HTTPServer {
	gin.SetMode(gin.ReleaseMode)
	engine := gin.New()
	engine.Use(gin.Recovery())

	s := &HTTPServer{
		broker: b,
		engine: engine,
		logger: logger,
		port:   port,
	}

	s.setupRoutes()
	return s
}

func (s *HTTPServer) setupRoutes() {
	v1 := s.engine.Group("/v1")
	{
		// Namespace
		v1.POST("/namespaces", s.createNamespace)
		v1.DELETE("/namespaces/:namespace", s.deleteNamespace)
		v1.GET("/namespaces", s.listNamespaces)

		// Topic
		v1.POST("/namespaces/:namespace/topics", s.createTopic)
		v1.DELETE("/namespaces/:namespace/topics/:topic", s.deleteTopic)
		v1.GET("/namespaces/:namespace/topics", s.listTopics)

		// Publish
		v1.POST("/namespaces/:namespace/topics/:topic/publish", s.publish)

		// Consume
		v1.POST("/namespaces/:namespace/topics/:topic/queues/:queueId/consume", s.consume)

		// Subscribe (SSE)
		v1.GET("/namespaces/:namespace/topics/:topic/queues/:queueId/subscribe", s.subscribe)

		// Ack
		v1.POST("/namespaces/:namespace/topics/:topic/queues/:queueId/ack", s.ack)
	}

	// Health check
	s.engine.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
}

// Start begins listening for HTTP connections.
func (s *HTTPServer) Start() error {
	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: s.engine,
	}

	s.logger.Info("HTTP server starting", zap.Int("port", s.port))

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error("HTTP server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop gracefully shuts down the HTTP server.
func (s *HTTPServer) Stop() {
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.server.Shutdown(ctx)
	}
}

// --- JSON request/response types ---

type createNamespaceReq struct {
	Name string `json:"name" binding:"required"`
}

type createTopicReq struct {
	Name       string `json:"name" binding:"required"`
	QueueCount int    `json:"queue_count" binding:"required,min=1"`
}

type publishReq struct {
	Payload    json.RawMessage   `json:"payload" binding:"required"`
	Headers    map[string]string `json:"headers,omitempty"`
	RoutingKey string            `json:"routing_key,omitempty"`
	QueueID    *int              `json:"queue_id,omitempty"` // nil = auto (-1)
}

type consumeReq struct {
	BatchSize int  `json:"batch_size,omitempty"`
	AutoAck   bool `json:"auto_ack,omitempty"`
}

type ackReq struct {
	MessageIDs []string `json:"message_ids" binding:"required"`
}

type apiResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

// --- Handlers ---

func (s *HTTPServer) createNamespace(c *gin.Context) {
	var req createNamespaceReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, apiResponse{Success: false, Message: err.Error()})
		return
	}

	if err := s.broker.CreateNamespace(c.Request.Context(), req.Name); err != nil {
		c.JSON(http.StatusConflict, apiResponse{Success: false, Message: err.Error()})
		return
	}

	c.JSON(http.StatusCreated, apiResponse{Success: true, Message: "namespace created"})
}

func (s *HTTPServer) deleteNamespace(c *gin.Context) {
	name := c.Param("namespace")
	if err := s.broker.DeleteNamespace(c.Request.Context(), name); err != nil {
		c.JSON(http.StatusNotFound, apiResponse{Success: false, Message: err.Error()})
		return
	}
	c.JSON(http.StatusOK, apiResponse{Success: true, Message: "namespace deleted"})
}

func (s *HTTPServer) listNamespaces(c *gin.Context) {
	names := s.broker.ListNamespaces(c.Request.Context())
	c.JSON(http.StatusOK, apiResponse{Success: true, Data: names})
}

func (s *HTTPServer) createTopic(c *gin.Context) {
	namespace := c.Param("namespace")
	var req createTopicReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, apiResponse{Success: false, Message: err.Error()})
		return
	}

	if err := s.broker.CreateTopic(c.Request.Context(), namespace, req.Name, req.QueueCount); err != nil {
		c.JSON(http.StatusConflict, apiResponse{Success: false, Message: err.Error()})
		return
	}

	c.JSON(http.StatusCreated, apiResponse{Success: true, Message: "topic created"})
}

func (s *HTTPServer) deleteTopic(c *gin.Context) {
	namespace := c.Param("namespace")
	topic := c.Param("topic")

	if err := s.broker.DeleteTopic(c.Request.Context(), namespace, topic); err != nil {
		c.JSON(http.StatusNotFound, apiResponse{Success: false, Message: err.Error()})
		return
	}
	c.JSON(http.StatusOK, apiResponse{Success: true, Message: "topic deleted"})
}

func (s *HTTPServer) listTopics(c *gin.Context) {
	namespace := c.Param("namespace")
	infos, err := s.broker.ListTopics(c.Request.Context(), namespace)
	if err != nil {
		c.JSON(http.StatusNotFound, apiResponse{Success: false, Message: err.Error()})
		return
	}
	c.JSON(http.StatusOK, apiResponse{Success: true, Data: infos})
}

func (s *HTTPServer) publish(c *gin.Context) {
	namespace := c.Param("namespace")
	topic := c.Param("topic")

	var req publishReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, apiResponse{Success: false, Message: err.Error()})
		return
	}

	// Default queueID to -1 (auto) if not specified
	queueID := -1
	if req.QueueID != nil {
		queueID = *req.QueueID
	}

	resp, err := s.broker.Publish(c.Request.Context(), &broker.PublishRequest{
		Namespace:  namespace,
		Topic:      topic,
		Payload:    []byte(req.Payload),
		Headers:    req.Headers,
		RoutingKey: req.RoutingKey,
		QueueID:    queueID,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, apiResponse{Success: false, Message: err.Error()})
		return
	}

	c.JSON(http.StatusOK, apiResponse{
		Success: true,
		Data: gin.H{
			"message_id": resp.MessageID,
			"queue_id":   resp.QueueID,
		},
	})
}

func (s *HTTPServer) consume(c *gin.Context) {
	namespace := c.Param("namespace")
	topic := c.Param("topic")
	queueID, err := strconv.Atoi(c.Param("queueId"))
	if err != nil {
		c.JSON(http.StatusBadRequest, apiResponse{Success: false, Message: "invalid queue_id"})
		return
	}

	var req consumeReq
	// Allow empty body (defaults will be used)
	_ = c.ShouldBindJSON(&req)

	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = 1
	}

	msgs, err := s.broker.Consume(c.Request.Context(), &broker.ConsumeRequest{
		Namespace: namespace,
		Topic:     topic,
		QueueID:   queueID,
		BatchSize: batchSize,
		AutoAck:   req.AutoAck,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, apiResponse{Success: false, Message: err.Error()})
		return
	}

	c.JSON(http.StatusOK, apiResponse{Success: true, Data: msgs})
}

func (s *HTTPServer) subscribe(c *gin.Context) {
	namespace := c.Param("namespace")
	topic := c.Param("topic")
	queueID, err := strconv.Atoi(c.Param("queueId"))
	if err != nil {
		c.JSON(http.StatusBadRequest, apiResponse{Success: false, Message: "invalid queue_id"})
		return
	}

	autoAck := c.Query("auto_ack") == "true"

	// Server-Sent Events
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("Connection", "keep-alive")
	c.Header("X-Accel-Buffering", "no")

	msgCh := make(chan *model.Message, 64)
	ctx, cancel := context.WithCancel(c.Request.Context())
	defer cancel()

	if err := s.broker.Subscribe(ctx, &broker.SubscribeRequest{
		Namespace: namespace,
		Topic:     topic,
		QueueID:   queueID,
		AutoAck:   autoAck,
	}, msgCh); err != nil {
		c.JSON(http.StatusInternalServerError, apiResponse{Success: false, Message: err.Error()})
		return
	}

	c.Stream(func(w io.Writer) bool {
		select {
		case msg, ok := <-msgCh:
			if !ok {
				return false
			}
			data, _ := json.Marshal(msg)
			c.SSEvent("message", string(data))
			return true
		case <-ctx.Done():
			return false
		}
	})
}

func (s *HTTPServer) ack(c *gin.Context) {
	namespace := c.Param("namespace")
	topic := c.Param("topic")
	queueID, err := strconv.Atoi(c.Param("queueId"))
	if err != nil {
		c.JSON(http.StatusBadRequest, apiResponse{Success: false, Message: "invalid queue_id"})
		return
	}

	var req ackReq
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, apiResponse{Success: false, Message: err.Error()})
		return
	}

	if err := s.broker.Ack(c.Request.Context(), namespace, topic, queueID, req.MessageIDs); err != nil {
		c.JSON(http.StatusInternalServerError, apiResponse{Success: false, Message: err.Error()})
		return
	}

	c.JSON(http.StatusOK, apiResponse{Success: true, Message: "messages acknowledged"})
}
