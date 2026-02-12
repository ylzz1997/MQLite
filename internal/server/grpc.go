package server

import (
	"context"
	"fmt"
	"net"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	pb "mqlite/api/proto/gen"
	"mqlite/internal/broker"
	"mqlite/internal/codec"
	"mqlite/internal/model"
)

// GRPCServer wraps the gRPC service implementation.
type GRPCServer struct {
	pb.UnimplementedMQLiteServiceServer
	broker *broker.Broker
	server *grpc.Server
	logger *zap.Logger
	port   int
}

// NewGRPCServer creates a new gRPC server.
func NewGRPCServer(b *broker.Broker, port int, logger *zap.Logger) *GRPCServer {
	return &GRPCServer{
		broker: b,
		logger: logger,
		port:   port,
	}
}

// Start begins listening for gRPC connections.
func (s *GRPCServer) Start() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("gRPC listen: %w", err)
	}

	s.server = grpc.NewServer()
	pb.RegisterMQLiteServiceServer(s.server, s)
	reflection.Register(s.server)

	s.logger.Info("gRPC server starting", zap.Int("port", s.port))

	go func() {
		if err := s.server.Serve(lis); err != nil {
			s.logger.Error("gRPC server error", zap.Error(err))
		}
	}()

	return nil
}

// Stop gracefully shuts down the gRPC server.
func (s *GRPCServer) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}

// --- Service implementations ---

func (s *GRPCServer) CreateNamespace(ctx context.Context, req *pb.CreateNamespaceRequest) (*pb.CreateNamespaceResponse, error) {
	err := s.broker.CreateNamespace(ctx, req.GetName())
	if err != nil {
		return &pb.CreateNamespaceResponse{Success: false, Message: err.Error()}, nil
	}
	return &pb.CreateNamespaceResponse{Success: true, Message: "namespace created"}, nil
}

func (s *GRPCServer) DeleteNamespace(ctx context.Context, req *pb.DeleteNamespaceRequest) (*pb.DeleteNamespaceResponse, error) {
	err := s.broker.DeleteNamespace(ctx, req.GetName())
	if err != nil {
		return &pb.DeleteNamespaceResponse{Success: false, Message: err.Error()}, nil
	}
	return &pb.DeleteNamespaceResponse{Success: true, Message: "namespace deleted"}, nil
}

func (s *GRPCServer) ListNamespaces(ctx context.Context, req *pb.ListNamespacesRequest) (*pb.ListNamespacesResponse, error) {
	names := s.broker.ListNamespaces(ctx)
	return &pb.ListNamespacesResponse{Namespaces: names}, nil
}

func (s *GRPCServer) CreateTopic(ctx context.Context, req *pb.CreateTopicRequest) (*pb.CreateTopicResponse, error) {
	err := s.broker.CreateTopic(ctx, req.GetNamespace(), req.GetName(), int(req.GetQueueCount()))
	if err != nil {
		return &pb.CreateTopicResponse{Success: false, Message: err.Error()}, nil
	}
	return &pb.CreateTopicResponse{Success: true, Message: "topic created"}, nil
}

func (s *GRPCServer) DeleteTopic(ctx context.Context, req *pb.DeleteTopicRequest) (*pb.DeleteTopicResponse, error) {
	err := s.broker.DeleteTopic(ctx, req.GetNamespace(), req.GetName())
	if err != nil {
		return &pb.DeleteTopicResponse{Success: false, Message: err.Error()}, nil
	}
	return &pb.DeleteTopicResponse{Success: true, Message: "topic deleted"}, nil
}

func (s *GRPCServer) ListTopics(ctx context.Context, req *pb.ListTopicsRequest) (*pb.ListTopicsResponse, error) {
	infos, err := s.broker.ListTopics(ctx, req.GetNamespace())
	if err != nil {
		return nil, err
	}
	return &pb.ListTopicsResponse{Topics: codec.TopicInfosToProto(infos)}, nil
}

func (s *GRPCServer) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	// Proto default for int32 is 0. Use queue_id field:
	//   -1 or omitted (0) = auto routing; positive = direct routing
	queueID := int(req.GetQueueId())
	if queueID == 0 {
		queueID = -1 // treat proto default (0) as auto
	}
	resp, err := s.broker.Publish(ctx, &broker.PublishRequest{
		Namespace:  req.GetNamespace(),
		Topic:      req.GetTopic(),
		Payload:    req.GetPayload(),
		Headers:    req.GetHeaders(),
		RoutingKey: req.GetRoutingKey(),
		QueueID:    queueID,
	})
	if err != nil {
		return nil, err
	}
	return &pb.PublishResponse{
		MessageId: resp.MessageID,
		QueueId:   int32(resp.QueueID),
	}, nil
}

func (s *GRPCServer) Consume(ctx context.Context, req *pb.ConsumeRequest) (*pb.ConsumeResponse, error) {
	msgs, err := s.broker.Consume(ctx, &broker.ConsumeRequest{
		Namespace: req.GetNamespace(),
		Topic:     req.GetTopic(),
		QueueID:   int(req.GetQueueId()),
		BatchSize: int(req.GetBatchSize()),
		AutoAck:   req.GetAutoAck(),
	})
	if err != nil {
		return nil, err
	}
	return &pb.ConsumeResponse{Messages: codec.MessagesToProto(msgs)}, nil
}

func (s *GRPCServer) Subscribe(req *pb.SubscribeRequest, stream pb.MQLiteService_SubscribeServer) error {
	msgCh := make(chan *model.Message, 64)

	ctx := stream.Context()
	err := s.broker.Subscribe(ctx, &broker.SubscribeRequest{
		Namespace: req.GetNamespace(),
		Topic:     req.GetTopic(),
		QueueID:   int(req.GetQueueId()),
		AutoAck:   req.GetAutoAck(),
	}, msgCh)
	if err != nil {
		return err
	}

	for msg := range msgCh {
		pbMsg := codec.MessageToProto(msg)
		if err := stream.Send(pbMsg); err != nil {
			return err
		}
	}

	return nil
}

func (s *GRPCServer) Ack(ctx context.Context, req *pb.AckRequest) (*pb.AckResponse, error) {
	err := s.broker.Ack(ctx, req.GetNamespace(), req.GetTopic(), int(req.GetQueueId()), req.GetMessageIds())
	if err != nil {
		return &pb.AckResponse{Success: false}, err
	}
	return &pb.AckResponse{Success: true}, nil
}

func (s *GRPCServer) ResizeTopic(ctx context.Context, req *pb.ResizeTopicRequest) (*pb.ResizeTopicResponse, error) {
	resp, err := s.broker.ResizeTopic(ctx, req.GetNamespace(), req.GetName(), int(req.GetNewQueueCount()))
	if err != nil {
		return &pb.ResizeTopicResponse{Success: false, Message: err.Error()}, nil
	}
	return &pb.ResizeTopicResponse{
		Success:       true,
		Message:       "topic resized",
		NewQueueCount: int32(resp.NewQueueCount),
		Version:       resp.Version,
		Draining:      resp.Draining,
	}, nil
}

func (s *GRPCServer) RebalanceTopic(ctx context.Context, req *pb.RebalanceTopicRequest) (*pb.RebalanceTopicResponse, error) {
	err := s.broker.RebalanceTopic(ctx, req.GetNamespace(), req.GetName())
	if err != nil {
		return &pb.RebalanceTopicResponse{Success: false, Message: err.Error()}, nil
	}
	return &pb.RebalanceTopicResponse{Success: true, Message: "topic rebalanced"}, nil
}
