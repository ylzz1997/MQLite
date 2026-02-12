package codec

import (
	"encoding/json"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	pb "mqlite/api/proto/gen"
	"mqlite/internal/model"
)

// MessageToProto converts an internal Message to a protobuf Message.
func MessageToProto(msg *model.Message) *pb.Message {
	if msg == nil {
		return nil
	}
	return &pb.Message{
		Id:        msg.ID,
		Namespace: msg.Namespace,
		Topic:     msg.Topic,
		QueueId:   int32(msg.QueueID),
		Payload:   msg.Payload,
		Timestamp: msg.Timestamp,
		Headers:   msg.Headers,
	}
}

// ProtoToMessage converts a protobuf Message to an internal Message.
func ProtoToMessage(pbMsg *pb.Message) *model.Message {
	if pbMsg == nil {
		return nil
	}
	return &model.Message{
		ID:        pbMsg.Id,
		Namespace: pbMsg.Namespace,
		Topic:     pbMsg.Topic,
		QueueID:   int(pbMsg.QueueId),
		Payload:   pbMsg.Payload,
		Timestamp: pbMsg.Timestamp,
		Headers:   pbMsg.Headers,
	}
}

// MessagesToProto converts a slice of internal messages to protobuf messages.
func MessagesToProto(msgs []*model.Message) []*pb.Message {
	result := make([]*pb.Message, len(msgs))
	for i, msg := range msgs {
		result[i] = MessageToProto(msg)
	}
	return result
}

// MessageToJSON serializes an internal Message to JSON bytes.
func MessageToJSON(msg *model.Message) ([]byte, error) {
	return json.Marshal(msg)
}

// MessagesToJSON serializes a slice of messages to JSON bytes.
func MessagesToJSON(msgs []*model.Message) ([]byte, error) {
	return json.Marshal(msgs)
}

// ProtoToJSON converts a protobuf message to JSON bytes using protojson.
func ProtoToJSON(msg proto.Message) ([]byte, error) {
	marshaler := protojson.MarshalOptions{
		EmitUnpopulated: true,
		UseProtoNames:   true,
	}
	return marshaler.Marshal(msg)
}

// JSONToProto unmarshals JSON bytes into a protobuf message.
func JSONToProto(data []byte, msg proto.Message) error {
	unmarshaler := protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}
	return unmarshaler.Unmarshal(data, msg)
}

// TopicInfosToProto converts internal TopicInfo to protobuf TopicInfo.
func TopicInfosToProto(infos []model.TopicInfo) []*pb.TopicInfo {
	result := make([]*pb.TopicInfo, len(infos))
	for i, info := range infos {
		result[i] = &pb.TopicInfo{
			Name:       info.Name,
			QueueCount: int32(info.QueueCount),
			Version:    info.Version,
		}
	}
	return result
}
