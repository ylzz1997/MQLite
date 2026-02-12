# MQLite

轻量级消息队列服务器，支持多协议接入、任务窃取和 AOF 持久化。

## 特性

- **Namespace / Topic / 多队列模型** — 顶层命名空间隔离，每个 Topic 支持多个队列
- **多协议支持** — gRPC (9090)、HTTP REST (8080)、TCP (7070)
- **任务窃取 (Work Stealing)** — 当队列为空时自动从同 Topic 最忙的队列尾部窃取一半消息
- **AOF 持久化** — 类 Redis 的 Append-Only File，支持 always/everysec/no 三种 fsync 策略
- **AOF 重写** — 后台自动压缩 AOF 文件
- **消息确认 (Ack)** — 支持手动/自动确认，超时自动重投
- **Protobuf 内部传输** — gRPC 使用 Protobuf，HTTP/TCP 支持 JSON

## 快速开始

### 编译

```bash
mkdir bin
go build -o ./bin/mqlite ./cmd/mqlite
go build -o ./bin/mqlite_cli ./cmd/mqlite-cli
```

### 运行

```bash
./mqlite
# 或指定配置文件
./mqlite -config config.yaml
```

### Docker (可选)

```dockerfile
FROM golang:1.25-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o mqlite ./cmd/mqlite

FROM alpine:latest
COPY --from=builder /app/mqlite /usr/local/bin/
COPY config.yaml /etc/mqlite/
EXPOSE 9090 8080 7070
CMD ["mqlite", "-config", "/etc/mqlite/config.yaml"]
```

## 配置

参见 [config.yaml](config.yaml)，主要配置项：

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| server.grpc.port | 9090 | gRPC 端口 |
| server.http.port | 8080 | HTTP REST 端口 |
| server.tcp.port | 7070 | TCP 端口 |
| persistence.aof.fsync | everysec | AOF 刷盘策略 |
| persistence.aof.rewrite_min_size | 64MB | AOF 重写触发最小大小 |
| ack_timeout | 30s | 消息确认超时时间 |

## HTTP API

### Namespace

```bash
# 创建 Namespace
curl -X POST http://localhost:8080/v1/namespaces \
  -H "Content-Type: application/json" \
  -d '{"name": "production"}'

# 列出所有 Namespace
curl http://localhost:8080/v1/namespaces

# 删除 Namespace
curl -X DELETE http://localhost:8080/v1/namespaces/production
```

### Topic

```bash
# 创建 Topic（3 个队列）
curl -X POST http://localhost:8080/v1/namespaces/production/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "orders", "queue_count": 3}'

# 列出 Topic
curl http://localhost:8080/v1/namespaces/production/topics

# 删除 Topic
curl -X DELETE http://localhost:8080/v1/namespaces/production/topics/orders
```

### 发布消息

```bash
# 发布到 Topic（自动分配队列）
curl -X POST http://localhost:8080/v1/namespaces/production/topics/orders/publish \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {"order_id": "12345", "amount": 99.99},
    "headers": {"priority": "high"}
  }'

# 发布到指定队列
curl -X POST http://localhost:8080/v1/namespaces/production/topics/orders/publish \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {"order_id": "12345"},
    "queue_id": 1
  }'

# 使用 routing key（hash 分配）
curl -X POST http://localhost:8080/v1/namespaces/production/topics/orders/publish \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {"order_id": "12345"},
    "routing_key": "user-1001"
  }'
```

### 消费消息

```bash
# 从队列 0 消费 1 条（需要手动 Ack）
curl -X POST http://localhost:8080/v1/namespaces/production/topics/orders/queues/0/consume \
  -H "Content-Type: application/json" \
  -d '{"batch_size": 1}'

# 自动确认模式（消费即确认）
curl -X POST http://localhost:8080/v1/namespaces/production/topics/orders/queues/0/consume \
  -H "Content-Type: application/json" \
  -d '{"batch_size": 5, "auto_ack": true}'
```

### 确认消息

```bash
curl -X POST http://localhost:8080/v1/namespaces/production/topics/orders/queues/0/ack \
  -H "Content-Type: application/json" \
  -d '{"message_ids": ["uuid-1", "uuid-2"]}'
```

### SSE 订阅

```bash
# Server-Sent Events 实时订阅
curl -N http://localhost:8080/v1/namespaces/production/topics/orders/queues/0/subscribe?auto_ack=true
```

## gRPC API

使用生成的 Protobuf 定义 (`api/proto/mqlite.proto`)：

```bash
# 使用 grpcurl 测试
grpcurl -plaintext -d '{"name": "production"}' \
  localhost:9090 mqlite.v1.MQLiteService/CreateNamespace

grpcurl -plaintext -d '{"namespace": "production", "name": "orders", "queue_count": 3}' \
  localhost:9090 mqlite.v1.MQLiteService/CreateTopic

grpcurl -plaintext -d '{"namespace": "production", "topic": "orders", "payload": "eyJvcmRlciI6MTIzfQ=="}' \
  localhost:9090 mqlite.v1.MQLiteService/Publish
```

## TCP 协议

TCP 使用自定义帧协议：

```
[4 字节: payload 长度 (big-endian)]
[1 字节: 编码标记 (0=JSON)]
[N 字节: JSON payload]
```

命令格式：

```json
{
  "action": "publish",
  "data": {
    "namespace": "production",
    "topic": "orders",
    "payload": {"key": "value"},
    "queue_id": -1
  }
}
```

支持的 action: `create_namespace`, `delete_namespace`, `list_namespaces`, `create_topic`, `delete_topic`, `list_topics`, `publish`, `consume`, `subscribe`, `ack`

## 架构

```
                    ┌─────────┐  ┌──────────┐  ┌──────────┐
                    │  gRPC   │  │   HTTP   │  │   TCP    │
                    │ :9090   │  │  :8080   │  │  :7070   │
                    └────┬────┘  └────┬─────┘  └────┬─────┘
                         │            │              │
                         └──────┬─────┴──────────────┘
                                │
                    ┌───────────▼────────────┐
                    │     Broker (Router)     │
                    │  ┌──────────────────┐  │
                    │  │  Namespace Mgr   │  │
                    │  │  Topic Mgr       │  │
                    │  │  Queue Engine    │  │
                    │  │  Work Stealer    │  │
                    │  └──────────────────┘  │
                    └───────────┬────────────┘
                                │
                    ┌───────────▼────────────┐
                    │   AOF Persistence      │
                    │   (append-only file)   │
                    └────────────────────────┘
```

## 任务窃取

当 Consumer 从一个空队列消费时：

1. 遍历同 Topic 下所有队列
2. 找到消息数最多的队列
3. 从该队列尾部窃取一半消息到目标队列
4. 使用有序双锁机制避免死锁

## 项目结构

```
MQLite/
├── cmd/mqlite/main.go          # 启动入口
├── api/proto/
│   ├── mqlite.proto            # Protobuf 定义
│   └── gen/                    # 生成的 Go 代码
├── internal/
│   ├── broker/                 # Broker 核心
│   ├── model/                  # 数据模型
│   ├── server/                 # 协议服务器
│   ├── persistence/            # AOF 持久化
│   ├── codec/                  # 编解码
│   └── config/                 # 配置管理
├── config.yaml                 # 配置文件
└── data/                       # AOF 数据目录
```
