# MQLite API Reference

MQLite 提供三种协议接入：**HTTP REST** (默认端口 8080)、**gRPC** (默认端口 9090)、**TCP** (默认端口 7070)。

本文档覆盖 HTTP REST 和 gRPC 两种协议的完整接口说明。TCP 协议请参阅 [README - TCP 协议](../README.md#tcp-协议)。

---

## 目录

- [通用约定](#通用约定)
- [1. Namespace 管理](#1-namespace-管理)
  - [1.1 创建 Namespace](#11-创建-namespace)
  - [1.2 删除 Namespace](#12-删除-namespace)
  - [1.3 列出 Namespace](#13-列出-namespace)
- [2. Topic 管理](#2-topic-管理)
  - [2.1 创建 Topic](#21-创建-topic)
  - [2.2 删除 Topic](#22-删除-topic)
  - [2.3 列出 Topic](#23-列出-topic)
  - [2.4 调整 Topic 队列数 (Resize)](#24-调整-topic-队列数-resize)
  - [2.5 重平衡 Topic (Rebalance)](#25-重平衡-topic-rebalance)
- [3. 消息发布](#3-消息发布)
  - [3.1 发布消息 (Publish)](#31-发布消息-publish)
- [4. 消息消费](#4-消息消费)
  - [4.1 拉取消费 (Consume)](#41-拉取消费-consume)
  - [4.2 实时订阅 (Subscribe)](#42-实时订阅-subscribe)
- [5. 消息确认](#5-消息确认)
  - [5.1 确认消息 (Ack)](#51-确认消息-ack)
- [6. 健康检查](#6-健康检查)
- [附录：数据模型](#附录数据模型)

---

## 通用约定

### HTTP REST

| 项目 | 说明 |
|------|------|
| Base URL | `http://localhost:8080` |
| API 前缀 | `/v1` |
| Content-Type | `application/json` |
| 字符编码 | UTF-8 |

**统一响应格式：**

```json
{
  "success": true,
  "message": "操作说明（可选）",
  "data": "响应数据（可选）"
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| success | bool | 操作是否成功 |
| message | string | 操作结果描述，成功/失败时均可能出现 |
| data | any | 返回数据，仅部分接口包含 |

**常见 HTTP 状态码：**

| 状态码 | 含义 |
|--------|------|
| 200 | 成功 |
| 201 | 创建成功 |
| 400 | 请求参数错误 |
| 404 | 资源不存在 |
| 409 | 资源冲突（已存在） |
| 500 | 服务器内部错误 |

### gRPC

| 项目 | 说明 |
|------|------|
| 地址 | `localhost:9090` |
| 服务名 | `mqlite.v1.MQLiteService` |
| Proto 文件 | `api/proto/mqlite.proto` |
| 反射 | 已启用（支持 grpcurl 自动发现） |

---

## 1. Namespace 管理

Namespace 是顶层隔离域，所有 Topic 归属于某个 Namespace。

### 1.1 创建 Namespace

创建一个新的命名空间。

#### HTTP

```
POST /v1/namespaces
```

**请求体：**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| name | string | 是 | 命名空间名称 |

**响应：**

| 状态码 | 说明 |
|--------|------|
| 201 | 创建成功 |
| 400 | 缺少 name 字段 |
| 409 | 命名空间已存在 |

```json
// 成功 (201)
{ "success": true, "message": "namespace created" }

// 失败 (409)
{ "success": false, "message": "namespace already exists" }
```

**curl 示例：**

```bash
curl -X POST http://localhost:8080/v1/namespaces \
  -H "Content-Type: application/json" \
  -d '{"name": "production"}'
```

#### gRPC

```
rpc CreateNamespace(CreateNamespaceRequest) returns (CreateNamespaceResponse)
```

| 字段 | 类型 | 说明 |
|------|------|------|
| **请求** name | string | 命名空间名称 |
| **响应** success | bool | 是否成功 |
| **响应** message | string | 结果描述 |

**grpcurl 示例：**

```bash
grpcurl -plaintext -d '{"name": "production"}' \
  localhost:9090 mqlite.v1.MQLiteService/CreateNamespace
```

---

### 1.2 删除 Namespace

删除指定命名空间及其下所有 Topic 和消息。

#### HTTP

```
DELETE /v1/namespaces/:namespace
```

**路径参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| namespace | string | 命名空间名称 |

**响应：**

| 状态码 | 说明 |
|--------|------|
| 200 | 删除成功 |
| 404 | 命名空间不存在 |

```json
// 成功 (200)
{ "success": true, "message": "namespace deleted" }
```

**curl 示例：**

```bash
curl -X DELETE http://localhost:8080/v1/namespaces/production
```

#### gRPC

```
rpc DeleteNamespace(DeleteNamespaceRequest) returns (DeleteNamespaceResponse)
```

| 字段 | 类型 | 说明 |
|------|------|------|
| **请求** name | string | 命名空间名称 |
| **响应** success | bool | 是否成功 |
| **响应** message | string | 结果描述 |

**grpcurl 示例：**

```bash
grpcurl -plaintext -d '{"name": "production"}' \
  localhost:9090 mqlite.v1.MQLiteService/DeleteNamespace
```

---

### 1.3 列出 Namespace

获取所有命名空间列表。

#### HTTP

```
GET /v1/namespaces
```

**响应 (200)：**

```json
{
  "success": true,
  "data": ["production", "staging", "test"]
}
```

**curl 示例：**

```bash
curl http://localhost:8080/v1/namespaces
```

#### gRPC

```
rpc ListNamespaces(ListNamespacesRequest) returns (ListNamespacesResponse)
```

| 字段 | 类型 | 说明 |
|------|------|------|
| **请求** | (无字段) | 空请求 |
| **响应** namespaces | repeated string | 命名空间名称列表 |

**grpcurl 示例：**

```bash
grpcurl -plaintext localhost:9090 mqlite.v1.MQLiteService/ListNamespaces
```

---

## 2. Topic 管理

Topic 是消息的逻辑分组，每个 Topic 包含一个或多个队列 (Queue)。消息通过路由策略分配到不同队列。

### 2.1 创建 Topic

在指定命名空间下创建 Topic。

#### HTTP

```
POST /v1/namespaces/:namespace/topics
```

**路径参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| namespace | string | 命名空间名称 |

**请求体：**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| name | string | 是 | Topic 名称 |
| queue_count | int | 是 | 队列数量（最小为 1） |

**响应：**

| 状态码 | 说明 |
|--------|------|
| 201 | 创建成功 |
| 400 | 参数错误 |
| 409 | Topic 已存在 |

```json
// 成功 (201)
{ "success": true, "message": "topic created" }
```

**curl 示例：**

```bash
curl -X POST http://localhost:8080/v1/namespaces/production/topics \
  -H "Content-Type: application/json" \
  -d '{"name": "orders", "queue_count": 3}'
```

#### gRPC

```
rpc CreateTopic(CreateTopicRequest) returns (CreateTopicResponse)
```

| 字段 | 类型 | 说明 |
|------|------|------|
| **请求** namespace | string | 命名空间名称 |
| **请求** name | string | Topic 名称 |
| **请求** queue_count | int32 | 队列数量 |
| **响应** success | bool | 是否成功 |
| **响应** message | string | 结果描述 |

**grpcurl 示例：**

```bash
grpcurl -plaintext \
  -d '{"namespace": "production", "name": "orders", "queue_count": 3}' \
  localhost:9090 mqlite.v1.MQLiteService/CreateTopic
```

---

### 2.2 删除 Topic

删除指定 Topic 及其所有队列和消息。

#### HTTP

```
DELETE /v1/namespaces/:namespace/topics/:topic
```

**路径参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| namespace | string | 命名空间名称 |
| topic | string | Topic 名称 |

**响应：**

| 状态码 | 说明 |
|--------|------|
| 200 | 删除成功 |
| 404 | Topic 不存在 |

```json
// 成功 (200)
{ "success": true, "message": "topic deleted" }
```

**curl 示例：**

```bash
curl -X DELETE http://localhost:8080/v1/namespaces/production/topics/orders
```

#### gRPC

```
rpc DeleteTopic(DeleteTopicRequest) returns (DeleteTopicResponse)
```

| 字段 | 类型 | 说明 |
|------|------|------|
| **请求** namespace | string | 命名空间名称 |
| **请求** name | string | Topic 名称 |
| **响应** success | bool | 是否成功 |
| **响应** message | string | 结果描述 |

**grpcurl 示例：**

```bash
grpcurl -plaintext \
  -d '{"namespace": "production", "name": "orders"}' \
  localhost:9090 mqlite.v1.MQLiteService/DeleteTopic
```

---

### 2.3 列出 Topic

列出指定命名空间下的所有 Topic 及其元数据。

#### HTTP

```
GET /v1/namespaces/:namespace/topics
```

**路径参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| namespace | string | 命名空间名称 |

**响应 (200)：**

```json
{
  "success": true,
  "data": [
    { "name": "orders", "queue_count": 3, "version": 1 },
    { "name": "events", "queue_count": 1, "version": 0 }
  ]
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| name | string | Topic 名称 |
| queue_count | int | 当前队列数量 |
| version | uint64 | Topic 版本号（每次 resize 递增） |

**curl 示例：**

```bash
curl http://localhost:8080/v1/namespaces/production/topics
```

#### gRPC

```
rpc ListTopics(ListTopicsRequest) returns (ListTopicsResponse)
```

| 字段 | 类型 | 说明 |
|------|------|------|
| **请求** namespace | string | 命名空间名称 |
| **响应** topics | repeated TopicInfo | Topic 信息列表 |

**TopicInfo：**

| 字段 | 类型 | 说明 |
|------|------|------|
| name | string | Topic 名称 |
| queue_count | int32 | 队列数量 |
| version | uint64 | 版本号 |

**grpcurl 示例：**

```bash
grpcurl -plaintext \
  -d '{"namespace": "production"}' \
  localhost:9090 mqlite.v1.MQLiteService/ListTopics
```

---

### 2.4 调整 Topic 队列数 (Resize)

动态调整 Topic 的队列数量。扩容时立即生效；缩容时启动异步 drain，将被移除队列的消息迁移到保留队列。

#### HTTP

```
PUT /v1/namespaces/:namespace/topics/:topic/resize
```

**路径参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| namespace | string | 命名空间名称 |
| topic | string | Topic 名称 |

**请求体：**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| new_queue_count | int | 是 | 新的队列数量（最小为 1） |

**响应 (200)：**

```json
{
  "success": true,
  "message": "topic resized",
  "data": {
    "new_queue_count": 5,
    "version": 2,
    "draining": false
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| new_queue_count | int | 调整后的队列数量 |
| version | uint64 | 新版本号 |
| draining | bool | 是否正在异步 drain（缩容时为 true） |

**curl 示例：**

```bash
curl -X PUT http://localhost:8080/v1/namespaces/production/topics/orders/resize \
  -H "Content-Type: application/json" \
  -d '{"new_queue_count": 5}'
```

#### gRPC

```
rpc ResizeTopic(ResizeTopicRequest) returns (ResizeTopicResponse)
```

| 字段 | 类型 | 说明 |
|------|------|------|
| **请求** namespace | string | 命名空间名称 |
| **请求** name | string | Topic 名称 |
| **请求** new_queue_count | int32 | 新的队列数量 |
| **响应** success | bool | 是否成功 |
| **响应** message | string | 结果描述 |
| **响应** new_queue_count | int32 | 调整后的队列数量 |
| **响应** version | uint64 | 新版本号 |
| **响应** draining | bool | 是否正在 drain |

**grpcurl 示例：**

```bash
grpcurl -plaintext \
  -d '{"namespace": "production", "name": "orders", "new_queue_count": 5}' \
  localhost:9090 mqlite.v1.MQLiteService/ResizeTopic
```

---

### 2.5 重平衡 Topic (Rebalance)

手动触发 Topic 下所有队列的消息重平衡，使各队列消息数量趋于均匀。

#### HTTP

```
POST /v1/namespaces/:namespace/topics/:topic/rebalance
```

**路径参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| namespace | string | 命名空间名称 |
| topic | string | Topic 名称 |

**响应 (200)：**

```json
{ "success": true, "message": "topic rebalanced" }
```

**curl 示例：**

```bash
curl -X POST http://localhost:8080/v1/namespaces/production/topics/orders/rebalance
```

#### gRPC

```
rpc RebalanceTopic(RebalanceTopicRequest) returns (RebalanceTopicResponse)
```

| 字段 | 类型 | 说明 |
|------|------|------|
| **请求** namespace | string | 命名空间名称 |
| **请求** name | string | Topic 名称 |
| **响应** success | bool | 是否成功 |
| **响应** message | string | 结果描述 |

**grpcurl 示例：**

```bash
grpcurl -plaintext \
  -d '{"namespace": "production", "name": "orders"}' \
  localhost:9090 mqlite.v1.MQLiteService/RebalanceTopic
```

---

## 3. 消息发布

### 3.1 发布消息 (Publish)

向指定 Topic 发布一条消息。支持三种路由方式：

| 路由方式 | 说明 |
|----------|------|
| **自动分配** | 不指定 `queue_id` 和 `routing_key`，round-robin 轮询分配 |
| **指定队列** | 设置 `queue_id` 为目标队列索引 |
| **routing key** | 设置 `routing_key`，通过 hash 确定目标队列 |

#### HTTP

```
POST /v1/namespaces/:namespace/topics/:topic/publish
```

**路径参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| namespace | string | 命名空间名称 |
| topic | string | Topic 名称 |

**请求体：**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| payload | any (JSON) | 是 | 消息内容，任意合法 JSON |
| headers | map\<string, string\> | 否 | 消息头，键值对 |
| routing_key | string | 否 | 路由键，hash 后决定目标队列 |
| queue_id | int | 否 | 指定目标队列索引；不传或 null 表示自动分配 |

> **路由优先级**：`queue_id` > `routing_key` > 自动分配

**响应 (200)：**

```json
{
  "success": true,
  "data": {
    "message_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
    "queue_id": 0
  }
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| message_id | string | 消息唯一 ID (UUID) |
| queue_id | int | 实际分配到的队列索引 |

**curl 示例：**

```bash
# 自动分配队列
curl -X POST http://localhost:8080/v1/namespaces/production/topics/orders/publish \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {"order_id": "12345", "amount": 99.99},
    "headers": {"priority": "high"}
  }'

# 指定队列
curl -X POST http://localhost:8080/v1/namespaces/production/topics/orders/publish \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {"order_id": "12345"},
    "queue_id": 1
  }'

# 使用 routing key
curl -X POST http://localhost:8080/v1/namespaces/production/topics/orders/publish \
  -H "Content-Type: application/json" \
  -d '{
    "payload": {"order_id": "12345"},
    "routing_key": "user-1001"
  }'
```

#### gRPC

```
rpc Publish(PublishRequest) returns (PublishResponse)
```

| 字段 | 类型 | 说明 |
|------|------|------|
| **请求** namespace | string | 命名空间名称 |
| **请求** topic | string | Topic 名称 |
| **请求** payload | bytes | 消息内容（二进制） |
| **请求** headers | map\<string, string\> | 消息头 |
| **请求** routing_key | string | 路由键（可选） |
| **请求** queue_id | int32 | 目标队列（-1 或 0 = 自动） |
| **响应** message_id | string | 消息 ID |
| **响应** queue_id | int32 | 实际分配的队列 |

> **注意**：gRPC 中 `queue_id` 的 proto 默认值为 0，服务端将 0 视为自动分配（等同于 -1）。若需指定队列 0，请使用 `routing_key` 或直接传入正数。

**grpcurl 示例：**

```bash
# payload 需要 base64 编码
grpcurl -plaintext \
  -d '{"namespace": "production", "topic": "orders", "payload": "eyJvcmRlcl9pZCI6ICIxMjM0NSJ9"}' \
  localhost:9090 mqlite.v1.MQLiteService/Publish
```

---

## 4. 消息消费

### 4.1 拉取消费 (Consume)

从指定队列拉取消息。支持批量消费和自动确认模式。

当目标队列为空时，会自动触发**任务窃取**（Work Stealing）：从同 Topic 下消息最多的队列尾部窃取一半消息。

#### HTTP

```
POST /v1/namespaces/:namespace/topics/:topic/queues/:queueId/consume
```

**路径参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| namespace | string | 命名空间名称 |
| topic | string | Topic 名称 |
| queueId | int | 队列索引（从 0 开始） |

**请求体（可选）：**

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| batch_size | int | 否 | 1 | 批量消费数量 |
| auto_ack | bool | 否 | false | 是否自动确认（true = 消费即确认） |

**响应 (200)：**

```json
{
  "success": true,
  "data": [
    {
      "id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
      "namespace": "production",
      "topic": "orders",
      "queue_id": 0,
      "payload": "eyJvcmRlcl9pZCI6ICIxMjM0NSJ9",
      "timestamp": 1739347200000000000,
      "headers": {"priority": "high"}
    }
  ]
}
```

返回的 `data` 是 [Message](#message) 数组。若队列为空，返回空数组 `[]`。

**curl 示例：**

```bash
# 消费 1 条，需手动 Ack
curl -X POST http://localhost:8080/v1/namespaces/production/topics/orders/queues/0/consume \
  -H "Content-Type: application/json" \
  -d '{"batch_size": 1}'

# 批量消费 5 条，自动确认
curl -X POST http://localhost:8080/v1/namespaces/production/topics/orders/queues/0/consume \
  -H "Content-Type: application/json" \
  -d '{"batch_size": 5, "auto_ack": true}'
```

#### gRPC

```
rpc Consume(ConsumeRequest) returns (ConsumeResponse)
```

| 字段 | 类型 | 说明 |
|------|------|------|
| **请求** namespace | string | 命名空间名称 |
| **请求** topic | string | Topic 名称 |
| **请求** queue_id | int32 | 队列索引 |
| **请求** batch_size | int32 | 批量大小（默认 1） |
| **请求** auto_ack | bool | 是否自动确认 |
| **响应** messages | repeated Message | 消息列表 |

**grpcurl 示例：**

```bash
grpcurl -plaintext \
  -d '{"namespace": "production", "topic": "orders", "queue_id": 0, "batch_size": 10}' \
  localhost:9090 mqlite.v1.MQLiteService/Consume
```

---

### 4.2 实时订阅 (Subscribe)

推送模式：服务端主动推送新消息到客户端。HTTP 使用 SSE（Server-Sent Events），gRPC 使用 Server Stream。

连接保持直到客户端断开或服务端关闭。

#### HTTP (SSE)

```
GET /v1/namespaces/:namespace/topics/:topic/queues/:queueId/subscribe
```

**路径参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| namespace | string | 命名空间名称 |
| topic | string | Topic 名称 |
| queueId | int | 队列索引 |

**查询参数：**

| 参数 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| auto_ack | string | "false" | 设为 "true" 时自动确认 |

**响应格式：** `text/event-stream`

```
event: message
data: {"id":"uuid-xxx","namespace":"production","topic":"orders","queue_id":0,"payload":"...","timestamp":1739347200000000000}

event: message
data: {"id":"uuid-yyy","namespace":"production","topic":"orders","queue_id":0,"payload":"...","timestamp":1739347201000000000}
```

每个 SSE 事件的 `data` 字段是一个 JSON 序列化的 [Message](#message) 对象。

**curl 示例：**

```bash
# -N 禁用缓冲以实时接收事件
curl -N "http://localhost:8080/v1/namespaces/production/topics/orders/queues/0/subscribe?auto_ack=true"
```

#### gRPC (Server Stream)

```
rpc Subscribe(SubscribeRequest) returns (stream Message)
```

| 字段 | 类型 | 说明 |
|------|------|------|
| **请求** namespace | string | 命名空间名称 |
| **请求** topic | string | Topic 名称 |
| **请求** queue_id | int32 | 队列索引 |
| **请求** auto_ack | bool | 是否自动确认 |
| **响应** (stream) | Message | 持续推送的消息流 |

**grpcurl 示例：**

```bash
grpcurl -plaintext \
  -d '{"namespace": "production", "topic": "orders", "queue_id": 0, "auto_ack": true}' \
  localhost:9090 mqlite.v1.MQLiteService/Subscribe
```

---

## 5. 消息确认

### 5.1 确认消息 (Ack)

手动确认已消费的消息。未确认的消息在超时（默认 30 秒，可通过 `config.yaml` 的 `ack_timeout` 配置）后会被重新投递。

使用 `auto_ack=true` 消费时无需手动调用此接口。

#### HTTP

```
POST /v1/namespaces/:namespace/topics/:topic/queues/:queueId/ack
```

**路径参数：**

| 参数 | 类型 | 说明 |
|------|------|------|
| namespace | string | 命名空间名称 |
| topic | string | Topic 名称 |
| queueId | int | 队列索引 |

**请求体：**

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| message_ids | string[] | 是 | 需要确认的消息 ID 列表 |

**响应 (200)：**

```json
{ "success": true, "message": "messages acknowledged" }
```

**curl 示例：**

```bash
curl -X POST http://localhost:8080/v1/namespaces/production/topics/orders/queues/0/ack \
  -H "Content-Type: application/json" \
  -d '{"message_ids": ["a1b2c3d4-e5f6-7890-abcd-ef1234567890"]}'
```

#### gRPC

```
rpc Ack(AckRequest) returns (AckResponse)
```

| 字段 | 类型 | 说明 |
|------|------|------|
| **请求** namespace | string | 命名空间名称 |
| **请求** topic | string | Topic 名称 |
| **请求** queue_id | int32 | 队列索引 |
| **请求** message_ids | repeated string | 消息 ID 列表 |
| **响应** success | bool | 是否成功 |

**grpcurl 示例：**

```bash
grpcurl -plaintext \
  -d '{"namespace": "production", "topic": "orders", "queue_id": 0, "message_ids": ["uuid-1", "uuid-2"]}' \
  localhost:9090 mqlite.v1.MQLiteService/Ack
```

---

## 6. 健康检查

检查服务是否正常运行。

#### HTTP

```
GET /health
```

**响应 (200)：**

```json
{ "status": "ok" }
```

**curl 示例：**

```bash
curl http://localhost:8080/health
```

> gRPC 端已启用反射服务，可使用 `grpcurl -plaintext localhost:9090 list` 验证连通性。

---

## 附录：数据模型

### Message

消息是 MQLite 中的基本数据单元。

| 字段 | JSON 类型 | Protobuf 类型 | 说明 |
|------|-----------|---------------|------|
| id | string | string | 消息唯一标识 (UUID) |
| namespace | string | string | 所属命名空间 |
| topic | string | string | 所属 Topic |
| queue_id | int | int32 | 所在队列索引 |
| payload | any / base64 | bytes | 消息内容。HTTP 中为原始 JSON；gRPC 中为 bytes（base64 编码） |
| timestamp | int64 | int64 | 消息创建时间戳（纳秒级 Unix 时间） |
| headers | object | map\<string, string\> | 消息头键值对（可选） |

### TopicInfo

Topic 的元数据信息。

| 字段 | JSON 类型 | Protobuf 类型 | 说明 |
|------|-----------|---------------|------|
| name | string | string | Topic 名称 |
| queue_count | int | int32 | 当前队列数量 |
| version | uint64 | uint64 | 版本号，每次 resize 操作递增 |

---

## 接口速查表

### HTTP REST

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/v1/namespaces` | 创建 Namespace |
| DELETE | `/v1/namespaces/:namespace` | 删除 Namespace |
| GET | `/v1/namespaces` | 列出 Namespace |
| POST | `/v1/namespaces/:namespace/topics` | 创建 Topic |
| DELETE | `/v1/namespaces/:namespace/topics/:topic` | 删除 Topic |
| GET | `/v1/namespaces/:namespace/topics` | 列出 Topic |
| PUT | `/v1/namespaces/:namespace/topics/:topic/resize` | 调整队列数 |
| POST | `/v1/namespaces/:namespace/topics/:topic/rebalance` | 重平衡 |
| POST | `/v1/namespaces/:namespace/topics/:topic/publish` | 发布消息 |
| POST | `/v1/namespaces/:namespace/topics/:topic/queues/:queueId/consume` | 消费消息 |
| GET | `/v1/namespaces/:namespace/topics/:topic/queues/:queueId/subscribe` | SSE 订阅 |
| POST | `/v1/namespaces/:namespace/topics/:topic/queues/:queueId/ack` | 确认消息 |
| GET | `/health` | 健康检查 |

### gRPC (`mqlite.v1.MQLiteService`)

| 方法 | 类型 | 说明 |
|------|------|------|
| CreateNamespace | Unary | 创建 Namespace |
| DeleteNamespace | Unary | 删除 Namespace |
| ListNamespaces | Unary | 列出 Namespace |
| CreateTopic | Unary | 创建 Topic |
| DeleteTopic | Unary | 删除 Topic |
| ListTopics | Unary | 列出 Topic |
| ResizeTopic | Unary | 调整队列数 |
| RebalanceTopic | Unary | 重平衡 |
| Publish | Unary | 发布消息 |
| Consume | Unary | 消费消息 |
| Subscribe | Server Stream | 实时订阅 |
| Ack | Unary | 确认消息 |
