package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "mqlite/api/proto/gen"
)

// ================================================================
// Color helpers
// ================================================================

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorCyan   = "\033[36m"
	colorBold   = "\033[1m"
	colorDim    = "\033[2m"
)

func green(s string) string  { return colorGreen + s + colorReset }
func red(s string) string    { return colorRed + s + colorReset }
func cyan(s string) string   { return colorCyan + s + colorReset }
func yellow(s string) string { return colorYellow + s + colorReset }
func bold(s string) string   { return colorBold + s + colorReset }
func dim(s string) string    { return colorDim + s + colorReset }

// ================================================================
// HTTP Client
// ================================================================

type HTTPClient struct {
	baseURL string
	client  *http.Client
}

func NewHTTPClient(addr string) *HTTPClient {
	return &HTTPClient{
		baseURL: "http://" + addr,
		client:  &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *HTTPClient) doJSON(method, path string, body interface{}) (map[string]interface{}, error) {
	var reqBody io.Reader
	if body != nil {
		data, _ := json.Marshal(body)
		reqBody = bytes.NewReader(data)
	}
	req, err := http.NewRequest(method, c.baseURL+path, reqBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *HTTPClient) CreateNamespace(name string) error {
	r, err := c.doJSON("POST", "/v1/namespaces", map[string]string{"name": name})
	if err != nil {
		return err
	}
	printResult("CreateNamespace", r)
	return nil
}

func (c *HTTPClient) ListNamespaces() error {
	r, err := c.doJSON("GET", "/v1/namespaces", nil)
	if err != nil {
		return err
	}
	printResult("ListNamespaces", r)
	return nil
}

func (c *HTTPClient) CreateTopic(ns, name string, queueCount int) error {
	r, err := c.doJSON("POST", "/v1/namespaces/"+ns+"/topics", map[string]interface{}{
		"name": name, "queue_count": queueCount,
	})
	if err != nil {
		return err
	}
	printResult("CreateTopic", r)
	return nil
}

func (c *HTTPClient) ListTopics(ns string) error {
	r, err := c.doJSON("GET", "/v1/namespaces/"+ns+"/topics", nil)
	if err != nil {
		return err
	}
	printResult("ListTopics", r)
	return nil
}

func (c *HTTPClient) Publish(ns, topic string, payload interface{}, headers map[string]string) error {
	body := map[string]interface{}{"payload": payload}
	if headers != nil {
		body["headers"] = headers
	}
	r, err := c.doJSON("POST", "/v1/namespaces/"+ns+"/topics/"+topic+"/publish", body)
	if err != nil {
		return err
	}
	printResult("Publish", r)
	return nil
}

func (c *HTTPClient) Consume(ns, topic string, queueID, batchSize int, autoAck bool) error {
	r, err := c.doJSON("POST",
		fmt.Sprintf("/v1/namespaces/%s/topics/%s/queues/%d/consume", ns, topic, queueID),
		map[string]interface{}{"batch_size": batchSize, "auto_ack": autoAck})
	if err != nil {
		return err
	}
	printResult("Consume", r)
	return nil
}

func (c *HTTPClient) Ack(ns, topic string, queueID int, msgIDs []string) error {
	r, err := c.doJSON("POST",
		fmt.Sprintf("/v1/namespaces/%s/topics/%s/queues/%d/ack", ns, topic, queueID),
		map[string]interface{}{"message_ids": msgIDs})
	if err != nil {
		return err
	}
	printResult("Ack", r)
	return nil
}

// ================================================================
// TCP Client
// ================================================================

type TCPClient struct {
	conn   net.Conn
	reader *bufio.Reader
	writer *bufio.Writer
}

func NewTCPClient(addr string) (*TCPClient, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return &TCPClient{
		conn:   conn,
		reader: bufio.NewReaderSize(conn, 64*1024),
		writer: bufio.NewWriterSize(conn, 64*1024),
	}, nil
}

func (c *TCPClient) sendCommand(action string, data interface{}) (map[string]interface{}, error) {
	dataBytes, _ := json.Marshal(data)
	cmd := map[string]interface{}{
		"action": action,
		"data":   json.RawMessage(dataBytes),
	}
	payload, _ := json.Marshal(cmd)

	// Write frame: [4B length][1B encoding=0 (JSON)][payload]
	totalLen := uint32(1 + len(payload))
	if err := binary.Write(c.writer, binary.BigEndian, totalLen); err != nil {
		return nil, err
	}
	if err := c.writer.WriteByte(0); err != nil { // JSON encoding
		return nil, err
	}
	if _, err := c.writer.Write(payload); err != nil {
		return nil, err
	}
	if err := c.writer.Flush(); err != nil {
		return nil, err
	}

	// Read response frame
	var respLen uint32
	if err := binary.Read(c.reader, binary.BigEndian, &respLen); err != nil {
		return nil, err
	}
	if respLen < 1 {
		return nil, fmt.Errorf("invalid response frame")
	}
	// Read encoding byte
	if _, err := c.reader.ReadByte(); err != nil {
		return nil, err
	}
	respData := make([]byte, respLen-1)
	if _, err := io.ReadFull(c.reader, respData); err != nil {
		return nil, err
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respData, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *TCPClient) CreateNamespace(name string) error {
	r, err := c.sendCommand("create_namespace", map[string]string{"name": name})
	if err != nil {
		return err
	}
	printResult("CreateNamespace", r)
	return nil
}

func (c *TCPClient) ListNamespaces() error {
	r, err := c.sendCommand("list_namespaces", map[string]string{})
	if err != nil {
		return err
	}
	printResult("ListNamespaces", r)
	return nil
}

func (c *TCPClient) CreateTopic(ns, name string, queueCount int) error {
	r, err := c.sendCommand("create_topic", map[string]interface{}{
		"namespace": ns, "name": name, "queue_count": queueCount,
	})
	if err != nil {
		return err
	}
	printResult("CreateTopic", r)
	return nil
}

func (c *TCPClient) ListTopics(ns string) error {
	r, err := c.sendCommand("list_topics", map[string]string{"namespace": ns})
	if err != nil {
		return err
	}
	printResult("ListTopics", r)
	return nil
}

func (c *TCPClient) Publish(ns, topic string, payload interface{}) error {
	payloadBytes, _ := json.Marshal(payload)
	r, err := c.sendCommand("publish", map[string]interface{}{
		"namespace": ns, "topic": topic, "payload": json.RawMessage(payloadBytes),
	})
	if err != nil {
		return err
	}
	printResult("Publish", r)
	return nil
}

func (c *TCPClient) Consume(ns, topic string, queueID, batchSize int, autoAck bool) error {
	r, err := c.sendCommand("consume", map[string]interface{}{
		"namespace": ns, "topic": topic, "queue_id": queueID,
		"batch_size": batchSize, "auto_ack": autoAck,
	})
	if err != nil {
		return err
	}
	printResult("Consume", r)
	return nil
}

func (c *TCPClient) Close() {
	c.conn.Close()
}

// ================================================================
// gRPC Client
// ================================================================

type GRPCClient struct {
	conn   *grpc.ClientConn
	client pb.MQLiteServiceClient
}

func NewGRPCClient(addr string) (*GRPCClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &GRPCClient{
		conn:   conn,
		client: pb.NewMQLiteServiceClient(conn),
	}, nil
}

func (c *GRPCClient) CreateNamespace(name string) error {
	r, err := c.client.CreateNamespace(context.Background(), &pb.CreateNamespaceRequest{Name: name})
	if err != nil {
		return err
	}
	fmt.Printf("  %s success=%v message=%q\n", green("[OK]"), r.Success, r.Message)
	return nil
}

func (c *GRPCClient) ListNamespaces() error {
	r, err := c.client.ListNamespaces(context.Background(), &pb.ListNamespacesRequest{})
	if err != nil {
		return err
	}
	fmt.Printf("  %s namespaces=%v\n", green("[OK]"), r.Namespaces)
	return nil
}

func (c *GRPCClient) CreateTopic(ns, name string, queueCount int) error {
	r, err := c.client.CreateTopic(context.Background(), &pb.CreateTopicRequest{
		Namespace: ns, Name: name, QueueCount: int32(queueCount),
	})
	if err != nil {
		return err
	}
	fmt.Printf("  %s success=%v message=%q\n", green("[OK]"), r.Success, r.Message)
	return nil
}

func (c *GRPCClient) ListTopics(ns string) error {
	r, err := c.client.ListTopics(context.Background(), &pb.ListTopicsRequest{Namespace: ns})
	if err != nil {
		return err
	}
	for _, t := range r.Topics {
		fmt.Printf("  %s topic=%q queues=%d\n", green("[OK]"), t.Name, t.QueueCount)
	}
	return nil
}

func (c *GRPCClient) Publish(ns, topic string, payload []byte) error {
	r, err := c.client.Publish(context.Background(), &pb.PublishRequest{
		Namespace: ns, Topic: topic, Payload: payload, QueueId: -1,
	})
	if err != nil {
		return err
	}
	fmt.Printf("  %s message_id=%s queue_id=%d\n", green("[OK]"), r.MessageId, r.QueueId)
	return nil
}

func (c *GRPCClient) Consume(ns, topic string, queueID, batchSize int, autoAck bool) error {
	r, err := c.client.Consume(context.Background(), &pb.ConsumeRequest{
		Namespace: ns, Topic: topic, QueueId: int32(queueID),
		BatchSize: int32(batchSize), AutoAck: autoAck,
	})
	if err != nil {
		return err
	}
	if len(r.Messages) == 0 {
		fmt.Printf("  %s no messages\n", yellow("[EMPTY]"))
	}
	for _, m := range r.Messages {
		fmt.Printf("  %s id=%s queue=%d payload=%s\n",
			green("[MSG]"), m.Id[:8]+"...", m.QueueId, string(m.Payload))
	}
	return nil
}

func (c *GRPCClient) Subscribe(ns, topic string, queueID int, autoAck bool, count int) error {
	stream, err := c.client.Subscribe(context.Background(), &pb.SubscribeRequest{
		Namespace: ns, Topic: topic, QueueId: int32(queueID), AutoAck: autoAck,
	})
	if err != nil {
		return err
	}
	fmt.Printf("  %s Listening for messages (max %d)...\n", cyan("[SUB]"), count)
	for i := 0; i < count; i++ {
		msg, err := stream.Recv()
		if err != nil {
			fmt.Printf("  %s stream ended: %v\n", yellow("[END]"), err)
			return nil
		}
		fmt.Printf("  %s id=%s queue=%d payload=%s\n",
			green("[MSG]"), msg.Id[:8]+"...", msg.QueueId, string(msg.Payload))
	}
	return nil
}

func (c *GRPCClient) Ack(ns, topic string, queueID int, msgIDs []string) error {
	r, err := c.client.Ack(context.Background(), &pb.AckRequest{
		Namespace: ns, Topic: topic, QueueId: int32(queueID), MessageIds: msgIDs,
	})
	if err != nil {
		return err
	}
	fmt.Printf("  %s success=%v\n", green("[OK]"), r.Success)
	return nil
}

func (c *GRPCClient) Close() {
	c.conn.Close()
}

// ================================================================
// Helpers
// ================================================================

func printResult(op string, r map[string]interface{}) {
	data, _ := json.MarshalIndent(r, "  ", "  ")
	status := "ok"
	if s, ok := r["status"]; ok {
		status = fmt.Sprint(s)
	}
	if s, ok := r["success"]; ok {
		if s == true {
			status = "ok"
		} else {
			status = "error"
		}
	}
	if status == "ok" || status == "true" {
		fmt.Printf("  %s\n", green(string(data)))
	} else {
		fmt.Printf("  %s\n", red(string(data)))
	}
}

func printBanner() {
	fmt.Println(bold(cyan(`
  __  __  ___  _     _ _       
 |  \/  |/ _ \| |   (_) |_ ___ 
 | |\/| | | | | |   | | __/ _ \
 | |  | | |_| | |___| | ||  __/
 |_|  |_|\__\_\_____|_|\__\___|
                    CLI Client
`)))
}

func printHelp() {
	fmt.Println(bold("\n  Available Commands:"))
	fmt.Println()
	cmds := []struct{ cmd, desc string }{
		{"create-ns <name>", "Create a namespace"},
		{"list-ns", "List all namespaces"},
		{"create-topic <ns> <name> <queues>", "Create a topic with N queues"},
		{"list-topics <ns>", "List topics in a namespace"},
		{"pub <ns> <topic> <json-payload>", "Publish a message"},
		{"pub-batch <ns> <topic> <count>", "Publish N test messages"},
		{"consume <ns> <topic> <queueId> [batch] [autoack]", "Consume messages"},
		{"ack <ns> <topic> <queueId> <msgId1,msgId2,...>", "Acknowledge messages"},
		{"subscribe <ns> <topic> <queueId> [count]", "Subscribe via gRPC stream"},
		{"demo", "Run full demo (all features + work stealing)"},
		{"help", "Show this help"},
		{"quit / exit", "Exit"},
	}
	for _, c := range cmds {
		fmt.Printf("    %-50s %s\n", cyan(c.cmd), dim(c.desc))
	}
	fmt.Println()
}

// ================================================================
// Demo: Full feature walkthrough
// ================================================================

func runDemo(httpAddr string) {
	c := NewHTTPClient(httpAddr)
	sep := strings.Repeat("─", 60)

	fmt.Println(bold("\n  ══════════ MQLite Full Demo ══════════\n"))

	// Step 1
	fmt.Println(bold(cyan("  [Step 1] Create Namespace \"demo\"")))
	fmt.Println("  " + sep)
	c.CreateNamespace("demo")
	fmt.Println()

	// Step 2
	fmt.Println(bold(cyan("  [Step 2] Create Topic \"orders\" with 3 queues")))
	fmt.Println("  " + sep)
	c.CreateTopic("demo", "orders", 3)
	fmt.Println()

	// Step 3
	fmt.Println(bold(cyan("  [Step 3] Publish 12 messages (round-robin across 3 queues)")))
	fmt.Println("  " + sep)
	for i := 1; i <= 12; i++ {
		payload := map[string]interface{}{"order_id": i, "item": fmt.Sprintf("product-%d", i)}
		r, _ := c.doJSON("POST", "/v1/namespaces/demo/topics/orders/publish",
			map[string]interface{}{"payload": payload})
		qid := r["data"].(map[string]interface{})["queue_id"]
		fmt.Printf("    msg %2d -> queue %v\n", i, qid)
	}
	fmt.Println()

	// Step 4
	fmt.Println(bold(cyan("  [Step 4] Consume all from Queue 0 (expect 4)")))
	fmt.Println("  " + sep)
	c.Consume("demo", "orders", 0, 10, true)
	fmt.Println()

	// Step 5
	fmt.Println(bold(cyan("  [Step 5] Consume all from Queue 1 (expect 4)")))
	fmt.Println("  " + sep)
	c.Consume("demo", "orders", 1, 10, true)
	fmt.Println()

	// Step 6
	fmt.Println(bold(cyan("  [Step 6] Queue 2 still has 4 messages. Consume from empty Queue 0")))
	fmt.Println(bold(yellow("           >>> This triggers WORK STEALING from Queue 2! <<<")))
	fmt.Println("  " + sep)
	c.Consume("demo", "orders", 0, 10, true)
	fmt.Println()

	// Step 7
	fmt.Println(bold(cyan("  [Step 7] Consume remaining from Queue 2")))
	fmt.Println("  " + sep)
	c.Consume("demo", "orders", 2, 10, true)
	fmt.Println()

	// Step 8: Manual ack demo
	fmt.Println(bold(cyan("  [Step 8] Manual Ack demo - publish, consume without auto-ack, then ack")))
	fmt.Println("  " + sep)
	c.Publish("demo", "orders", map[string]interface{}{"ack_test": true}, nil)
	r, _ := c.doJSON("POST", "/v1/namespaces/demo/topics/orders/queues/0/consume",
		map[string]interface{}{"batch_size": 1, "auto_ack": false})
	fmt.Printf("    Consumed (no auto-ack):\n")
	printResult("Consume", r)
	if data, ok := r["data"].([]interface{}); ok && len(data) > 0 {
		msg := data[0].(map[string]interface{})
		msgID := msg["id"].(string)
		fmt.Printf("    Acking message %s...\n", msgID[:8]+"...")
		c.Ack("demo", "orders", 0, []string{msgID})
	}
	fmt.Println()

	// Step 9
	fmt.Println(bold(cyan("  [Step 9] List Namespaces & Topics")))
	fmt.Println("  " + sep)
	c.ListNamespaces()
	c.ListTopics("demo")
	fmt.Println()

	fmt.Println(bold(green("  ══════════ Demo Complete ══════════\n")))
}

// ================================================================
// Demo: Multi-protocol test
// ================================================================

func runProtocolDemo(httpAddr, grpcAddr, tcpAddr string) {
	sep := strings.Repeat("─", 60)
	fmt.Println(bold("\n  ══════════ Multi-Protocol Demo ══════════\n"))

	// HTTP
	fmt.Println(bold(cyan("  [HTTP] Creating namespace and topic via HTTP REST")))
	fmt.Println("  " + sep)
	hc := NewHTTPClient(httpAddr)
	hc.CreateNamespace("multi")
	hc.CreateTopic("multi", "events", 2)
	fmt.Println()

	// gRPC
	fmt.Println(bold(cyan("  [gRPC] Publishing 4 messages via gRPC")))
	fmt.Println("  " + sep)
	gc, err := NewGRPCClient(grpcAddr)
	if err != nil {
		fmt.Printf("  %s gRPC connect failed: %v\n", red("[ERR]"), err)
		return
	}
	defer gc.Close()
	for i := 1; i <= 4; i++ {
		payload, _ := json.Marshal(map[string]interface{}{"event": fmt.Sprintf("grpc-event-%d", i)})
		gc.Publish("multi", "events", payload)
	}
	fmt.Println()

	// TCP
	fmt.Println(bold(cyan("  [TCP] Consuming messages via TCP")))
	fmt.Println("  " + sep)
	tc, err := NewTCPClient(tcpAddr)
	if err != nil {
		fmt.Printf("  %s TCP connect failed: %v\n", red("[ERR]"), err)
		return
	}
	defer tc.Close()
	tc.Consume("multi", "events", 0, 5, true)
	tc.Consume("multi", "events", 1, 5, true)
	fmt.Println()

	// gRPC list
	fmt.Println(bold(cyan("  [gRPC] Listing namespaces via gRPC")))
	fmt.Println("  " + sep)
	gc.ListNamespaces()
	fmt.Println()

	fmt.Println(bold(green("  ══════════ Multi-Protocol Demo Complete ══════════\n")))
}

// ================================================================
// Interactive REPL
// ================================================================

func runREPL(httpAddr, grpcAddr, tcpAddr string) {
	hc := NewHTTPClient(httpAddr)

	var gc *GRPCClient
	var tc *TCPClient

	scanner := bufio.NewScanner(os.Stdin)

	printBanner()
	fmt.Printf("  Server: HTTP=%s  gRPC=%s  TCP=%s\n", cyan(httpAddr), cyan(grpcAddr), cyan(tcpAddr))
	printHelp()

	for {
		fmt.Print(bold("mqlite> "))
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.Fields(line)
		cmd := parts[0]
		args := parts[1:]

		var err error
		switch cmd {

		case "create-ns":
			if len(args) < 1 {
				fmt.Println(red("  Usage: create-ns <name>"))
				continue
			}
			err = hc.CreateNamespace(args[0])

		case "list-ns":
			err = hc.ListNamespaces()

		case "create-topic":
			if len(args) < 3 {
				fmt.Println(red("  Usage: create-topic <ns> <name> <queue_count>"))
				continue
			}
			qc, _ := strconv.Atoi(args[2])
			err = hc.CreateTopic(args[0], args[1], qc)

		case "list-topics":
			if len(args) < 1 {
				fmt.Println(red("  Usage: list-topics <ns>"))
				continue
			}
			err = hc.ListTopics(args[0])

		case "pub":
			if len(args) < 3 {
				fmt.Println(red("  Usage: pub <ns> <topic> <json-payload>"))
				continue
			}
			payloadStr := strings.Join(args[2:], " ")
			var payload interface{}
			if jsonErr := json.Unmarshal([]byte(payloadStr), &payload); jsonErr != nil {
				payload = payloadStr // treat as string
			}
			err = hc.Publish(args[0], args[1], payload, nil)

		case "pub-batch":
			if len(args) < 3 {
				fmt.Println(red("  Usage: pub-batch <ns> <topic> <count>"))
				continue
			}
			count, _ := strconv.Atoi(args[2])
			for i := 1; i <= count; i++ {
				hc.Publish(args[0], args[1], map[string]interface{}{
					"seq": i, "data": fmt.Sprintf("batch-msg-%d", i),
				}, nil)
			}

		case "consume":
			if len(args) < 3 {
				fmt.Println(red("  Usage: consume <ns> <topic> <queueId> [batch_size] [auto_ack]"))
				continue
			}
			qid, _ := strconv.Atoi(args[2])
			batch := 1
			autoAck := true
			if len(args) > 3 {
				batch, _ = strconv.Atoi(args[3])
			}
			if len(args) > 4 {
				autoAck = args[4] == "true" || args[4] == "1"
			}
			err = hc.Consume(args[0], args[1], qid, batch, autoAck)

		case "ack":
			if len(args) < 4 {
				fmt.Println(red("  Usage: ack <ns> <topic> <queueId> <msgId1,msgId2,...>"))
				continue
			}
			qid, _ := strconv.Atoi(args[2])
			ids := strings.Split(args[3], ",")
			err = hc.Ack(args[0], args[1], qid, ids)

		case "subscribe":
			if len(args) < 3 {
				fmt.Println(red("  Usage: subscribe <ns> <topic> <queueId> [count]"))
				continue
			}
			if gc == nil {
				gc, err = NewGRPCClient(grpcAddr)
				if err != nil {
					fmt.Printf("  %s gRPC connect failed: %v\n", red("[ERR]"), err)
					continue
				}
			}
			qid, _ := strconv.Atoi(args[2])
			count := 5
			if len(args) > 3 {
				count, _ = strconv.Atoi(args[3])
			}
			err = gc.Subscribe(args[0], args[1], qid, true, count)

		case "tcp-pub":
			if len(args) < 3 {
				fmt.Println(red("  Usage: tcp-pub <ns> <topic> <json-payload>"))
				continue
			}
			if tc == nil {
				tc, err = NewTCPClient(tcpAddr)
				if err != nil {
					fmt.Printf("  %s TCP connect failed: %v\n", red("[ERR]"), err)
					continue
				}
			}
			payloadStr := strings.Join(args[2:], " ")
			var payload interface{}
			if jsonErr := json.Unmarshal([]byte(payloadStr), &payload); jsonErr != nil {
				payload = payloadStr
			}
			err = tc.Publish(args[0], args[1], payload)

		case "tcp-consume":
			if len(args) < 3 {
				fmt.Println(red("  Usage: tcp-consume <ns> <topic> <queueId> [batch_size]"))
				continue
			}
			if tc == nil {
				tc, err = NewTCPClient(tcpAddr)
				if err != nil {
					fmt.Printf("  %s TCP connect failed: %v\n", red("[ERR]"), err)
					continue
				}
			}
			qid, _ := strconv.Atoi(args[2])
			batch := 1
			if len(args) > 3 {
				batch, _ = strconv.Atoi(args[3])
			}
			err = tc.Consume(args[0], args[1], qid, batch, true)

		case "demo":
			runDemo(httpAddr)

		case "demo-protocol":
			runProtocolDemo(httpAddr, grpcAddr, tcpAddr)

		case "help", "?":
			printHelp()

		case "quit", "exit", "q":
			fmt.Println(dim("  Bye!"))
			if gc != nil {
				gc.Close()
			}
			if tc != nil {
				tc.Close()
			}
			return

		default:
			fmt.Printf("  %s Unknown command: %s (type 'help' for usage)\n", red("[ERR]"), cmd)
		}

		if err != nil {
			fmt.Printf("  %s %v\n", red("[ERR]"), err)
		}
	}
}

// ================================================================
// Main
// ================================================================

func main() {
	httpAddr := flag.String("http", "localhost:8080", "HTTP server address")
	grpcAddr := flag.String("grpc", "localhost:9090", "gRPC server address")
	tcpAddr := flag.String("tcp", "localhost:7070", "TCP server address")
	mode := flag.String("mode", "repl", "Mode: repl | demo | demo-protocol")
	flag.Parse()

	switch *mode {
	case "demo":
		runDemo(*httpAddr)
	case "demo-protocol":
		runProtocolDemo(*httpAddr, *grpcAddr, *tcpAddr)
	default:
		runREPL(*httpAddr, *grpcAddr, *tcpAddr)
	}
}
