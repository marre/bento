package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"

	"bytes"
	"io"
	"net"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	xdgscram "github.com/xdg-go/scram"

	"github.com/warpstreamlabs/bento/public/service"
)

func TestKafkaServerInputConfig(t *testing.T) {
	tests := []struct {
		name    string
		config  string
		wantErr bool
	}{
		{
			name: "valid minimal config",
			config: `
address: "127.0.0.1:19092"
`,
			wantErr: false,
		},
		{
			name: "valid config with topics",
			config: `
address: "127.0.0.1:19092"
topics:
  - test-topic
  - events
`,
			wantErr: false,
		},
		{
			name: "valid config with timeout",
			config: `
address: "127.0.0.1:19092"
timeout: "10s"
max_message_bytes: 2097152
`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spec := kafkaServerInputConfig()
			env := service.NewEnvironment()

			parsed, err := spec.ParseYAML(tt.config, env)
			require.NoError(t, err)

			_, err = newKafkaServerInputFromConfig(parsed, service.MockResources())
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestKafkaServerInputBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19092"
timeout: "5s"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	// Connect the input
	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	// Give the server a moment to start listening
	time.Sleep(100 * time.Millisecond)

	// Create a franz-go producer client
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19092"),
		kgo.RequestTimeoutOverhead(5*time.Second),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelDebug, nil)),
	)
	require.NoError(t, err)
	defer client.Close()

	// Produce a test message
	testTopic := "test-topic"
	testKey := "test-key"
	testValue := "test-value"

	record := &kgo.Record{
		Topic: testTopic,
		Key:   []byte(testKey),
		Value: []byte(testValue),
		Headers: []kgo.RecordHeader{
			{Key: "header1", Value: []byte("value1")},
		},
	}

	// Send message asynchronously and wait for acknowledgment
	produceChan := make(chan error, 1)
	t.Logf("Starting producer goroutine")
	go func() {
		t.Logf("Producer: before ProduceSync")
		results := client.ProduceSync(ctx, record)
		t.Logf("Producer: after ProduceSync, results len=%d err=%v", len(results), results[0].Err)
		if len(results) > 0 {
			produceChan <- results[0].Err
		} else {
			produceChan <- fmt.Errorf("no results")
		}
	}()

	// Read message from input
	batch, ackFn, err := input.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	msg := batch[0]

	// Verify message content
	msgBytes, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, testValue, string(msgBytes))

	// Verify metadata
	topic, exists := msg.MetaGet("kafka_server_topic")
	assert.True(t, exists)
	assert.Equal(t, testTopic, topic)

	key, exists := msg.MetaGet("kafka_server_key")
	assert.True(t, exists)
	assert.Equal(t, testKey, key)

	partitionAny, exists := msg.MetaGetMut("kafka_server_partition")
	assert.True(t, exists)
	t.Logf("Got partition: value=%v, type=%T", partitionAny, partitionAny)
	partition, ok := partitionAny.(int)
	assert.True(t, ok, "partition should be int")
	assert.Equal(t, 0, partition)

	// Verify header
	header1, exists := msg.MetaGet("header1")
	assert.True(t, exists)
	assert.Equal(t, "value1", header1)

	// Acknowledge the message
	err = ackFn(ctx, nil)
	require.NoError(t, err)

	// Verify producer received acknowledgment
	select {
	case err := <-produceChan:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for produce acknowledgment")
	}
}

func TestKafkaServerInputMultipleMessages(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19093"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create producer
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19093"),
		kgo.ProducerBatchCompression(kgo.NoCompression()), // Disable compression for testing
	)
	require.NoError(t, err)
	defer client.Close()

	// Produce multiple messages
	numMessages := 5
	testTopic := "multi-test"

	for i := 0; i < numMessages; i++ {
		record := &kgo.Record{
			Topic: testTopic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}

		go func() {
			client.Produce(ctx, record, func(r *kgo.Record, err error) {
				// Callback - could check errors here
			})
		}()
	}

	// Read messages
	receivedCount := 0
	timeout := time.After(10 * time.Second)

	for receivedCount < numMessages {
		select {
		case <-timeout:
			t.Fatalf("Timeout: only received %d of %d messages", receivedCount, numMessages)
		default:
			readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
			batch, ackFn, err := input.ReadBatch(readCtx)
			readCancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					continue
				}
				t.Fatalf("Failed to read batch: %v", err)
			}

			receivedCount += len(batch)

			// Acknowledge
			err = ackFn(ctx, nil)
			require.NoError(t, err)
		}
	}

	assert.Equal(t, numMessages, receivedCount)
}

func TestKafkaServerInputTopicFiltering(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input with topic filtering
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19094"
topics:
  - allowed-topic
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create producer
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19094"),
	)
	require.NoError(t, err)
	defer client.Close()

	// Try to produce to allowed topic
	allowedRecord := &kgo.Record{
		Topic: "allowed-topic",
		Value: []byte("allowed"),
	}

	var allowedErr error
	go func() {
		results := client.ProduceSync(ctx, allowedRecord)
		if len(results) > 0 {
			allowedErr = results[0].Err
		}
	}()

	// Read the allowed message
	readCtx, readCancel := context.WithTimeout(ctx, 5*time.Second)
	batch, ackFn, err := input.ReadBatch(readCtx)
	readCancel()

	require.NoError(t, err)
	require.Len(t, batch, 1)

	msgBytes, err := batch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "allowed", string(msgBytes))

	err = ackFn(ctx, nil)
	require.NoError(t, err)

	// Verify no error from producer
	time.Sleep(100 * time.Millisecond)
	assert.NoError(t, allowedErr)

	// Try to produce to disallowed topic - the server should reject it silently
	disallowedRecord := &kgo.Record{
		Topic: "disallowed-topic",
		Value: []byte("disallowed"),
	}

	go func() {
		client.Produce(ctx, disallowedRecord, func(r *kgo.Record, err error) {
			// Callback
		})
	}()

	// Try to read - should timeout since message was filtered
	readCtx2, readCancel2 := context.WithTimeout(ctx, 2*time.Second)
	_, _, err = input.ReadBatch(readCtx2)
	readCancel2()

	// Should timeout since no message should be received
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestKafkaServerInputClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19095"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	// Close should not error
	err = input.Close(ctx)
	assert.NoError(t, err)

	// Reading after close should return error
	_, _, err = input.ReadBatch(ctx)
	assert.Error(t, err)
}

func TestKafkaServerInputNullKeyAndValue(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19096"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19096"),
	)
	require.NoError(t, err)
	defer client.Close()

	// Produce message with null value (tombstone)
	record := &kgo.Record{
		Topic: "test-topic",
		Key:   []byte("key-with-null-value"),
		Value: nil,
	}

	go func() {
		client.ProduceSync(ctx, record)
	}()

	// Read message
	batch, ackFn, err := input.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	msg := batch[0]
	msgBytes, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Empty(t, msgBytes) // Null value should be empty

	// Key should still be present
	key, exists := msg.MetaGet("kafka_server_key")
	assert.True(t, exists)
	assert.Equal(t, "key-with-null-value", key)

	err = ackFn(ctx, nil)
	require.NoError(t, err)
}

// Helper that creates a kafkaServerInput with an SCRAM-SHA-256 user for tests.
func makeSCRAMTestServer(t *testing.T, username, password string) *kafkaServerInput {
	k := &kafkaServerInput{
		logger:         service.MockResources().Logger(),
		scramSHA256:    make(map[string]xdgscram.StoredCredentials),
		saslMechanisms: []string{"SCRAM-SHA-256"},
		saslEnabled:    true,
		timeout:        5 * time.Second,
	}
	creds, err := generateSCRAMCredentials("SCRAM-SHA-256", username, password)
	if err != nil {
		t.Fatalf("failed to create scram credentials: %v", err)
	}
	k.scramSHA256[username] = creds
	return k
}

// Test that SASLAuthenticate requests encoded with an Int32 BYTES prefix are
// properly parsed and core SCRAM server-first responses do not contain NUL.
func TestHandleSaslAuthenticate_Int32Bytes_ParsingAndTrim(t *testing.T) {
	k := makeSCRAMTestServer(t, "scramuser", "password")

	// Build client-first payload and append trailing NULs to simulate padding
	payload := []byte("n,,n=scramuser,r=nonce12345")
	payload = append(payload, 0, 0)

	data := make([]byte, 4+len(payload))
	binary.BigEndian.PutUint32(data[0:4], uint32(len(payload)))
	copy(data[4:], payload)

	// Prepare a dummy connection pair
	sConn, cConn := net.Pipe()
	defer sConn.Close()
	defer cConn.Close()

	connState := &connectionState{scramMechanism: "SCRAM-SHA-256"}

	done := make(chan error, 1)
	go func() {
		_, err := k.handleSaslAuthenticate(sConn, 1, 1, data, 2, connState)
		done <- err
	}()

	// Read response from client side
	var size int32
	if err := binary.Read(cConn, binary.BigEndian, &size); err != nil {
		t.Fatalf("failed to read size: %v", err)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(cConn, buf); err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	// Parse response as SASLAuthenticate (skip correlation id)
	// Locate the server-first message within the response body by searching for "r="
	body := buf[4:]
	start := bytes.Index(body, []byte("r="))
	if start < 0 {
		t.Fatalf("could not find server-first 'r=' in response: %x", body)
	}
	// Slice until first NUL (if present) to avoid trailing padding
	endNull := bytes.IndexByte(body[start:], 0)
	var token []byte
	if endNull >= 0 {
		token = body[start : start+endNull]
	} else {
		token = body[start:]
	}
	if len(token) == 0 {
		t.Fatalf("empty server-first token")
	}
	if bytes.Contains(token, []byte{0}) {
		t.Fatalf("server-first token contains NULs: %x", token)
	}

	// Ensure the handler returned without error
	if err := <-done; err != nil {
		t.Fatalf("handleSaslAuthenticate returned error: %v", err)
	}
}

// Test that SASLAuthenticate requests encoded as CompactBytes are parsed correctly.
func TestHandleSaslAuthenticate_CompactBytes_Parsing(t *testing.T) {
	k := makeSCRAMTestServer(t, "scramuser", "password")

	payload := []byte("n,,n=scramuser,r=nonce_compact")
	// Kafka compact encoding uses uvarint of (len + 1). For small values this is one byte.
	prefix := byte(len(payload) + 1)
	data := append([]byte{prefix}, payload...)

	sConn, cConn := net.Pipe()
	defer sConn.Close()
	defer cConn.Close()

	connState := &connectionState{scramMechanism: "SCRAM-SHA-256"}

	done := make(chan error, 1)
	go func() {
		_, err := k.handleSaslAuthenticate(sConn, 2, 2, data, 2, connState)
		done <- err
	}()

	// Read response from client side
	var size int32
	if err := binary.Read(cConn, binary.BigEndian, &size); err != nil {
		t.Fatalf("failed to read size: %v", err)
	}
	buf := make([]byte, size)
	if _, err := io.ReadFull(cConn, buf); err != nil {
		t.Fatalf("failed to read body: %v", err)
	}

	// Locate "r=" in body for compact case as well
	body2 := buf[4:]
	start2 := bytes.Index(body2, []byte("r="))
	if start2 < 0 {
		t.Fatalf("could not find server-first 'r=' in compact response: %x", body2)
	}
	endNull2 := bytes.IndexByte(body2[start2:], 0)
	var token2 []byte
	if endNull2 >= 0 {
		token2 = body2[start2 : start2+endNull2]
	} else {
		token2 = body2[start2:]
	}
	if len(token2) == 0 {
		t.Fatalf("empty server-first token in compact case")
	}
	if bytes.Contains(token2, []byte{0}) {
		t.Fatalf("server-first token contains NULs in compact case: %x", token2)
	}
	if err := <-done; err != nil {
		t.Fatalf("handleSaslAuthenticate returned error: %v", err)
	}
}

// Test that leading control bytes (0x00/0x01) in the client payload are trimmed
// by handleSCRAMAuth before processing.
func TestHandleSCRAMAuth_TrimsLeadingControlBytes(t *testing.T) {
	k := makeSCRAMTestServer(t, "scramuser", "password")
	connState := &connectionState{scramMechanism: "SCRAM-SHA-256"}

	// Construct a client-first message with a leading 0x01 control byte
	payload := append([]byte{0x01}, []byte("n,,n=scramuser,r=nonce_lead")...)

	resp := kmsg.NewSASLAuthenticateResponse()
	resp.SetVersion(2)

	ok, err := k.handleSCRAMAuth(3, payload, connState, &resp)
	if err != nil {
		t.Fatalf("handleSCRAMAuth returned error: %v", err)
	}
	// First-step SCRAM should not be complete (conversation continues)
	if ok {
		t.Fatalf("expected authentication not to be completed for client-first message")
	}
	if len(resp.SASLAuthBytes) == 0 {
		t.Fatalf("expected SCRAM server-first response SASLAuthBytes, got none")
	}
	if bytes.Contains(resp.SASLAuthBytes, []byte{0}) {
		t.Fatalf("server response contains NUL bytes: %x", resp.SASLAuthBytes)
	}
}

func TestKafkaServerInputAcks0(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19097"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19097"),
	)
	require.NoError(t, err)
	defer client.Close()

	// Produce message with acks=0 (NoResponse)
	record := &kgo.Record{
		Topic: "test-topic",
		Value: []byte("test-value-acks-0"),
	}

	// We can't easily force kgo to use acks=0 per record, but we can configure the client
	// However, for this test we want to verify the server handles it.
	// The franz-go client defaults to acks=all.
	// To test acks=0, we might need to construct a raw request or configure the client.
	// kgo.RequiredAcks(kgo.NoAck())

	clientAcks0, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19097"),
		kgo.RequiredAcks(kgo.NoAck()),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer clientAcks0.Close()

	done := make(chan struct{})
	go func() {
		// ProduceAsync with NoAck should return immediately
		clientAcks0.Produce(ctx, record, nil)
		close(done)
	}()

	select {
	case <-done:
		// Success - produce returned
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for produce with acks=0")
	}

	// Read message
	batch, ackFn, err := input.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	msgBytes, err := batch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "test-value-acks-0", string(msgBytes))

	err = ackFn(ctx, nil)
	require.NoError(t, err)
}

func TestKafkaServerInputAcks1(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19098"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Configure client for acks=1 (LeaderAck)
	clientAcks1, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19098"),
		kgo.RequiredAcks(kgo.LeaderAck()),
		kgo.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer clientAcks1.Close()

	record := &kgo.Record{
		Topic: "test-topic",
		Value: []byte("test-value-acks-1"),
	}

	produceChan := make(chan error, 1)
	go func() {
		results := clientAcks1.ProduceSync(ctx, record)
		if len(results) > 0 {
			produceChan <- results[0].Err
		}
	}()

	// Read message
	batch, ackFn, err := input.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	msgBytes, err := batch[0].AsBytes()
	require.NoError(t, err)
	assert.Equal(t, "test-value-acks-1", string(msgBytes))

	// Acknowledge the message
	err = ackFn(ctx, nil)
	require.NoError(t, err)

	// Verify producer received acknowledgment
	select {
	case err := <-produceChan:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for produce acknowledgment")
	}
}

// Quick round-trip test: ensure the metadata response we construct can be
// appended-to a buffer and parsed back by kmsg to avoid the 'not enough
// data' errors the client observed in integration tests.
func TestApiVersionsResponseStructure(t *testing.T) {
	resp := kmsg.NewApiVersionsResponse()
	resp.SetVersion(3)
	resp.ErrorCode = 0
	resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
		{ApiKey: 18, MinVersion: 0, MaxVersion: 3},
		{ApiKey: 3, MinVersion: 0, MaxVersion: 12},
		{ApiKey: 0, MinVersion: 0, MaxVersion: 9},
	}

	// Build buffer like sendResponse does
	buf := kbin.AppendInt32(nil, 0) // correlationID = 0
	buf = resp.AppendTo(buf)

	t.Logf("ApiVersions response hex (total %d bytes): %x", len(buf), buf)
	t.Logf("Response body (after correlationID, %d bytes): %x", len(buf)-4, buf[4:])
	t.Logf("Is flexible: %v", resp.IsFlexible())
}

func TestMetadataResponseRoundTrip(t *testing.T) {
	resp := kmsg.NewMetadataResponse()
	resp.SetVersion(12)

	// fill sample broker and topic like server does
	resp.Brokers = []kmsg.MetadataResponseBroker{{NodeID: 1, Host: "127.0.0.1", Port: 19092}}
	clusterID := "kafka-server-cluster"
	resp.ClusterID = &clusterID
	resp.ControllerID = 1

	resp.Topics = []kmsg.MetadataResponseTopic{
		{
			Topic:      kmsg.StringPtr("test-topic"),
			ErrorCode:  0,
			Partitions: []kmsg.MetadataResponseTopicPartition{{Partition: 0, Leader: 1, Replicas: []int32{1}, ISR: []int32{1}, ErrorCode: 0}},
		},
	}

	// Build buffer as sendResponse would (correlation id + message bytes)
	buf := kbin.AppendInt32(nil, 123)
	buf = resp.AppendTo(buf)

	// Debug: print the hex
	t.Logf("Metadata response hex (total %d bytes): %x", len(buf), buf)
	t.Logf("Response body (after correlationID, %d bytes): %x", len(buf)-4, buf[4:])

	// Now parse back after skipping correlation id
	parsed := kmsg.NewMetadataResponse()
	parsed.SetVersion(12)
	if err := parsed.ReadFrom(buf[4:]); err != nil {
		t.Fatalf("metadata response failed to parse: %v", err)
	}

	// Verify parsed data
	require.Equal(t, 1, len(parsed.Brokers), "should have 1 broker")
	require.Equal(t, "127.0.0.1", parsed.Brokers[0].Host, "broker host")
	require.Equal(t, int32(19092), parsed.Brokers[0].Port, "broker port")
	require.Equal(t, 1, len(parsed.Topics), "should have 1 topic")
	if parsed.Topics[0].Topic != nil {
		require.Equal(t, "test-topic", *parsed.Topics[0].Topic, "topic name")
	}
}

func TestKafkaServerInputCompression(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19099"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create producer with Snappy compression (default when batching)
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19099"),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)
	require.NoError(t, err)
	defer client.Close()

	// Produce multiple compressed messages
	numMessages := 5
	testTopic := "compressed-test"

	for i := 0; i < numMessages; i++ {
		record := &kgo.Record{
			Topic: testTopic,
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}

		go func() {
			client.Produce(ctx, record, func(r *kgo.Record, err error) {
				// Callback
			})
		}()
	}

	// Read messages
	receivedCount := 0
	timeout := time.After(10 * time.Second)

	for receivedCount < numMessages {
		select {
		case <-timeout:
			t.Fatalf("Timeout: only received %d of %d compressed messages", receivedCount, numMessages)
		default:
			readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
			batch, ackFn, err := input.ReadBatch(readCtx)
			readCancel()

			if err != nil {
				if err == context.DeadlineExceeded {
					continue
				}
				t.Fatalf("Failed to read batch: %v", err)
			}

			receivedCount += len(batch)

			// Verify metadata exists
			for _, msg := range batch {
				topic, exists := msg.MetaGet("kafka_server_topic")
				assert.True(t, exists)
				assert.Equal(t, testTopic, topic)
			}

			// Acknowledge
			err = ackFn(ctx, nil)
			require.NoError(t, err)
		}
	}

	assert.Equal(t, numMessages, receivedCount)
	t.Logf("Successfully received %d compressed messages", receivedCount)
}

func TestKafkaServerInputSASLPlain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input with SASL PLAIN authentication
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19100"
sasl:
  - mechanism: PLAIN
    username: "testuser"
    password: "testpass"
  - mechanism: PLAIN
    username: "anotheruser"
    password: "anotherpass"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	t.Logf("Creating client with SASL PLAIN authentication")

	// Create client with SASL PLAIN authentication
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19100"),
		kgo.SASL(plain.Plain(func(context.Context) (plain.Auth, error) {
			t.Logf("SASL PLAIN auth callback called")
			return plain.Auth{
				User: "testuser",
				Pass: "testpass",
			}, nil
		})),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, func() string {
			return "[CLIENT] "
		})),
	)
	require.NoError(t, err)
	defer client.Close()

	t.Logf("Client created successfully")

	// Produce a test message
	testTopic := "authenticated-topic"
	testValue := "authenticated-message"

	record := &kgo.Record{
		Topic: testTopic,
		Value: []byte(testValue),
	}

	produceChan := make(chan error, 1)
	go func() {
		t.Logf("Producer goroutine: calling ProduceSync")
		results := client.ProduceSync(ctx, record)
		t.Logf("Producer goroutine: ProduceSync returned, results len=%d", len(results))
		if len(results) > 0 {
			produceChan <- results[0].Err
		}
	}()

	t.Logf("Calling ReadBatch")
	// Read message from input
	batch, ackFn, err := input.ReadBatch(ctx)
	t.Logf("ReadBatch returned: batch len=%d, err=%v", len(batch), err)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	msg := batch[0]

	// Verify message content
	msgBytes, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, testValue, string(msgBytes))

	// Verify metadata
	topic, exists := msg.MetaGet("kafka_server_topic")
	assert.True(t, exists)
	assert.Equal(t, testTopic, topic)

	// Acknowledge the message
	err = ackFn(ctx, nil)
	require.NoError(t, err)

	// Verify producer received acknowledgment
	select {
	case err := <-produceChan:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for produce acknowledgment")
	}
}

func TestKafkaServerInputSASLFailedAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input with SASL PLAIN authentication
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19101"
sasl:
  - mechanism: PLAIN
    username: "validuser"
    password: "validpass"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create client with WRONG credentials
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19101"),
		kgo.SASL(plain.Plain(func(context.Context) (plain.Auth, error) {
			return plain.Auth{
				User: "validuser",
				Pass: "wrongpass",
			}, nil
		})),
	)
	require.NoError(t, err)
	defer client.Close()

	// Try to produce a message - should fail
	testTopic := "test-topic"
	testValue := "test-value"

	record := &kgo.Record{
		Topic: testTopic,
		Value: []byte(testValue),
	}

	results := client.ProduceSync(ctx, record)
	require.Len(t, results, 1)

	// Should get an authentication error
	assert.Error(t, results[0].Err)
	t.Logf("Expected authentication error received: %v", results[0].Err)
}

func TestKafkaServerInputSASLWithoutAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input with SASL enabled
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19102"
sasl:
  - mechanism: PLAIN
    username: "testuser"
    password: "testpass"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create client WITHOUT SASL authentication
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19102"),
	)
	require.NoError(t, err)
	defer client.Close()

	// Try to produce a message - should fail
	testTopic := "test-topic"
	testValue := "test-value"

	record := &kgo.Record{
		Topic: testTopic,
		Value: []byte(testValue),
	}

	results := client.ProduceSync(ctx, record)
	require.Len(t, results, 1)

	// Should get an error due to missing authentication
	assert.Error(t, results[0].Err)
	t.Logf("Expected authentication error received: %v", results[0].Err)
}

func TestKafkaServerInputSCRAMSHA256(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input with SCRAM-SHA-256 authentication
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19103"
sasl:
  - mechanism: SCRAM-SHA-256
    username: "scramuser"
    password: "scrampass"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create client with SCRAM-SHA-256 authentication
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19103"),
		kgo.SASL(scram.Sha256(func(context.Context) (scram.Auth, error) {
			t.Logf("SCRAM-SHA-256 auth callback called")
			return scram.Auth{
				User: "scramuser",
				Pass: "scrampass",
			}, nil
		})),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelDebug, func() string {
			return "[CLIENT] "
		})),
	)
	require.NoError(t, err)
	defer client.Close()

	t.Logf("Client created successfully")

	// Produce a message
	testTopic := "test-topic"
	testValue := "test-value-scram"

	record := &kgo.Record{
		Topic: testTopic,
		Value: []byte(testValue),
	}

	// Use channel to synchronize producer and consumer
	produceChan := make(chan error, 1)
	go func() {
		results := client.ProduceSync(ctx, record)
		if len(results) > 0 {
			produceChan <- results[0].Err
		}
	}()

	// Read message from input
	batch, ackFn, err := input.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	msg := batch[0]

	// Verify message content
	msgBytes, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, testValue, string(msgBytes))

	// Verify metadata
	topic, exists := msg.MetaGet("kafka_server_topic")
	assert.True(t, exists)
	assert.Equal(t, testTopic, topic)

	// Acknowledge the message
	err = ackFn(ctx, nil)
	require.NoError(t, err)

	// Verify producer received acknowledgment
	select {
	case err := <-produceChan:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for produce acknowledgment")
	}
}

func TestKafkaServerInputSCRAMSHA512(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input with SCRAM-SHA-512 authentication
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19104"
sasl:
  - mechanism: SCRAM-SHA-512
    username: "scramuser512"
    password: "scrampass512"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create client with SCRAM-SHA-512 authentication
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19104"),
		kgo.SASL(scram.Sha512(func(context.Context) (scram.Auth, error) {
			return scram.Auth{
				User: "scramuser512",
				Pass: "scrampass512",
			}, nil
		})),
	)
	require.NoError(t, err)
	defer client.Close()

	// Produce a message
	testTopic := "test-topic-512"
	testValue := "test-value-scram-512"

	record := &kgo.Record{
		Topic: testTopic,
		Value: []byte(testValue),
	}

	// Use channel to synchronize producer and consumer
	produceChan := make(chan error, 1)
	go func() {
		results := client.ProduceSync(ctx, record)
		if len(results) > 0 {
			produceChan <- results[0].Err
		}
	}()

	// Read message from input
	batch, ackFn, err := input.ReadBatch(ctx)
	require.NoError(t, err)
	require.Len(t, batch, 1)

	msg := batch[0]

	// Verify message content
	msgBytes, err := msg.AsBytes()
	require.NoError(t, err)
	assert.Equal(t, testValue, string(msgBytes))

	// Verify metadata
	topic, exists := msg.MetaGet("kafka_server_topic")
	assert.True(t, exists)
	assert.Equal(t, testTopic, topic)

	// Acknowledge the message
	err = ackFn(ctx, nil)
	require.NoError(t, err)

	// Verify producer received acknowledgment
	select {
	case err := <-produceChan:
		assert.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for produce acknowledgment")
	}
}

func TestKafkaServerInputSCRAMFailedAuth(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create kafka_server input with SCRAM-SHA-256 authentication
	spec := kafkaServerInputConfig()
	env := service.NewEnvironment()

	config := `
address: "127.0.0.1:19105"
sasl:
  - mechanism: SCRAM-SHA-256
    username: "validuser"
    password: "validpass"
`

	parsed, err := spec.ParseYAML(config, env)
	require.NoError(t, err)

	input, err := newKafkaServerInputFromConfig(parsed, service.MockResources())
	require.NoError(t, err)

	err = input.Connect(ctx)
	require.NoError(t, err)
	defer input.Close(ctx)

	time.Sleep(100 * time.Millisecond)

	// Create client with WRONG credentials
	client, err := kgo.NewClient(
		kgo.SeedBrokers("127.0.0.1:19105"),
		kgo.SASL(scram.Sha256(func(context.Context) (scram.Auth, error) {
			return scram.Auth{
				User: "validuser",
				Pass: "wrongpass",
			}, nil
		})),
	)
	require.NoError(t, err)
	defer client.Close()

	// Try to produce a message - should fail
	testTopic := "test-topic"
	testValue := "test-value"

	record := &kgo.Record{
		Topic: testTopic,
		Value: []byte(testValue),
	}

	results := client.ProduceSync(ctx, record)
	require.Len(t, results, 1)

	// Should get an authentication error
	assert.Error(t, results[0].Err)
	t.Logf("Expected SCRAM authentication error received: %v", results[0].Err)
}
