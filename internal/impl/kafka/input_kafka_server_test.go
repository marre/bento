package kafka

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"

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
