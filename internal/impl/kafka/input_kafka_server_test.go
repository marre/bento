package kafka

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"

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
	go func() {
		results := client.ProduceSync(ctx, record)
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

	partition, exists := msg.MetaGet("kafka_server_partition")
	assert.True(t, exists)
	assert.Equal(t, int32(0), partition)

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
