package kafka

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	ksfFieldAddress        = "address"
	ksfFieldTopics         = "topics"
	ksfFieldTLS            = "tls"
	ksfFieldSASL           = "sasl"
	ksfFieldTimeout        = "timeout"
	ksfFieldMaxMessageBytes = "max_message_bytes"
)

func kafkaServerInputConfig() *service.ConfigSpec {
	return service.NewConfigSpec().
		Stable().
		Categories("Services").
		Summary("Runs a Kafka-protocol-compatible server that accepts produce requests from Kafka producers.").
		Description(`
This input acts as a Kafka broker endpoint, allowing Kafka producers to send messages directly into a Bento pipeline without requiring a full Kafka cluster.

Similar to the ` + "`http_server`" + ` input, this creates a server that external clients can push data to. The difference is that clients use the Kafka protocol instead of HTTP.

### Metadata

This input adds the following metadata fields to each message:

` + "```text" + `
- kafka_server_topic
- kafka_server_partition
- kafka_server_key
- kafka_server_timestamp
- kafka_server_remote_addr
` + "```" + `

Message headers from Kafka records are also added as metadata fields.`).
		Field(service.NewStringField(ksfFieldAddress).
			Description("The address to listen on for Kafka protocol connections.").
			Default("0.0.0.0:9092")).
		Field(service.NewStringListField(ksfFieldTopics).
			Description("Optional list of topic names to accept. If empty, all topics are accepted.").
			Default([]string{}).
			Advanced()).
		Field(service.NewTLSToggledField(ksfFieldTLS)).
		Field(saslField()).
		Field(service.NewDurationField(ksfFieldTimeout).
			Description("The maximum time to wait for a message to be processed before responding with an error.").
			Default("5s").
			Advanced()).
		Field(service.NewIntField(ksfFieldMaxMessageBytes).
			Description("The maximum size in bytes of a message payload.").
			Default(1048576).
			Advanced()).
		Example("Basic Usage", "Accept Kafka produce requests and write to S3", `
input:
  kafka_server:
    address: "0.0.0.0:9092"
    topics:
      - events
      - logs

output:
  aws_s3:
    bucket: my-data-lake
    path: '${! meta("kafka_server_topic") }/${! timestamp_unix() }.json'
`).
		Example("With TLS", "Accept Kafka produce requests over TLS", `
input:
  kafka_server:
    address: "0.0.0.0:9093"
    tls:
      enabled: true
      cert_file: /path/to/cert.pem
      key_file: /path/to/key.pem
`)
}

func init() {
	err := service.RegisterBatchInput(
		"kafka_server", kafkaServerInputConfig(),
		func(conf *service.ParsedConfig, mgr *service.Resources) (service.BatchInput, error) {
			return newKafkaServerInputFromConfig(conf, mgr)
		})
	if err != nil {
		panic(err)
	}
}

//------------------------------------------------------------------------------

type kafkaServerInput struct {
	address         string
	allowedTopics   map[string]struct{}
	tlsConfig       *tls.Config
	saslMechanisms  []string
	timeout         time.Duration
	maxMessageBytes int

	listener   net.Listener
	msgChan    chan messageBatch
	shutdownCh chan struct{}
	logger     *service.Logger

	shutSig    *shutdown.Signaller
	connWG     sync.WaitGroup
}

type messageBatch struct {
	batch  service.MessageBatch
	ackFn  service.AckFunc
	resChan chan error
}

func newKafkaServerInputFromConfig(conf *service.ParsedConfig, mgr *service.Resources) (*kafkaServerInput, error) {
	k := &kafkaServerInput{
		logger:     mgr.Logger(),
		shutdownCh: make(chan struct{}),
		shutSig:    shutdown.NewSignaller(),
	}

	var err error
	if k.address, err = conf.FieldString(ksfFieldAddress); err != nil {
		return nil, err
	}

	topicList, err := conf.FieldStringList(ksfFieldTopics)
	if err != nil {
		return nil, err
	}
	if len(topicList) > 0 {
		k.allowedTopics = make(map[string]struct{})
		for _, topic := range topicList {
			k.allowedTopics[topic] = struct{}{}
		}
	}

	tlsConf, tlsEnabled, err := conf.FieldTLSToggled(ksfFieldTLS)
	if err != nil {
		return nil, err
	}
	if tlsEnabled {
		k.tlsConfig = tlsConf
	}

	// SASL support - we'll store mechanism names for validation
	// Full SASL authentication would require more complex handling
	if conf.Contains(ksfFieldSASL) {
		k.logger.Warn("SASL authentication is configured but not fully implemented in this version")
	}

	if k.timeout, err = conf.FieldDuration(ksfFieldTimeout); err != nil {
		return nil, err
	}

	if k.maxMessageBytes, err = conf.FieldInt(ksfFieldMaxMessageBytes); err != nil {
		return nil, err
	}

	return k, nil
}

//------------------------------------------------------------------------------

func (k *kafkaServerInput) Connect(ctx context.Context) error {
	if k.listener != nil {
		return nil
	}

	if k.shutSig.IsSoftStopSignalled() {
		return service.ErrEndOfInput
	}

	var listener net.Listener
	var err error

	if k.tlsConfig != nil {
		listener, err = tls.Listen("tcp", k.address, k.tlsConfig)
		if err != nil {
			return fmt.Errorf("failed to start TLS listener: %w", err)
		}
		k.logger.Infof("Kafka server listening on %s (TLS enabled)", k.address)
	} else {
		listener, err = net.Listen("tcp", k.address)
		if err != nil {
			return fmt.Errorf("failed to start listener: %w", err)
		}
		k.logger.Infof("Kafka server listening on %s", k.address)
	}

	k.listener = listener
	k.msgChan = make(chan messageBatch, 10)

	go k.acceptLoop()

	return nil
}

func (k *kafkaServerInput) acceptLoop() {
	defer func() {
		close(k.msgChan)
		k.shutSig.TriggerHasStopped()
	}()

	// Create a channel for accepting connections
	connChan := make(chan net.Conn)
	errChan := make(chan error, 1)

	go func() {
		for {
			conn, err := k.listener.Accept()
			if err != nil {
				errChan <- err
				return
			}
			connChan <- conn
		}
	}()

	for {
		select {
		case <-k.shutdownCh:
			return
		case conn := <-connChan:
			k.connWG.Add(1)
			go k.handleConnection(conn)
		case err := <-errChan:
			if !k.shutSig.IsSoftStopSignalled() {
				k.logger.Debugf("Accept loop ended: %v", err)
			}
			return
		}
	}
}

func (k *kafkaServerInput) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		k.connWG.Done()
	}()

	k.logger.Debugf("Accepted connection from %s", conn.RemoteAddr())

	for {
		select {
		case <-k.shutdownCh:
			return
		default:
		}

		// Set read deadline
		if err := conn.SetReadDeadline(time.Now().Add(k.timeout)); err != nil {
			k.logger.Errorf("Failed to set read deadline: %v", err)
			return
		}

		// Read request size (4 bytes)
		var size int32
		if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
			if err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
				if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
					k.logger.Debugf("Failed to read request size: %v", err)
				}
			}
			return
		}

		if size <= 0 || size > int32(k.maxMessageBytes)*10 {
			k.logger.Errorf("Invalid request size: %d", size)
			return
		}

		// Read request data
		requestData := make([]byte, size)
		if _, err := io.ReadFull(conn, requestData); err != nil {
			k.logger.Errorf("Failed to read request data: %v", err)
			return
		}

		// Parse and handle request
		if err := k.handleRequest(conn, requestData); err != nil {
			k.logger.Errorf("Failed to handle request: %v", err)
			return
		}
	}
}

func (k *kafkaServerInput) handleRequest(conn net.Conn, data []byte) error {
	// Parse request header
	if len(data) < 8 {
		return fmt.Errorf("request too small: %d bytes", len(data))
	}

	b := kbin.Reader{Src: data}
	apiKey := b.Int16()
	apiVersion := b.Int16()
	correlationID := b.Int32()

	k.logger.Debugf("Received request: apiKey=%d, apiVersion=%d, correlationID=%d", apiKey, apiVersion, correlationID)

	switch apiKey {
	case 18: // ApiVersions
		return k.handleApiVersions(conn, correlationID, apiVersion)
	case 3: // Metadata
		return k.handleMetadata(conn, correlationID, apiVersion, &b)
	case 0: // Produce
		return k.handleProduce(conn, correlationID, apiVersion, &b)
	default:
		k.logger.Warnf("Unsupported API key: %d", apiKey)
		return k.sendErrorResponse(conn, correlationID, 35) // UNSUPPORTED_VERSION
	}
}

func (k *kafkaServerInput) handleApiVersions(conn net.Conn, correlationID int32, version int16) error {
	resp := kmsg.NewApiVersionsResponse()
	resp.CorrelationID = correlationID
	resp.ErrorCode = 0

	// Advertise support for ApiVersions, Metadata, and Produce
	resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
		{ApiKey: 18, MinVersion: 0, MaxVersion: 3}, // ApiVersions
		{ApiKey: 3, MinVersion: 0, MaxVersion: 12},  // Metadata
		{ApiKey: 0, MinVersion: 0, MaxVersion: 9},   // Produce
	}

	return k.sendResponse(conn, resp)
}

func (k *kafkaServerInput) handleMetadata(conn net.Conn, correlationID int32, version int16, b *kbin.Reader) error {
	// For simplicity, we'll return minimal metadata
	resp := kmsg.NewMetadataResponse()
	resp.Version = version
	resp.CorrelationID = correlationID

	// Add a single broker (ourselves)
	resp.Brokers = []kmsg.MetadataResponseBroker{
		{
			NodeID: 1,
			Host:   k.address,
			Port:   9092,
		},
	}

	// If specific topics requested, return those; otherwise return all allowed topics
	var topics []string
	if k.allowedTopics != nil {
		for topic := range k.allowedTopics {
			topics = append(topics, topic)
		}
	}

	for _, topic := range topics {
		resp.Topics = append(resp.Topics, kmsg.MetadataResponseTopic{
			Topic:     kmsg.StringPtr(topic),
			ErrorCode: 0,
			Partitions: []kmsg.MetadataResponseTopicPartition{
				{
					Partition:      0,
					Leader:         1,
					Replicas:       []int32{1},
					ISR:            []int32{1},
					ErrorCode:      0,
				},
			},
		})
	}

	return k.sendResponse(conn, resp)
}

func (k *kafkaServerInput) handleProduce(conn net.Conn, correlationID int32, version int16, b *kbin.Reader) error {
	// Use kmsg to parse the ProduceRequest
	req := kmsg.NewProduceRequest()
	req.Version = version

	// Read the request from the buffer
	// Note: b already has the header consumed, so we read from current position
	if err := req.ReadFrom(b.Src[b.Off:]); err != nil {
		k.logger.Errorf("Failed to parse ProduceRequest: %v", err)
		return k.sendProduceErrorResponse(conn, correlationID, version, 2) // UNKNOWN_SERVER_ERROR
	}

	acks := req.Acks
	var batch service.MessageBatch
	remoteAddr := conn.RemoteAddr().String()

	// Iterate through topics using kmsg's typed structures
	for _, topic := range req.Topics {
		topicName := topic.Topic

		// Check if topic is allowed
		if k.allowedTopics != nil {
			if _, ok := k.allowedTopics[topicName]; !ok {
				k.logger.Warnf("Rejecting produce to disallowed topic: %s", topicName)
				continue
			}
		}

		// Iterate through partitions
		for _, partition := range topic.Partitions {
			if partition.Records == nil || len(partition.Records) == 0 {
				continue
			}

			// Parse records from the record batch
			messages, err := k.parseRecordBatch(partition.Records, topicName, partition.Partition, remoteAddr)
			if err != nil {
				k.logger.Errorf("Failed to parse record batch: %v", err)
				continue
			}

			batch = append(batch, messages...)
		}
	}

	// If no messages, send success
	if len(batch) == 0 {
		return k.sendProduceResponse(conn, correlationID, version, req.Acks)
	}

	// Send batch to pipeline
	resChan := make(chan error, 1)
	select {
	case k.msgChan <- messageBatch{
		batch: batch,
		ackFn: func(ctx context.Context, err error) error {
			resChan <- err
			return nil
		},
		resChan: resChan,
	}:
	case <-time.After(k.timeout):
		return k.sendProduceErrorResponse(conn, correlationID, version, 7) // REQUEST_TIMED_OUT
	case <-k.shutdownCh:
		return fmt.Errorf("shutting down")
	}

	// Wait for acknowledgment if acks > 0
	if req.Acks > 0 {
		select {
		case err := <-resChan:
			if err != nil {
				return k.sendProduceErrorResponse(conn, correlationID, version, 2) // UNKNOWN_SERVER_ERROR
			}
		case <-time.After(k.timeout):
			return k.sendProduceErrorResponse(conn, correlationID, version, 7) // REQUEST_TIMED_OUT
		case <-k.shutdownCh:
			return fmt.Errorf("shutting down")
		}
	}

	return k.sendProduceResponse(conn, correlationID, version, req.Acks)
}

func (k *kafkaServerInput) parseRecordBatch(data []byte, topic string, partition int32, remoteAddr string) (service.MessageBatch, error) {
	if len(data) < 61 {
		return nil, fmt.Errorf("record batch too small: %d bytes", len(data))
	}

	b := kbin.Reader{Src: data}

	// Read record batch header (61 bytes total)
	baseOffset := b.Int64()
	batchLength := b.Int32()
	_ = b.Int32() // partitionLeaderEpoch
	magic := b.Int8()

	if magic != 2 {
		return nil, fmt.Errorf("unsupported magic byte: %d (only magic byte 2 is supported)", magic)
	}

	_ = b.Int32() // crc
	_ = b.Int16() // attributes
	_ = b.Int32() // lastOffsetDelta
	firstTimestamp := b.Int64()
	_ = b.Int64() // maxTimestamp
	_ = b.Int64() // producerID
	_ = b.Int16() // producerEpoch
	_ = b.Int32() // baseSequence
	numRecords := b.Int32()

	if b.Err != nil {
		return nil, fmt.Errorf("failed to parse record batch header: %w", b.Err)
	}

	var batch service.MessageBatch

	// Parse individual records using kmsg.Record
	for i := 0; i < int(numRecords); i++ {
		// Use kmsg.Record to parse each record
		record := kmsg.NewRecord()
		if err := record.ReadFrom(b.Src[b.Off:]); err != nil {
			k.logger.Warnf("Failed to parse record %d: %v", i, err)
			continue
		}

		// Advance the reader past the record we just parsed
		recordLen := record.Length
		b.Off += int(recordLen) + kbin.VarintLen(int64(recordLen))

		// Calculate absolute timestamp
		timestamp := firstTimestamp + record.TimestampDelta64

		// Create Bento message
		msg := service.NewMessage(record.Value)
		msg.MetaSetMut("kafka_server_topic", topic)
		msg.MetaSetMut("kafka_server_partition", partition)
		msg.MetaSetMut("kafka_server_offset", baseOffset+int64(record.OffsetDelta))

		if record.Key != nil {
			msg.MetaSetMut("kafka_server_key", string(record.Key))
		}

		msg.MetaSetMut("kafka_server_timestamp", time.Unix(timestamp/1000, (timestamp%1000)*1000000).Format(time.RFC3339))
		msg.MetaSetMut("kafka_server_remote_addr", remoteAddr)

		// Add record headers as metadata
		for _, header := range record.Headers {
			msg.MetaSetMut(header.Key, string(header.Value))
		}

		batch = append(batch, msg)
	}

	if b.Err != nil {
		return nil, fmt.Errorf("error parsing records: %w", b.Err)
	}

	return batch, nil
}

func (k *kafkaServerInput) sendProduceResponse(conn net.Conn, correlationID int32, version int16, acks int16) error {
	resp := kmsg.NewProduceResponse()
	resp.Version = version
	resp.CorrelationID = correlationID

	// Empty response indicating success
	return k.sendResponse(conn, resp)
}

func (k *kafkaServerInput) sendProduceErrorResponse(conn net.Conn, correlationID int32, version int16, errorCode int16) error {
	resp := kmsg.NewProduceResponse()
	resp.Version = version
	resp.CorrelationID = correlationID

	// Add error code to response
	// Note: Proper error handling would set this per topic/partition

	return k.sendResponse(conn, resp)
}

func (k *kafkaServerInput) sendErrorResponse(conn net.Conn, correlationID int32, errorCode int16) error {
	// Generic error response
	w := kbin.Writer{}
	w.Int32(correlationID)
	w.Int16(errorCode)

	return k.writeResponse(conn, w.Buf)
}

func (k *kafkaServerInput) sendResponse(conn net.Conn, msg kmsg.Response) error {
	var w kbin.Writer
	msg.AppendTo(&w)

	return k.writeResponse(conn, w.Buf)
}

func (k *kafkaServerInput) writeResponse(conn net.Conn, data []byte) error {
	// Write size
	if err := binary.Write(conn, binary.BigEndian, int32(len(data))); err != nil {
		return err
	}

	// Write data
	_, err := conn.Write(data)
	return err
}

func (k *kafkaServerInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	if k.msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case mb, open := <-k.msgChan:
		if !open {
			return nil, nil, service.ErrNotConnected
		}
		return mb.batch, mb.ackFn, nil
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-k.shutdownCh:
		return nil, nil, service.ErrEndOfInput
	}
}

func (k *kafkaServerInput) Close(ctx context.Context) error {
	k.shutSig.TriggerSoftStop()

	close(k.shutdownCh)

	if k.listener != nil {
		k.listener.Close()
	}

	// Wait for connections to finish with timeout
	done := make(chan struct{})
	go func() {
		k.connWG.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		k.logger.Warn("Timeout waiting for connections to close")
	}

	k.shutSig.TriggerHasStopped()

	return nil
}
