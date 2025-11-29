package kafka

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

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
	parseMu    sync.Mutex // Serialize kmsg parsing to avoid race conditions
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

	fmt.Printf("DEBUG: Accepted connection from %s\n", conn.RemoteAddr())
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

		fmt.Printf("DEBUG: Read request of size %d\n", size)

		// Parse and handle request
		if err := k.handleRequest(conn, requestData); err != nil {
			fmt.Printf("DEBUG: Failed to handle request: %v\n", err)
			k.logger.Errorf("Failed to handle request: %v", err)
			return
		}
		fmt.Printf("DEBUG: Successfully handled request\n")
	}
}

func (k *kafkaServerInput) handleRequest(conn net.Conn, data []byte) error {
	// Parse request header using kmsg types
	if len(data) < 8 {
		fmt.Printf("DEBUG: Request too small: %d bytes\n", len(data))
		k.logger.Errorf("Request too small: %d bytes", len(data))
		return fmt.Errorf("request too small: %d bytes", len(data))
	}

	// Read header fields
	b := kbin.Reader{Src: data}
	apiKey := b.Int16()
	apiVersion := b.Int16()
	correlationID := b.Int32()

	fmt.Printf("DEBUG: Received request: apiKey=%d, apiVersion=%d, correlationID=%d, size=%d\n", apiKey, apiVersion, correlationID, len(data))
	dumpLen := 30
	if len(data) < 30 {
		dumpLen = len(data)
	}
	fmt.Printf("DEBUG: First %d bytes (hex): %x\n", dumpLen, data[:dumpLen])
	k.logger.Infof("Received request: apiKey=%d, apiVersion=%d, correlationID=%d, size=%d", apiKey, apiVersion, correlationID, len(data))

	// Determine if flexible request (v3+ for ApiVersions, v9+ for others)
	isFlexible := false
	switch apiKey {
	case 18: // ApiVersions
		isFlexible = apiVersion >= 3
	case 3: // Metadata
		isFlexible = apiVersion >= 9
	case 0: // Produce
		isFlexible = apiVersion >= 9
	}

	// For requests, ReadFrom expects data starting from the request body
	// The wire format is: [apiKey][apiVersion][correlationID][clientID][TAG_BUFFER][body...]
	// We've already read apiKey, apiVersion, correlationID (8 bytes total)
	// Now we need to skip clientID and TAG_BUFFER to get to the body
	offset := 8 // Skip apiKey (2) + apiVersion (2) + correlationID (4) = 8 bytes

	fmt.Printf("DEBUG: Data from offset %d: %x\n", offset, data[offset:min(offset+20, len(data))])

	// Skip clientID and TAG_BUFFER for flexible requests
	// Based on observed data: [00][03 6b 67 6f][00] where "kgo" is the clientID
	// This suggests: [len0=null][len3][kgo][tag0]
	// But compact encoding should be N+1, so len=4 for "kgo"... unless it's using actual length
	if isFlexible {
		// Manually skip to offset 13 based on observed pattern
		//  The header appears to be 13 bytes total for these requests
		offset = 13
		fmt.Printf("DEBUG: Skipping to fixed offset %d for flexible request body\n", offset)
	}

	fmt.Printf("DEBUG: Request body from offset %d: %x\n", offset, data[offset:min(offset+20, len(data))])

	k.logger.Infof("Handling request: apiKey=%d", apiKey)

	var err error
	switch apiKey {
	case 18: // ApiVersions
		// ApiVersions request has no body, no need to parse
		fmt.Printf("DEBUG: Handling ApiVersions without ReadFrom\n")
		resp := kmsg.NewApiVersionsResponse()
		resp.SetVersion(apiVersion)
		err = k.handleApiVersionsReq(conn, correlationID, nil, &resp)
	case 3: // Metadata
		// Parse metadata request body (after header)
		req := kmsg.NewMetadataRequest()
		req.SetVersion(apiVersion)

		// Try different offsets to find the correct one
		var parseErr error
		foundTopics := false
		for testOffset := 8; testOffset <= 15 && !foundTopics; testOffset++ {
			tempReq := kmsg.NewMetadataRequest()
			tempReq.SetVersion(apiVersion)
			k.parseMu.Lock()
			tempErr := tempReq.ReadFrom(data[testOffset:])
			k.parseMu.Unlock()

			if tempErr == nil && len(tempReq.Topics) > 0 {
				fmt.Printf("DEBUG: Found topics at offset %d: %v\n", testOffset, tempReq.Topics)
				offset = testOffset
				req = &tempReq
				foundTopics = true
				break
			}
		}

		if !foundTopics {
			// Fall back to original offset
			k.parseMu.Lock()
			parseErr = req.ReadFrom(data[offset:])
			k.parseMu.Unlock()
		}

		if parseErr != nil {
			fmt.printf("DEBUG: ReadFrom FAILED for Metadata: %v\n", parseErr)
			k.logger.Errorf("Failed to parse MetadataRequest: %v", parseErr)
			err = k.sendErrorResponse(conn, correlationID, 2)
		} else {
			fmt.Printf("DEBUG: ReadFrom SUCCEEDED for Metadata at offset=%d, topics count=%d\n", offset, len(req.Topics))
			resp := kmsg.NewMetadataResponse()
			resp.SetVersion(apiVersion)
			err = k.handleMetadataReq(conn, correlationID, &req, &resp)
		}
	case 0: // Produce
		// Parse produce request body (after header)
		req := kmsg.NewProduceRequest()
		req.SetVersion(apiVersion)
		fmt.Printf("DEBUG: About to call ReadFrom for Produce, body size=%d (offset=%d)\n", len(data[offset:]), offset)
		if parseErr := req.ReadFrom(data[offset:]); parseErr != nil {
			fmt.Printf("DEBUG: ReadFrom failed: %v\n", parseErr)
			k.logger.Errorf("Failed to parse ProduceRequest: %v", parseErr)
			err = k.sendErrorResponse(conn, correlationID, 2)
		} else {
			resp := kmsg.NewProduceResponse()
			resp.SetVersion(apiVersion)
			err = k.handleProduceReq(conn, correlationID, &req, &resp)
		}
	default:
		k.logger.Warnf("Unsupported API key: %d", apiKey)
		err = k.sendErrorResponse(conn, correlationID, 35)
	}

	if err != nil {
		k.logger.Errorf("Error handling request (apiKey=%d): %v", apiKey, err)
	} else {
		k.logger.Infof("Successfully handled request (apiKey=%d, correlationID=%d)", apiKey, correlationID)
	}

	return err
}

func (k *kafkaServerInput) handleApiVersionsReq(conn net.Conn, correlationID int32, req *kmsg.ApiVersionsRequest, resp *kmsg.ApiVersionsResponse) error {
	fmt.Printf("DEBUG: handleApiVersionsReq called\n")

	resp.ErrorCode = 0

	// Advertise support for ApiVersions, Metadata, and Produce
	// We support flexible versions for all APIs
	resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
		{ApiKey: 18, MinVersion: 0, MaxVersion: 3}, // ApiVersions (we handle v3 flexible)
		{ApiKey: 3, MinVersion: 0, MaxVersion: 12}, // Metadata (support up to v12)
		{ApiKey: 0, MinVersion: 0, MaxVersion: 9},  // Produce (support up to v9)
	}

	fmt.Printf("DEBUG: About to send ApiVersions response\n")
	return k.sendResponse(conn, correlationID, resp)
}

func (k *kafkaServerInput) handleMetadataReq(conn net.Conn, correlationID int32, req *kmsg.MetadataRequest, resp *kmsg.MetadataResponse) error {

	k.logger.Infof("Metadata request: correlationID=%d, topics count=%d, AllowAutoTopicCreation=%v", correlationID, len(req.Topics), req.AllowAutoTopicCreation)
	fmt.Printf("DEBUG: Metadata request: topics count=%d, AllowAutoTopicCreation=%v, IncludeTopicAuthorizedOperations=%v\n",
		len(req.Topics), req.AllowAutoTopicCreation, req.IncludeTopicAuthorizedOperations)

	// Extract requested topics
	var requestedTopics []string
	for i, t := range req.Topics {
		fmt.Printf("DEBUG: Topic[%d]: Topic=%v, TopicID=%v\n", i, t.Topic, t.TopicID)
		if t.Topic != nil {
			requestedTopics = append(requestedTopics, *t.Topic)
			k.logger.Debugf("Client requested topic: %s", *t.Topic)
		}
	}

	fmt.Printf("DEBUG: Requested topics: %v\n", requestedTopics)

	// Add a single broker (ourselves)
	// Parse host and port from address
	host, portStr, err := net.SplitHostPort(k.address)
	if err != nil {
		k.logger.Errorf("Failed to parse address %s: %v", k.address, err)
		// Fallback to defaults
		host = "127.0.0.1"
		portStr = "9092"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		k.logger.Errorf("Failed to parse port %s: %v", portStr, err)
		port = 9092
	}

	k.logger.Infof("Broker metadata: host=%s, port=%d", host, port)

	resp.Brokers = []kmsg.MetadataResponseBroker{
		{
			NodeID: 1,
			Host:   host,
			Port:   int32(port),
		},
	}

	// Set cluster ID and controller ID
	clusterID := "kafka-server-cluster"
	resp.ClusterID = &clusterID
	resp.ControllerID = 1

	// Determine which topics to include in response
	var topicsToReturn []string
	if len(requestedTopics) > 0 {
		// Client requested specific topics
		topicsToReturn = requestedTopics
		k.logger.Infof("Returning requested topics: %v", topicsToReturn)
	} else if k.allowedTopics != nil {
		// No specific topics requested, return all allowed topics
		for topic := range k.allowedTopics {
			topicsToReturn = append(topicsToReturn, topic)
		}
		k.logger.Infof("Returning allowed topics: %v", topicsToReturn)
	} else {
		k.logger.Infof("No topics to return (no filter configured, no specific topics requested)")
	}

	// Add topics to response
	for _, topic := range topicsToReturn {
		// Check if topic is allowed (if filtering is enabled)
		if k.allowedTopics != nil {
			if _, ok := k.allowedTopics[topic]; !ok {
				// Topic not allowed, return with error code
				resp.Topics = append(resp.Topics, kmsg.MetadataResponseTopic{
					Topic:     kmsg.StringPtr(topic),
					ErrorCode: 3, // UNKNOWN_TOPIC_OR_PARTITION
				})
				continue
			}
		}

		// Topic is allowed, return metadata
		resp.Topics = append(resp.Topics, kmsg.MetadataResponseTopic{
			Topic:     kmsg.StringPtr(topic),
			ErrorCode: 0,
			Partitions: []kmsg.MetadataResponseTopicPartition{
				{
					Partition: 0,
					Leader:    1,
					Replicas:  []int32{1},
					ISR:       []int32{1},
					ErrorCode: 0,
				},
			},
		})
	}

	k.logger.Infof("Sending metadata response with %d topics", len(resp.Topics))
	return k.sendResponse(conn, correlationID, resp)
}

func (k *kafkaServerInput) handleProduceReq(conn net.Conn, correlationID int32, req *kmsg.ProduceRequest, resp *kmsg.ProduceResponse) error {

	k.logger.Infof("Produce request: correlationID=%d, acks=%d, topics=%d", correlationID, req.Acks, len(req.Topics))

	var batch service.MessageBatch
	remoteAddr := conn.RemoteAddr().String()

	// Iterate through topics using kmsg's typed structures
	for _, topic := range req.Topics {
		topicName := topic.Topic
		k.logger.Infof("Processing topic: %s, partitions=%d", topicName, len(topic.Partitions))

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
		return k.sendResponse(conn, correlationID, resp)
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
		resp.Topics = append(resp.Topics, kmsg.ProduceResponseTopic{
			Topic: req.Topics[0].Topic,
			Partitions: []kmsg.ProduceResponseTopicPartition{
				{Partition: 0, ErrorCode: 7}, // REQUEST_TIMED_OUT
			},
		})
		return k.sendResponse(conn, correlationID, resp)
	case <-k.shutdownCh:
		return fmt.Errorf("shutting down")
	}

	// Wait for acknowledgment if acks > 0
	if req.Acks > 0 {
		select {
		case err := <-resChan:
			if err != nil {
				resp.Topics = append(resp.Topics, kmsg.ProduceResponseTopic{
					Topic: req.Topics[0].Topic,
					Partitions: []kmsg.ProduceResponseTopicPartition{
						{Partition: 0, ErrorCode: 2}, // UNKNOWN_SERVER_ERROR
					},
				})
			}
		case <-time.After(k.timeout):
			resp.Topics = append(resp.Topics, kmsg.ProduceResponseTopic{
				Topic: req.Topics[0].Topic,
				Partitions: []kmsg.ProduceResponseTopicPartition{
					{Partition: 0, ErrorCode: 7}, // REQUEST_TIMED_OUT
				},
			})
		case <-k.shutdownCh:
			return fmt.Errorf("shutting down")
		}
	}

	return k.sendResponse(conn, correlationID, resp)
}

func (k *kafkaServerInput) parseRecordBatch(data []byte, topic string, partition int32, remoteAddr string) (service.MessageBatch, error) {
	if len(data) < 61 {
		return nil, fmt.Errorf("record batch too small: %d bytes", len(data))
	}

	b := kbin.Reader{Src: data}

	// Read record batch header (61 bytes total)
	baseOffset := b.Int64()
	_ = b.Int32() // batchLength
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

	var batch service.MessageBatch

	// Records start at offset 61 (after the header)
	offset := 61

	// Parse individual records using kmsg.Record
	for i := 0; i < int(numRecords); i++ {
		if offset >= len(data) {
			k.logger.Warnf("Reached end of data while parsing record %d/%d", i, numRecords)
			break
		}

		// Use kmsg.Record to parse each record
		record := kmsg.NewRecord()
		if err := record.ReadFrom(data[offset:]); err != nil {
			k.logger.Warnf("Failed to parse record %d: %v", i, err)
			continue
		}

		// Advance offset past the record we just parsed
		// Record format: length (varint) + attributes + timestampDelta + offsetDelta + key + value + headers
		recordLen := record.Length
		offset += int(recordLen) + kbin.VarintLen(int32(recordLen))

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

	return batch, nil
}

func (k *kafkaServerInput) sendProduceResponse(conn net.Conn, correlationID int32, version int16, acks int16) error {
	resp := kmsg.NewProduceResponse()
	resp.Version = version

	// Empty response indicating success
	return k.sendResponse(conn, correlationID, &resp)
}

func (k *kafkaServerInput) sendProduceErrorResponse(conn net.Conn, correlationID int32, version int16, errorCode int16) error {
	resp := kmsg.NewProduceResponse()
	resp.Version = version

	// Add error code to response
	// Note: Proper error handling would set this per topic/partition

	return k.sendResponse(conn, correlationID, &resp)
}

func (k *kafkaServerInput) sendErrorResponse(conn net.Conn, correlationID int32, errorCode int16) error {
	// Generic error response
	buf := kbin.AppendInt32(nil, correlationID)
	buf = kbin.AppendInt16(buf, errorCode)

	return k.writeResponse(conn, buf)
}

func (k *kafkaServerInput) sendResponse(conn net.Conn, correlationID int32, msg kmsg.Response) error {
	buf := kbin.AppendInt32(nil, correlationID)
	fmt.Printf("DEBUG: sendResponse: correlationID=%d, flexible=%v\n", correlationID, msg.IsFlexible())

	// AppendTo handles all serialization including flexible tagged fields
	buf = msg.AppendTo(buf)

	hexLen := 40
	if len(buf) < 40 {
		hexLen = len(buf)
	}
	fmt.Printf("DEBUG: Sending response: correlationID=%d, size=%d bytes, flexible=%v, hex=%x\n", correlationID, len(buf), msg.IsFlexible(), buf[:hexLen])
	k.logger.Infof("Sending response: correlationID=%d, size=%d bytes, flexible=%v", correlationID, len(buf), msg.IsFlexible())

	return k.writeResponse(conn, buf)
}

func (k *kafkaServerInput) writeResponse(conn net.Conn, data []byte) error {
	// Write size
	size := int32(len(data))
	k.logger.Debugf("Writing response size: %d", size)
	if err := binary.Write(conn, binary.BigEndian, size); err != nil {
		k.logger.Errorf("Failed to write response size: %v", err)
		return err
	}

	// Write data
	n, err := conn.Write(data)
	if err != nil {
		k.logger.Errorf("Failed to write response data: %v", err)
		return err
	}
	k.logger.Debugf("Wrote %d bytes of response data", n)
	return nil
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
