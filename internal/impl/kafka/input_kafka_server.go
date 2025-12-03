package kafka

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/warpstreamlabs/bento/public/service"
)

const (
	ksfFieldAddress         = "address"
	ksfFieldTopics          = "topics"
	ksfFieldTLS             = "tls"
	ksfFieldSASL            = "sasl"
	ksfFieldTimeout         = "timeout"
	ksfFieldMaxMessageBytes = "max_message_bytes"
)

// Kafka compression codec values as defined in the Kafka protocol specification.
// These match franz-go's internal codecType constants.
const (
	codecNone   int8 = 0
	codecGzip   int8 = 1
	codecSnappy int8 = 2
	codecLZ4    int8 = 3
	codecZstd   int8 = 4

	// compressionCodecMask extracts the compression codec from the lower 3 bits of the Attributes field
	compressionCodecMask = 0x07
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

Similar to the `+"`http_server`"+` input, this creates a server that external clients can push data to. The difference is that clients use the Kafka protocol instead of HTTP.

### Metadata

This input adds the following metadata fields to each message:

`+"```text"+`
- kafka_server_topic
- kafka_server_partition
- kafka_server_key
- kafka_server_timestamp
- kafka_server_remote_addr
`+"```"+`

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
`).
		Example("With SASL Authentication", "Accept authenticated Kafka produce requests", `
input:
  kafka_server:
    address: "0.0.0.0:9092"
    tls:
      enabled: true
      cert_file: /path/to/cert.pem
      key_file: /path/to/key.pem
    sasl:
      - mechanism: PLAIN
        username: producer1
        password: secret123
      - mechanism: PLAIN
        username: producer2
        password: secret456
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
	saslEnabled     bool
	saslUsers       map[string]string // username -> password
	timeout         time.Duration
	maxMessageBytes int

	listener   net.Listener
	msgChan    chan messageBatch
	shutdownCh chan struct{}
	logger     *service.Logger

	shutSig *shutdown.Signaller
	connWG  sync.WaitGroup
	parseMu sync.Mutex // Protects kmsg ReadFrom calls to prevent concurrent map access in franz-go internals
}

type messageBatch struct {
	batch   service.MessageBatch
	ackFn   service.AckFunc
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

	// Parse SASL configuration
	if conf.Contains(ksfFieldSASL) {
		saslList, err := conf.FieldObjectList(ksfFieldSASL)
		if err != nil {
			return nil, fmt.Errorf("failed to parse SASL config: %w", err)
		}

		k.saslUsers = make(map[string]string)
		for i, saslConf := range saslList {
			mechanism, err := saslConf.FieldString("mechanism")
			if err != nil {
				return nil, fmt.Errorf("SASL config %d: %w", i, err)
			}

			// Only support PLAIN for now
			if mechanism != "PLAIN" {
				return nil, fmt.Errorf("SASL config %d: unsupported mechanism %q (only PLAIN is supported)", i, mechanism)
			}

			username, err := saslConf.FieldString("username")
			if err != nil {
				return nil, fmt.Errorf("SASL config %d: %w", i, err)
			}

			password, err := saslConf.FieldString("password")
			if err != nil {
				return nil, fmt.Errorf("SASL config %d: %w", i, err)
			}

			if username == "" {
				return nil, fmt.Errorf("SASL config %d: username cannot be empty", i)
			}

			k.saslUsers[username] = password
			k.logger.Infof("Registered SASL PLAIN user: %s", username)
		}

		k.saslEnabled = len(k.saslUsers) > 0
		if k.saslEnabled {
			k.logger.Infof("SASL authentication enabled with %d user(s)", len(k.saslUsers))
		}
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

	// Track authentication state for this connection
	// If SASL is disabled, consider the connection authenticated
	authenticated := !k.saslEnabled

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

		// Request size should not exceed maxMessageBytes * 2 (to account for protocol overhead)
		// Plus a reasonable upper bound for headers and metadata (100KB)
		maxRequestSize := int32(k.maxMessageBytes)*2 + 102400
		if size <= 0 || size > maxRequestSize {
			k.logger.Errorf("Invalid request size: %d (max: %d)", size, maxRequestSize)
			return
		}

		// Read request data
		requestData := make([]byte, size)
		if _, err := io.ReadFull(conn, requestData); err != nil {
			k.logger.Errorf("Failed to read request data: %v", err)
			return
		}

		// Parse and handle request (may update authenticated state)
		authUpdated, err := k.handleRequest(conn, requestData, &authenticated)
		if err != nil {
			k.logger.Errorf("Failed to handle request: %v", err)
			return
		}

		// If authentication was just completed, log it
		if authUpdated && authenticated {
			k.logger.Infof("Client %s authenticated successfully", conn.RemoteAddr())
		}
	}
}

func (k *kafkaServerInput) handleRequest(conn net.Conn, data []byte, authenticated *bool) (bool, error) {
	// Parse request header using kmsg types
	if len(data) < 8 {
		k.logger.Errorf("Request too small: %d bytes", len(data))
		return false, fmt.Errorf("request too small: %d bytes", len(data))
	}

	// Read header fields
	b := kbin.Reader{Src: data}
	apiKey := b.Int16()
	apiVersion := b.Int16()
	correlationID := b.Int32()

	fmt.Fprintf(os.Stderr, "[SERVER] handleRequest: apiKey=%d, apiVersion=%d, correlationID=%d, dataLen=%d\n", apiKey, apiVersion, correlationID, len(data))
	fmt.Fprintf(os.Stderr, "[SERVER] handleRequest: full data (first 40 bytes): %x\n", data[:min(40, len(data))])
	fmt.Fprintf(os.Stderr, "[SERVER] handleRequest: after header, b.Src len=%d, first 30 bytes: %x\n", len(b.Src), b.Src[:min(30, len(b.Src))])

	k.logger.Debugf("Received request: apiKey=%d, apiVersion=%d, correlationID=%d, size=%d", apiKey, apiVersion, correlationID, len(data))

	// Check if client needs authentication first
	if !*authenticated {
		// Only allow ApiVersions, SASLHandshake, and SASLAuthenticate before authentication
		switch kmsg.Key(apiKey) {
		case kmsg.ApiVersions, kmsg.SASLHandshake, kmsg.SASLAuthenticate:
			// These are allowed before authentication
		default:
			k.logger.Warnf("Rejecting unauthenticated request for API key %d from %s", apiKey, conn.RemoteAddr())
			return false, fmt.Errorf("authentication required")
		}
	}

	// Determine if flexible request (v3+ for ApiVersions, v9+ for others)
	isFlexible := false
	switch kmsg.Key(apiKey) {
	case kmsg.ApiVersions:
		isFlexible = apiVersion >= 3
	case kmsg.Metadata:
		isFlexible = apiVersion >= 9
	case kmsg.Produce:
		isFlexible = apiVersion >= 9
	case kmsg.SASLHandshake:
		isFlexible = false // SASL Handshake is never flexible
	case kmsg.SASLAuthenticate:
		// SASLAuthenticate v2+ has flexible request body but uses non-flexible header (v1)
		isFlexible = false
	}

	// Parse clientID from request header
	// The encoding depends on whether the request is flexible or not
	// For flexible requests (v2+), clientID is COMPACT_STRING (uvarint length)
	// For non-flexible requests, clientID is NULLABLE_STRING (int16 length)
	const maxClientIDLength = 10000 // Reasonable limit to prevent DoS

	if isFlexible {
		// Flexible: COMPACT_STRING (uvarint length)
		clientIDLen := b.Uvarint()
		fmt.Fprintf(os.Stderr, "[SERVER] Flexible clientID: raw uvarint=%d\n", clientIDLen)
		if clientIDLen > 0 {
			// Length is (actual_length + 1), so subtract 1
			actualLen := clientIDLen - 1
			if actualLen > maxClientIDLength {
				return false, fmt.Errorf("invalid client ID length: %d (max: %d)", actualLen, maxClientIDLength)
			}
			fmt.Fprintf(os.Stderr, "[SERVER] Consuming %d bytes for clientID\n", actualLen)
			b.Span(int(actualLen))
		}
	} else {
		// Non-flexible: NULLABLE_STRING (int16 length)
		clientIDLen := b.Int16()
		if clientIDLen < 0 || clientIDLen > maxClientIDLength {
			return false, fmt.Errorf("invalid client ID length: %d (max: %d)", clientIDLen, maxClientIDLength)
		}
		if clientIDLen > 0 {
			b.Span(int(clientIDLen))
		}
	}

	// For flexible requests, parse TAG_BUFFER (even for SASLAuthenticate which skips clientID)
	if isFlexible {
		// Flexible header v2 has TAG_BUFFER (compact array of tagged fields)
		tagCount := b.Uvarint()
		for i := 0; i < int(tagCount); i++ {
			tagID := b.Uvarint()
			tagSize := b.Uvarint()
			b.Span(int(tagSize))
			_ = tagID // unused
		}
	}

	// Now b is positioned at the start of the request body
	offset := len(data) - len(b.Src)

	k.logger.Debugf("Request body from offset %d (isFlexible=%v): %x", offset, isFlexible, data[offset:min(offset+20, len(data))])

	var err error
	authUpdated := false

	switch kmsg.Key(apiKey) {
	case kmsg.ApiVersions:
		// ApiVersions request has minimal/no body
		k.logger.Debugf("Handling ApiVersions")
		resp := kmsg.NewApiVersionsResponse()
		resp.SetVersion(apiVersion)
		err = k.handleApiVersionsReq(conn, correlationID, nil, &resp)
	case kmsg.SASLHandshake:
		// Handle SASL handshake
		k.logger.Debugf("Handling SASLHandshake")
		err = k.handleSaslHandshake(conn, correlationID, data[offset:], apiVersion)
	case kmsg.SASLAuthenticate:
		// Handle SASL authentication
		k.logger.Debugf("Handling SASLAuthenticate")
		authSuccess := false
		authSuccess, err = k.handleSaslAuthenticate(conn, correlationID, data[offset:], apiVersion)
		if err == nil && authSuccess {
			*authenticated = true
			authUpdated = true
		}
	case kmsg.Metadata:
		// Parse metadata request body (after header)
		req := kmsg.NewMetadataRequest()
		req.SetVersion(apiVersion)

		k.parseMu.Lock()
		parseErr := req.ReadFrom(data[offset:])
		k.parseMu.Unlock()

		if parseErr != nil {
			k.logger.Errorf("Failed to parse MetadataRequest: %v", parseErr)
			err = k.sendErrorResponse(conn, correlationID, kerr.UnknownServerError.Code)
		} else {
			k.logger.Debugf("ReadFrom SUCCEEDED for Metadata at offset=%d, topics count=%d", offset, len(req.Topics))
			resp := kmsg.NewMetadataResponse()
			resp.SetVersion(apiVersion)
			err = k.handleMetadataReq(conn, correlationID, &req, &resp)
		}
	case kmsg.Produce:
		// Parse produce request body (after header)
		req := kmsg.NewProduceRequest()
		req.SetVersion(apiVersion)
		k.logger.Debugf("About to call ReadFrom for Produce, body size=%d (offset=%d)", len(data[offset:]), offset)

		k.parseMu.Lock()
		parseErr := req.ReadFrom(data[offset:])
		k.parseMu.Unlock()

		if parseErr != nil {
			k.logger.Errorf("Failed to parse ProduceRequest: %v", parseErr)
			err = k.sendErrorResponse(conn, correlationID, kerr.UnknownServerError.Code)
		} else {
			resp := kmsg.NewProduceResponse()
			resp.SetVersion(apiVersion)
			err = k.handleProduceReq(conn, correlationID, &req, &resp)
		}
	default:
		k.logger.Warnf("Unsupported API key: %d", apiKey)
		err = k.sendErrorResponse(conn, correlationID, kerr.UnsupportedVersion.Code)
	}

	return authUpdated, err
}

func (k *kafkaServerInput) handleSaslHandshake(conn net.Conn, correlationID int32, data []byte, apiVersion int16) error {
	fmt.Fprintf(os.Stderr, "[SERVER] handleSaslHandshake called: correlationID=%d, apiVersion=%d, dataLen=%d\n", correlationID, apiVersion, len(data))

	req := kmsg.NewSASLHandshakeRequest()
	req.SetVersion(apiVersion)

	k.parseMu.Lock()
	err := req.ReadFrom(data)
	k.parseMu.Unlock()

	if err != nil {
		fmt.Fprintf(os.Stderr, "[SERVER] Failed to parse SASLHandshakeRequest: %v\n", err)
		return k.sendErrorResponse(conn, correlationID, kerr.IllegalSaslState.Code)
	}

	fmt.Fprintf(os.Stderr, "[SERVER] SASL handshake request for mechanism: '%s' (len=%d, bytes=%x)\n", req.Mechanism, len(req.Mechanism), []byte(req.Mechanism))

	resp := kmsg.NewSASLHandshakeResponse()
	resp.SetVersion(apiVersion)

	// Check if requested mechanism is PLAIN (the only one we support)
	fmt.Fprintf(os.Stderr, "[SERVER] Comparing mechanism: req.Mechanism='%s' vs 'PLAIN', equal=%v\n", req.Mechanism, req.Mechanism == "PLAIN")
	if req.Mechanism == "PLAIN" {
		resp.ErrorCode = 0
		resp.SupportedMechanisms = []string{"PLAIN"}
		fmt.Fprintf(os.Stderr, "[SERVER] SASL handshake accepted for PLAIN, ErrorCode=%d\n", resp.ErrorCode)
	} else {
		resp.ErrorCode = kerr.UnsupportedSaslMechanism.Code
		resp.SupportedMechanisms = []string{"PLAIN"}
		fmt.Fprintf(os.Stderr, "[SERVER] Unsupported SASL mechanism requested: '%s', ErrorCode=%d\n", req.Mechanism, resp.ErrorCode)
	}

	err = k.sendResponse(conn, correlationID, &resp)
	fmt.Fprintf(os.Stderr, "[SERVER] SASL handshake response sent, err=%v\n", err)
	return err
}

func (k *kafkaServerInput) handleSaslAuthenticate(conn net.Conn, correlationID int32, data []byte, apiVersion int16) (bool, error) {
	fmt.Fprintf(os.Stderr, "[SERVER] handleSaslAuthenticate called: correlationID=%d, apiVersion=%d, dataLen=%d\n", correlationID, apiVersion, len(data))
	fmt.Fprintf(os.Stderr, "[SERVER] handleSaslAuthenticate data (first 30 bytes): %x\n", data[:min(30, len(data))])

	// Manually parse SASL auth bytes
	// Format for v2: INT16 length + auth_bytes + TAG_BUFFER (for flexible)
	if len(data) < 2 {
		k.sendErrorResponse(conn, correlationID, kerr.SaslAuthenticationFailed.Code)
		return false, fmt.Errorf("SASL authenticate data too short")
	}

	b := kbin.Reader{Src: data}
	authLen := b.Int16()
	if authLen < 0 || int(authLen) > len(b.Src) {
		k.sendErrorResponse(conn, correlationID, kerr.SaslAuthenticationFailed.Code)
		return false, fmt.Errorf("invalid SASL auth bytes length: %d", authLen)
	}

	authBytes := b.Span(int(authLen))
	fmt.Fprintf(os.Stderr, "[SERVER] SASL authenticate request received, auth bytes length: %d\n", len(authBytes))

	resp := kmsg.NewSASLAuthenticateResponse()
	resp.SetVersion(apiVersion)

	// Validate PLAIN credentials
	authenticated := k.validatePlain(authBytes)

	if authenticated {
		resp.ErrorCode = 0
		fmt.Fprintf(os.Stderr, "[SERVER] SASL authentication succeeded for %s, sending success response\n", conn.RemoteAddr())
	} else {
		resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
		errMsg := "Authentication failed"
		resp.ErrorMessage = &errMsg
		fmt.Fprintf(os.Stderr, "[SERVER] SASL authentication failed for %s, sending failure response\n", conn.RemoteAddr())
	}

	err := k.sendResponse(conn, correlationID, &resp)
	fmt.Fprintf(os.Stderr, "[SERVER] SASL authenticate response sent, authenticated=%v, err=%v\n", authenticated, err)
	return authenticated, err
}

// validatePlain validates PLAIN SASL credentials
// PLAIN format: [authzid] \0 username \0 password
func (k *kafkaServerInput) validatePlain(authBytes []byte) bool {
	fmt.Fprintf(os.Stderr, "[SERVER] validatePlain: authBytes (hex): %x\n", authBytes)

	// Split by null bytes
	parts := bytes.Split(authBytes, []byte{0})
	fmt.Fprintf(os.Stderr, "[SERVER] validatePlain: split into %d parts\n", len(parts))
	for i, part := range parts {
		fmt.Fprintf(os.Stderr, "[SERVER] validatePlain: parts[%d] = %q (len=%d)\n", i, string(part), len(part))
	}

	if len(parts) < 3 {
		k.logger.Debugf("Invalid PLAIN auth format: expected at least 3 parts, got %d", len(parts))
		return false
	}

	// parts[0] is authzid (authorization identity), usually empty
	// parts[1] is username (authentication identity)
	// parts[2] is password
	username := string(parts[1])
	password := string(parts[2])

	k.logger.Debugf("Validating credentials for username: %s", username)

	expectedPassword, exists := k.saslUsers[username]
	if !exists {
		k.logger.Debugf("User not found: %s", username)
		return false
	}

	if expectedPassword != password {
		k.logger.Debugf("Password mismatch for user: %s", username)
		return false
	}

	k.logger.Debugf("Credentials validated successfully for user: %s", username)
	return true
}

func (k *kafkaServerInput) handleApiVersionsReq(conn net.Conn, correlationID int32, req *kmsg.ApiVersionsRequest, resp *kmsg.ApiVersionsResponse) error {
	k.logger.Debugf("handleApiVersionsReq called")

	resp.ErrorCode = 0

	// Advertise support for ApiVersions, Metadata, and Produce
	// We support flexible versions for all APIs
	resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
		{ApiKey: int16(kmsg.ApiVersions), MinVersion: 0, MaxVersion: 3}, // ApiVersions (we handle v3 flexible)
		{ApiKey: int16(kmsg.Metadata), MinVersion: 0, MaxVersion: 12},   // Metadata (support up to v12)
		{ApiKey: int16(kmsg.Produce), MinVersion: 0, MaxVersion: 9},     // Produce (support up to v9)
	}

	// Advertise SASL support if enabled
	if k.saslEnabled {
		resp.ApiKeys = append(resp.ApiKeys,
			kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.SASLHandshake), MinVersion: 0, MaxVersion: 1},
			kmsg.ApiVersionsResponseApiKey{ApiKey: int16(kmsg.SASLAuthenticate), MinVersion: 0, MaxVersion: 2},
		)
	}

	k.logger.Debugf("About to send ApiVersions response (SASL enabled: %v)", k.saslEnabled)
	return k.sendResponse(conn, correlationID, resp)
}

func (k *kafkaServerInput) handleMetadataReq(conn net.Conn, correlationID int32, req *kmsg.MetadataRequest, resp *kmsg.MetadataResponse) error {

	k.logger.Debugf("Metadata request: topics count=%d, AllowAutoTopicCreation=%v, IncludeTopicAuthorizedOperations=%v",
		len(req.Topics), req.AllowAutoTopicCreation, req.IncludeTopicAuthorizedOperations)

	// Extract requested topics
	var requestedTopics []string
	for _, t := range req.Topics {
		if t.Topic != nil {
			requestedTopics = append(requestedTopics, *t.Topic)
		}
	}

	k.logger.Debugf("Requested topics: %v", requestedTopics)

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
			// Rack is not set (defaults to nil for nullable field)
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
					ErrorCode: kerr.UnknownTopicOrPartition.Code,
				})
				continue
			}
		}

		// Topic is allowed, return metadata
		resp.Topics = append(resp.Topics, kmsg.MetadataResponseTopic{
			Topic:      kmsg.StringPtr(topic),
			ErrorCode:  0,
			IsInternal: false,
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
		k.logger.Debugf("Processing topic: %s, partitions=%d", topicName, len(topic.Partitions))

		// Check if topic is allowed
		if k.allowedTopics != nil {
			if _, ok := k.allowedTopics[topicName]; !ok {
				k.logger.Warnf("Rejecting produce to disallowed topic: %s", topicName)
				continue
			}
		}

		// Iterate through partitions
		for i, partition := range topic.Partitions {
			k.logger.Debugf("Partition %d: Records=%v, len=%d", i, partition.Records != nil, len(partition.Records))
			if len(partition.Records) == 0 {
				k.logger.Debugf("Skipping partition %d (empty records)", i)
				continue
			}

			// Parse records from the record batch
			k.logger.Debugf("About to parse record batch for partition %d", i)
			messages, err := k.parseRecordBatch(partition.Records, topicName, partition.Partition, remoteAddr)
			if err != nil {
				k.logger.Errorf("Failed to parse record batch for topic=%s partition=%d: %v", topicName, partition.Partition, err)
				continue
			}
			k.logger.Debugf("Parsed %d messages from partition %d", len(messages), i)

			batch = append(batch, messages...)
		}
	}

	k.logger.Debugf("Total batch size: %d messages", len(batch))
	// If no messages, send success
	if len(batch) == 0 {
		k.logger.Debugf("Sending empty response (no messages)")
		return k.sendResponse(conn, correlationID, resp)
	}

	// Send batch to pipeline
	resChan := make(chan error, 1)
	k.logger.Debugf("Sending batch to pipeline, acks=%d", req.Acks)
	select {
	case k.msgChan <- messageBatch{
		batch: batch,
		ackFn: func(ctx context.Context, err error) error {
			resChan <- err
			return nil
		},
		resChan: resChan,
	}:
		k.logger.Debugf("Successfully sent batch to pipeline")
	case <-time.After(k.timeout):
		k.logger.Debugf("Timeout sending batch to pipeline")
		// Build timeout error response for all topics/partitions
		for _, topic := range req.Topics {
			respTopic := kmsg.ProduceResponseTopic{
				Topic: topic.Topic,
			}
			for _, partition := range topic.Partitions {
				respTopic.Partitions = append(respTopic.Partitions, kmsg.ProduceResponseTopicPartition{
					Partition: partition.Partition,
					ErrorCode: kerr.RequestTimedOut.Code,
				})
			}
			resp.Topics = append(resp.Topics, respTopic)
		}
		return k.sendResponse(conn, correlationID, resp)
	case <-k.shutdownCh:
		return fmt.Errorf("shutting down")
	}

	// Wait for acknowledgment if acks != 0 (acks can be -1 for "all", 1 for "leader")
	k.logger.Debugf("Checking acks: req.Acks=%d", req.Acks)
	if req.Acks != 0 {
		k.logger.Debugf("Waiting for acknowledgment (acks != 0)")
		select {
		case err := <-resChan:
			// Build response for all topics/partitions that were in the request
			for _, topic := range req.Topics {
				respTopic := kmsg.ProduceResponseTopic{
					Topic: topic.Topic,
				}
				for _, partition := range topic.Partitions {
					errorCode := int16(0)
					if err != nil {
						errorCode = kerr.UnknownServerError.Code
					}
					respTopic.Partitions = append(respTopic.Partitions, kmsg.ProduceResponseTopicPartition{
						Partition:      partition.Partition,
						ErrorCode:      errorCode,
						BaseOffset:     0,  // Starting offset
						LogAppendTime:  -1, // -1 means CreateTime is used
						LogStartOffset: 0,  // Earliest offset in partition
					})
				}
				resp.Topics = append(resp.Topics, respTopic)
			}
		case <-time.After(k.timeout):
			// Build timeout error response for all topics/partitions
			for _, topic := range req.Topics {
				respTopic := kmsg.ProduceResponseTopic{
					Topic: topic.Topic,
				}
				for _, partition := range topic.Partitions {
					respTopic.Partitions = append(respTopic.Partitions, kmsg.ProduceResponseTopicPartition{
						Partition: partition.Partition,
						ErrorCode: kerr.RequestTimedOut.Code,
					})
				}
				resp.Topics = append(resp.Topics, respTopic)
			}
		case <-k.shutdownCh:
			return fmt.Errorf("shutting down")
		}
	}

	return k.sendResponse(conn, correlationID, resp)
}

// decompressRecords decompresses Kafka record data based on the compression codec
func decompressRecords(data []byte, codec int8) ([]byte, error) {
	switch codec {
	case codecNone:
		return data, nil
	case codecGzip:
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer reader.Close()
		return io.ReadAll(reader)
	case codecSnappy:
		decoded, err := snappy.Decode(nil, data)
		if err != nil {
			return nil, fmt.Errorf("failed to decode snappy: %w", err)
		}
		return decoded, nil
	case codecLZ4:
		reader := lz4.NewReader(bytes.NewReader(data))
		return io.ReadAll(reader)
	case codecZstd:
		decoder, err := zstd.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd reader: %w", err)
		}
		defer decoder.Close()
		return io.ReadAll(decoder)
	default:
		return nil, fmt.Errorf("unsupported compression codec: %d", codec)
	}
}

func (k *kafkaServerInput) parseRecordBatch(data []byte, topic string, partition int32, remoteAddr string) (service.MessageBatch, error) {
	// Use kmsg.RecordBatch to parse the batch header
	recordBatch := kmsg.RecordBatch{}

	k.parseMu.Lock()
	err := recordBatch.ReadFrom(data)
	k.parseMu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("failed to parse record batch: %w", err)
	}

	k.logger.Debugf("RecordBatch has %d records, Attributes=0x%x, Records len=%d", recordBatch.NumRecords, recordBatch.Attributes, len(recordBatch.Records))

	// Check for compression (lower 3 bits of Attributes)
	compression := int8(recordBatch.Attributes & compressionCodecMask)

	// Decompress records if needed
	recordsData := recordBatch.Records
	if compression != codecNone {
		k.logger.Debugf("Decompressing records with codec %d", compression)
		decompressed, err := decompressRecords(recordsData, compression)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress records: %w", err)
		}
		recordsData = decompressed
		k.logger.Debugf("Decompressed %d bytes to %d bytes", len(recordBatch.Records), len(recordsData))
	}

	var batch service.MessageBatch

	// Parse individual records from the Records byte array
	offset := 0

	for i := 0; i < int(recordBatch.NumRecords); i++ {
		k.logger.Debugf("Parsing record %d/%d, offset=%d, recordsDataLen=%d", i+1, recordBatch.NumRecords, offset, len(recordsData))
		if offset >= len(recordsData) {
			k.logger.Warnf("Reached end of data while parsing record %d/%d", i, recordBatch.NumRecords)
			break
		}

		var recordData []byte
		var recordLen int

		// When there's only 1 record, it seems to not have a length prefix
		// When there are multiple records, each has a varint length prefix
		if recordBatch.NumRecords == 1 {
			// Single record - use all remaining data
			recordData = recordsData[offset:]
			recordLen = len(recordData)
			k.logger.Debugf("Single record mode - using all %d bytes", recordLen)
		} else {
			// Multiple records - read the varint length prefix
			lenBytesConsumed := 0
			recordLen32, lenBytesConsumed := kbin.Varint(recordsData[offset:])
			if lenBytesConsumed == 0 {
				k.logger.Warnf("Failed to read varint length for record %d", i)
				break
			}
			recordLen = int(recordLen32)
			k.logger.Debugf("Record %d has length varint: %d bytes, recordLen=%d", i, lenBytesConsumed, recordLen)
			offset += lenBytesConsumed

			if offset+recordLen > len(recordsData) {
				k.logger.Warnf("Record %d extends beyond available data: offset=%d, recordLen=%d, available=%d", i, offset, recordLen, len(recordsData))
				break
			}

			recordData = recordsData[offset : offset+recordLen]
		}

		// Parse the record
		record := kmsg.NewRecord()
		k.logger.Debugf("Parsing record %d from %d bytes, hex: %x", i, len(recordData), recordData[:min(10, len(recordData))])

		k.parseMu.Lock()
		recordErr := record.ReadFrom(recordData)
		k.parseMu.Unlock()

		if recordErr != nil {
			k.logger.Warnf("Failed to parse record %d: %v", i, recordErr)
			offset += recordLen // Skip this bad record
			continue
		}

		offset += recordLen

		// Calculate absolute timestamp
		timestamp := recordBatch.FirstTimestamp + record.TimestampDelta64
		timestampTime := time.Unix(timestamp/1000, (timestamp%1000)*1000000)

		// Create Bento message
		msg := service.NewMessage(record.Value)
		msg.MetaSetMut("kafka_server_topic", topic)
		msg.MetaSetMut("kafka_server_partition", int(partition)) // Convert to int for consistency with franz input
		msg.MetaSetMut("kafka_server_offset", recordBatch.FirstOffset+int64(record.OffsetDelta))
		msg.MetaSetMut("kafka_server_tombstone_message", record.Value == nil)

		if record.Key != nil {
			msg.MetaSetMut("kafka_server_key", string(record.Key))
		}

		msg.MetaSetMut("kafka_server_timestamp_unix", timestampTime.Unix())
		msg.MetaSetMut("kafka_server_timestamp", timestampTime.Format(time.RFC3339))
		msg.MetaSetMut("kafka_server_remote_addr", remoteAddr)

		// Add record headers as metadata
		for _, header := range record.Headers {
			msg.MetaSetMut(header.Key, string(header.Value))
		}

		batch = append(batch, msg)
	}

	return batch, nil
}

func (k *kafkaServerInput) sendErrorResponse(conn net.Conn, correlationID int32, errorCode int16) error {
	// Generic error response
	buf := kbin.AppendInt32(nil, correlationID)
	buf = kbin.AppendInt16(buf, errorCode)

	return k.writeResponse(conn, buf)
}

func (k *kafkaServerInput) sendResponse(conn net.Conn, correlationID int32, msg kmsg.Response) error {
	buf := kbin.AppendInt32(nil, correlationID)

	// For flexible responses (EXCEPT ApiVersions key 18), add response header TAG_BUFFER
	// This matches the kfake implementation in franz-go
	if msg.IsFlexible() && msg.Key() != int16(kmsg.ApiVersions) {
		buf = append(buf, 0) // Empty TAG_BUFFER (0 tags)
	}

	// AppendTo generates the response body
	buf = msg.AppendTo(buf)

	k.logger.Debugf("sendResponse: correlationID=%d, flexible=%v, key=%d, total_size=%d", correlationID, msg.IsFlexible(), msg.Key(), len(buf))

	return k.writeResponse(conn, buf)
}

func (k *kafkaServerInput) writeResponse(conn net.Conn, data []byte) error {
	// Write size
	size := int32(len(data))
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
	k.logger.Debugf("Wrote response: %d bytes", n)
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
