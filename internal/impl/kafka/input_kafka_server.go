package kafka

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Jeffail/shutdown"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/warpstreamlabs/bento/public/service"
	"github.com/xdg-go/scram"
	"golang.org/x/crypto/pbkdf2"
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

// Server configuration constants
const (
	// defaultMessageChanBuffer is the buffer size for the message channel
	defaultMessageChanBuffer = 10

	// shutdownGracePeriod is the maximum time to wait for connections to close during shutdown
	shutdownGracePeriod = 5 * time.Second

	// protocolOverheadBytes is the estimated overhead for Kafka protocol headers and metadata
	protocolOverheadBytes = 102400 // 100KB

	// requestSizeMultiplier is the multiplier applied to maxMessageBytes to account for protocol overhead
	requestSizeMultiplier = 2

	// maxClientIDLength is the maximum allowed client ID length to prevent DoS attacks
	maxClientIDLength = 10000

	// SCRAM authentication constants
	scramSaltSize   = 16   // Size of random salt for SCRAM credentials
	scramIterations = 4096 // PBKDF2 iterations (standard for SCRAM)
)

// SASL mechanism typed constants
const (
	saslMechanismPlain       = "PLAIN"
	saslMechanismScramSha256 = "SCRAM-SHA-256"
	saslMechanismScramSha512 = "SCRAM-SHA-512"
)

// SCRAM key derivation labels (RFC 5802)
const (
	scramClientKeyLabel = "Client Key"
	scramServerKeyLabel = "Server Key"
)

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
		Example("With SASL PLAIN Authentication", "Accept authenticated Kafka produce requests using PLAIN", `
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
`).
		Example("With SASL SCRAM Authentication", "Accept authenticated Kafka produce requests using SCRAM-SHA-256", `
input:
  kafka_server:
    address: "0.0.0.0:9092"
    tls:
      enabled: true
      cert_file: /path/to/cert.pem
      key_file: /path/to/key.pem
    sasl:
      - mechanism: SCRAM-SHA-256
        username: producer1
        password: secret123
      - mechanism: SCRAM-SHA-512
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
	address                 string
	allowedTopics           map[string]struct{}
	tlsConfig               *tls.Config
	saslEnabled             bool
	saslMechanisms          []string                           // List of enabled SASL mechanisms (in order)
	saslPlainCredentials    map[string]string                  // username -> password (for PLAIN)
	saslSCRAM256Credentials map[string]scram.StoredCredentials // username -> credentials (for SCRAM-SHA-256)
	saslSCRAM512Credentials map[string]scram.StoredCredentials // username -> credentials (for SCRAM-SHA-512)
	timeout                 time.Duration
	maxMessageBytes         int

	listener   net.Listener
	msgChan    chan messageBatch
	shutdownCh chan struct{}
	logger     *service.Logger

	shutSig *shutdown.Signaller
	connWG  sync.WaitGroup

	// Synchronization for safe shutdown
	shutdownOnce sync.Once
	shutdownDone atomic.Bool
	chanMu       sync.RWMutex // Protects msgChan access
	connectMu    sync.Mutex   // Protects Connect() from concurrent calls

	// Connection tracking for structured logging
	connCounter atomic.Uint64 // Generates unique connection IDs
}

type messageBatch struct {
	batch   service.MessageBatch
	ackFn   service.AckFunc
	resChan chan error
}

// generateSCRAMCredentials generates SCRAM stored credentials from a plaintext password.
// Generates StoredKey and ServerKey following RFC 5802, compatible with Kafka.
// Username is not part of key derivation per RFC 5802 - it is only used to look up credentials.
func generateSCRAMCredentials(mechanism, password string) (scram.StoredCredentials, error) {
	// Generate a random salt
	salt := make([]byte, scramSaltSize)
	if _, err := rand.Read(salt); err != nil {
		return scram.StoredCredentials{}, fmt.Errorf("failed to generate salt: %w", err)
	}

	// Determine hash function based on mechanism
	var hashFunc func() hash.Hash
	var keyLen int
	switch mechanism {
	case saslMechanismScramSha256:
		hashFunc = sha256.New
		keyLen = sha256.Size
	case saslMechanismScramSha512:
		hashFunc = sha512.New
		keyLen = sha512.Size
	default:
		return scram.StoredCredentials{}, fmt.Errorf("unsupported mechanism: %s", mechanism)
	}

	// Derive SaltedPassword using PBKDF2
	// SaltedPassword = PBKDF2(password, salt, iterations, keyLen)
	saltedPassword := pbkdf2.Key([]byte(password), salt, scramIterations, keyLen, hashFunc)

	// Compute ClientKey = HMAC(SaltedPassword, "Client Key")
	clientKeyHMAC := hmac.New(hashFunc, saltedPassword)
	clientKeyHMAC.Write([]byte(scramClientKeyLabel))
	clientKey := clientKeyHMAC.Sum(nil)

	// Compute StoredKey = Hash(ClientKey)
	storedKeyHash := hashFunc()
	storedKeyHash.Write(clientKey)
	storedKey := storedKeyHash.Sum(nil)

	// Compute ServerKey = HMAC(SaltedPassword, "Server Key")
	serverKeyHMAC := hmac.New(hashFunc, saltedPassword)
	serverKeyHMAC.Write([]byte(scramServerKeyLabel))
	serverKey := serverKeyHMAC.Sum(nil)

	// Return credentials in xdg-go/scram format.
	// Salt must be stored as raw bytes (in the string). The xdg-go SCRAM
	// library Base64-encodes the salt when emitting the server-first message,
	// so the salt is stored as raw bytes in the string.
	return scram.StoredCredentials{
		KeyFactors: scram.KeyFactors{
			Salt:  string(salt),
			Iters: scramIterations,
		},
		StoredKey: storedKey,
		ServerKey: serverKey,
	}, nil
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

		k.saslPlainCredentials = make(map[string]string)
		k.saslSCRAM256Credentials = make(map[string]scram.StoredCredentials)
		k.saslSCRAM512Credentials = make(map[string]scram.StoredCredentials)
		mechanismSet := make(map[string]bool)

		for i, saslConf := range saslList {
			mechanismStr, err := saslConf.FieldString("mechanism")
			if err != nil {
				return nil, fmt.Errorf("SASL config %d: %w", i, err)
			}

			// Validate mechanism string and store credentials accordingly
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

			switch mechanismStr {
			case saslMechanismPlain:
				k.saslPlainCredentials[username] = password
				k.logger.Infof("Registered SASL PLAIN user: %s", username)
			case saslMechanismScramSha256:
				creds, err := generateSCRAMCredentials(saslMechanismScramSha256, password)
				if err != nil {
					return nil, fmt.Errorf("SASL config %d: failed to generate SCRAM-SHA-256 credentials: %w", i, err)
				}
				k.saslSCRAM256Credentials[username] = creds
				k.logger.Infof("Registered SASL SCRAM-SHA-256 user: %s", username)
			case saslMechanismScramSha512:
				creds, err := generateSCRAMCredentials(saslMechanismScramSha512, password)
				if err != nil {
					return nil, fmt.Errorf("SASL config %d: failed to generate SCRAM-SHA-512 credentials: %w", i, err)
				}
				k.saslSCRAM512Credentials[username] = creds
				k.logger.Infof("Registered SASL SCRAM-SHA-512 user: %s", username)
			default:
				return nil, fmt.Errorf("SASL config %d: unsupported mechanism %q (supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", i, mechanismStr)
			}

			// Track unique mechanisms
			mechanismSet[mechanismStr] = true
		}

		// Build list of enabled mechanisms in priority order (SCRAM preferred over PLAIN)
		if mechanismSet[saslMechanismScramSha512] {
			k.saslMechanisms = append(k.saslMechanisms, saslMechanismScramSha512)
		}
		if mechanismSet[saslMechanismScramSha256] {
			k.saslMechanisms = append(k.saslMechanisms, saslMechanismScramSha256)
		}
		if mechanismSet[saslMechanismPlain] {
			k.saslMechanisms = append(k.saslMechanisms, saslMechanismPlain)
		}

		k.saslEnabled = len(k.saslMechanisms) > 0
		if k.saslEnabled {
			k.logger.Infof("SASL authentication enabled with mechanisms: %v", append([]string{}, k.saslMechanisms...))
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
	k.connectMu.Lock()
	defer k.connectMu.Unlock()

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
	k.setMsgChan(make(chan messageBatch, defaultMessageChanBuffer))

	go k.acceptLoop()

	return nil
}

func (k *kafkaServerInput) acceptLoop() {
	defer func() {
		// Safely close msgChan
		msgChan := k.getMsgChan()
		if msgChan != nil {
			close(msgChan)
			k.setMsgChan(nil)
		}
		k.shutSig.TriggerHasStopped()
	}()

	// Create a channel for accepting connections
	connChan := make(chan net.Conn)
	errChan := make(chan error, 1)

	go func() {
		for {
			conn, err := k.listener.Accept()
			if err != nil {
				select {
				case errChan <- err:
				case <-k.shutdownCh:
					// Shutdown triggered, exit without sending error
				}
				return
			}
			select {
			case connChan <- conn:
			case <-k.shutdownCh:
				// Shutdown triggered, close connection and exit
				conn.Close()
				return
			}
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

// connectionState tracks per-connection authentication state
type connectionState struct {
	authenticated     bool
	scramConversation *scram.ServerConversation // For SCRAM multi-step auth
	scramMechanism    string                    // Which SCRAM mechanism is being used
}

// saslAuthenticator defines the interface for SASL authentication handlers.
// This interface enables testing with mock authenticators.
type saslAuthenticator interface {
	// validatePlain validates PLAIN SASL credentials and returns true if valid.
	validatePlain(connID uint64, authBytes []byte) bool
	// handleSCRAMAuth handles SCRAM challenge-response authentication.
	// Returns (authenticated, error) where authenticated is true only when
	// the full SCRAM conversation completes successfully.
	handleSCRAMAuth(connID uint64, authBytes []byte, connState *connectionState, resp *kmsg.SASLAuthenticateResponse) (bool, error)
}

// Compile-time check that kafkaServerInput implements saslAuthenticator.
var _ saslAuthenticator = (*kafkaServerInput)(nil)

func (k *kafkaServerInput) handleConnection(conn net.Conn) {
	// Generate unique connection ID for structured logging
	connID := k.connCounter.Add(1)
	remoteAddr := conn.RemoteAddr().String()

	defer func() {
		conn.Close()
		k.connWG.Done()
		k.logger.Debugf("[conn:%d] Connection closed from %s", connID, remoteAddr)
	}()

	k.logger.Infof("[conn:%d] Accepted connection from %s", connID, remoteAddr)

	// Track authentication state for this connection
	// If SASL is disabled, consider the connection authenticated
	connState := &connectionState{
		authenticated: !k.saslEnabled,
	}

	for {
		select {
		case <-k.shutdownCh:
			return
		default:
		}

		// Set read deadline
		if err := conn.SetReadDeadline(time.Now().Add(k.timeout)); err != nil {
			k.logger.Errorf("[conn:%d] Failed to set read deadline: %v", connID, err)
			return
		}

		// Read request size (4 bytes)
		var size int32
		if err := binary.Read(conn, binary.BigEndian, &size); err != nil {
			if err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
				if netErr, ok := err.(net.Error); !ok || !netErr.Timeout() {
					k.logger.Debugf("[conn:%d] Failed to read request size: %v", connID, err)
				}
			}
			return
		}

		// Request size should not exceed maxMessageBytes * requestSizeMultiplier (to account for protocol overhead)
		// Plus a reasonable upper bound for headers and metadata
		maxRequestSize := int32(k.maxMessageBytes)*requestSizeMultiplier + protocolOverheadBytes
		if size <= 0 || size > maxRequestSize {
			k.logger.Errorf("[conn:%d] Invalid request size: %d (max: %d)", connID, size, maxRequestSize)
			return
		}

		// Read request data
		requestData := make([]byte, size)
		if _, err := io.ReadFull(conn, requestData); err != nil {
			k.logger.Errorf("[conn:%d] Failed to read request data: %v", connID, err)
			return
		}

		// Parse and handle request (may update authenticated state)
		authUpdated, err := k.handleRequest(conn, connID, remoteAddr, requestData, connState)
		if err != nil {
			k.logger.Errorf("[conn:%d] Failed to handle request: %v", connID, err)
			return
		}

		// If authentication was just completed, log it
		if authUpdated && connState.authenticated {
			k.logger.Infof("[conn:%d] Client authenticated successfully", connID)
		}
	}
}

// requestHeader contains parsed Kafka request header fields.
type requestHeader struct {
	apiKey        int16
	apiVersion    int16
	correlationID int32
	isFlexible    bool
	bodyOffset    int
}

// parseRequestHeader parses a Kafka request header and returns the header fields and body offset.
func parseRequestHeader(data []byte) (requestHeader, error) {
	if len(data) < 8 {
		return requestHeader{}, fmt.Errorf("request too small: %d bytes", len(data))
	}

	b := kbin.Reader{Src: data}
	hdr := requestHeader{
		apiKey:        b.Int16(),
		apiVersion:    b.Int16(),
		correlationID: b.Int32(),
	}

	// Determine if flexible request based on API key and version
	switch kmsg.Key(hdr.apiKey) {
	case kmsg.ApiVersions:
		hdr.isFlexible = hdr.apiVersion >= 3
	case kmsg.Metadata:
		hdr.isFlexible = hdr.apiVersion >= 9
	case kmsg.Produce:
		hdr.isFlexible = hdr.apiVersion >= 9
	case kmsg.SASLHandshake:
		hdr.isFlexible = false // SASL Handshake is never flexible
	case kmsg.SASLAuthenticate:
		hdr.isFlexible = hdr.apiVersion >= 2
	}

	// Read clientID (nullable string: int16 length, then data)
	clientIDLen := b.Int16()
	if clientIDLen > maxClientIDLength {
		return requestHeader{}, fmt.Errorf("client ID too large: %d bytes", clientIDLen)
	}
	if clientIDLen > 0 {
		b.Span(int(clientIDLen))
	}

	if hdr.isFlexible {
		// Flexible header v2 has TAG_BUFFER (compact array of tagged fields)
		tagCount := b.Uvarint()
		for i := 0; i < int(tagCount); i++ {
			_ = b.Uvarint() // tagID (unused)
			tagSize := b.Uvarint()
			b.Span(int(tagSize))
		}
	}

	hdr.bodyOffset = len(data) - len(b.Src)
	return hdr, nil
}

func (k *kafkaServerInput) handleRequest(conn net.Conn, connID uint64, remoteAddr string, data []byte, connState *connectionState) (bool, error) {
	hdr, err := parseRequestHeader(data)
	if err != nil {
		k.logger.Errorf("[conn:%d] Failed to parse request header: %v", connID, err)
		return false, err
	}

	k.logger.Debugf("[conn:%d] Received request: apiKey=%d, apiVersion=%d, correlationID=%d, size=%d", connID, hdr.apiKey, hdr.apiVersion, hdr.correlationID, len(data))

	// Check if client needs authentication first (only if SASL is enabled)
	if k.saslEnabled && !connState.authenticated {
		switch kmsg.Key(hdr.apiKey) {
		case kmsg.ApiVersions, kmsg.SASLHandshake, kmsg.SASLAuthenticate:
			// These are allowed before authentication
		default:
			k.logger.Warnf("[conn:%d] Rejecting unauthenticated request for API key %d", connID, hdr.apiKey)
			return false, fmt.Errorf("authentication required")
		}
	}

	k.logger.Debugf("[conn:%d] Request body from offset %d (isFlexible=%v): %x", connID, hdr.bodyOffset, hdr.isFlexible, data[hdr.bodyOffset:min(hdr.bodyOffset+20, len(data))])

	var handleErr error
	authUpdated := false

	switch kmsg.Key(hdr.apiKey) {
	case kmsg.ApiVersions:
		// ApiVersions request has minimal/no body
		k.logger.Debugf("[conn:%d] Handling ApiVersions", connID)
		resp := kmsg.NewApiVersionsResponse()
		resp.SetVersion(hdr.apiVersion)
		handleErr = k.handleApiVersionsReq(conn, connID, hdr.correlationID, nil, &resp)
	case kmsg.SASLHandshake:
		// Handle SASL handshake
		k.logger.Debugf("[conn:%d] Handling SASLHandshake", connID)
		handleErr = k.handleSaslHandshake(conn, connID, hdr.correlationID, data[hdr.bodyOffset:], hdr.apiVersion, connState)
	case kmsg.SASLAuthenticate:
		// Handle SASL authentication
		k.logger.Debugf("[conn:%d] Handling SASLAuthenticate", connID)
		k.logger.Debugf("[conn:%d] SASLAuthenticate body offset=%d len=%d first bytes: %x", connID, hdr.bodyOffset, len(data[hdr.bodyOffset:]), data[hdr.bodyOffset:min(hdr.bodyOffset+40, len(data))])
		authSuccess := false
		authSuccess, handleErr = k.handleSaslAuthenticate(conn, connID, hdr.correlationID, data[hdr.bodyOffset:], hdr.apiVersion, connState)
		if handleErr == nil && authSuccess {
			connState.authenticated = true
			authUpdated = true
		}
	case kmsg.Metadata:
		// Parse metadata request body (after header)
		req := kmsg.NewMetadataRequest()
		req.SetVersion(hdr.apiVersion)
		resp := kmsg.NewMetadataResponse()
		resp.SetVersion(hdr.apiVersion)

		parseErr := req.ReadFrom(data[hdr.bodyOffset:])

		if parseErr != nil {
			k.logger.Errorf("[conn:%d] Failed to parse MetadataRequest: %v", connID, parseErr)
			// Send empty response - can't properly signal error without knowing requested topics
			// The connection will likely be closed by the client after this invalid request
			handleErr = k.sendResponse(conn, connID, hdr.correlationID, &resp)
		} else {
			k.logger.Debugf("[conn:%d] ReadFrom SUCCEEDED for Metadata at offset=%d, topics count=%d", connID, hdr.bodyOffset, len(req.Topics))
			handleErr = k.handleMetadataReq(conn, connID, hdr.correlationID, &req, &resp)
		}
	case kmsg.Produce:
		// Parse produce request body (after header)
		req := kmsg.NewProduceRequest()
		req.SetVersion(hdr.apiVersion)
		resp := kmsg.NewProduceResponse()
		resp.SetVersion(hdr.apiVersion)
		k.logger.Debugf("[conn:%d] About to call ReadFrom for Produce, body size=%d (offset=%d)", connID, len(data[hdr.bodyOffset:]), hdr.bodyOffset)

		parseErr := req.ReadFrom(data[hdr.bodyOffset:])

		if parseErr != nil {
			k.logger.Errorf("[conn:%d] Failed to parse ProduceRequest: %v", connID, parseErr)
			// Send empty response - can't properly signal error without knowing which topics/partitions
			// were requested. The connection will likely be closed by the client after this invalid request.
			handleErr = k.sendResponse(conn, connID, hdr.correlationID, &resp)
		} else {
			handleErr = k.handleProduceReq(conn, connID, remoteAddr, hdr.correlationID, &req, &resp)
		}
	default:
		k.logger.Warnf("[conn:%d] Unsupported API key: %d, closing connection", connID, hdr.apiKey)
		// For unsupported API keys, a proper response cannot be constructed without knowing the structure
		// The best approach is to close the connection, which signals an error to the client
		return false, fmt.Errorf("unsupported API key: %d", hdr.apiKey)
	}

	return authUpdated, handleErr
}

func (k *kafkaServerInput) handleSaslHandshake(conn net.Conn, connID uint64, correlationID int32, data []byte, apiVersion int16, connState *connectionState) error {
	req := kmsg.NewSASLHandshakeRequest()
	req.SetVersion(apiVersion)
	resp := kmsg.NewSASLHandshakeResponse()
	resp.SetVersion(apiVersion)

	err := req.ReadFrom(data)

	if err != nil {
		k.logger.Errorf("[conn:%d] Failed to parse SASL handshake request: %v", connID, err)
		// Send proper error response instead of generic error
		resp.ErrorCode = kerr.InvalidRequest.Code
		// Send supported mechanisms as-is
		resp.SupportedMechanisms = append([]string{}, k.saslMechanisms...)
		return k.sendResponse(conn, connID, correlationID, &resp)
	}

	k.logger.Debugf("[conn:%d] SASL mechanism %s requested", connID, req.Mechanism)

	// Check if requested mechanism is in the enabled list
	supported := false
	for _, mech := range k.saslMechanisms {
		if mech == req.Mechanism {
			supported = true
			break
		}
	}

	if supported {
		k.logger.Debugf("[conn:%d] SASL mechanism %s accepted", connID, req.Mechanism)
		resp.ErrorCode = 0
		// Store the mechanism for this connection
		connState.scramMechanism = req.Mechanism
	} else {
		k.logger.Warnf("[conn:%d] Unsupported SASL mechanism requested: %s", connID, req.Mechanism)
		resp.ErrorCode = kerr.UnsupportedSaslMechanism.Code
	}

	// Always send supported mechanism list
	resp.SupportedMechanisms = append([]string{}, k.saslMechanisms...)

	return k.sendResponse(conn, connID, correlationID, &resp)
}

func (k *kafkaServerInput) handleSaslAuthenticate(conn net.Conn, connID uint64, correlationID int32, data []byte, apiVersion int16, connState *connectionState) (bool, error) {
	// Create response using kmsg (like kfake does)
	req := kmsg.NewSASLAuthenticateRequest()
	req.SetVersion(apiVersion)
	resp := req.ResponseKind().(*kmsg.SASLAuthenticateResponse)

	k.logger.Debugf("[conn:%d] handleSaslAuthenticate: incoming body len=%d, apiVersion=%d", connID, len(data), apiVersion)

	// Parse auth bytes based on API version:
	// - v0, v1: AuthBytes is BYTES (Int32 length-prefixed)
	// - v2: AuthBytes is COMPACT_BYTES (uvarint len+1)
	b := kbin.Reader{Src: data}
	var authBytes []byte
	if apiVersion >= 2 {
		// v2 uses COMPACT_BYTES, but be tolerant: some clients mistakenly send Int32 (BYTES) prefixed data.
		authBytes = b.CompactBytes()
		k.logger.Debugf("[conn:%d] Parsed SASL auth bytes as CompactBytes (len=%d)", connID, len(authBytes))

		// Heuristic fallback: if compact parsing resulted in empty bytes or the payload
		// looks like an Int32 length-prefixed buffer, try parsing that format.
		if len(authBytes) == 0 && len(data) >= 4 {
			payloadLen := int(binary.BigEndian.Uint32(data[0:4]))
			if payloadLen <= len(data)-4 {
				authBytes = data[4 : 4+payloadLen]
				k.logger.Debugf("[conn:%d] Fallback parsed SASL auth bytes as Int32 BYTES (len=%d)", connID, len(authBytes))
			}
		}
	} else {
		authBytes = b.Bytes()
		k.logger.Debugf("[conn:%d] Parsed SASL auth bytes as Bytes (len=%d)", connID, len(authBytes))
	}

	var authenticated bool

	// Check if mechanism was set during handshake
	if connState.scramMechanism == "" {
		k.logger.Errorf("[conn:%d] SASLAuthenticate received without prior handshake", connID)
		resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
		errMsg := "SASL handshake required before authentication"
		resp.ErrorMessage = &errMsg
		return false, k.sendResponse(conn, connID, correlationID, resp)
	}

	// Handle authentication based on mechanism
	k.logger.Debugf("[conn:%d] handleSaslAuthenticate: mechanism=%s, authBytes len=%d", connID, connState.scramMechanism, len(authBytes))
	switch connState.scramMechanism {
	case saslMechanismPlain:
		// Validate PLAIN credentials
		authenticated = k.validatePlain(connID, authBytes)
		if authenticated {
			resp.ErrorCode = 0
			k.logger.Debugf("[conn:%d] SASL PLAIN authentication succeeded", connID)
		} else {
			k.logger.Warnf("[conn:%d] SASL PLAIN authentication failed", connID)
			resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
			errMsg := "Authentication failed"
			resp.ErrorMessage = &errMsg
		}

	case saslMechanismScramSha256, saslMechanismScramSha512:
		// Handle SCRAM challenge-response
		var authErr error
		authenticated, authErr = k.handleSCRAMAuth(connID, authBytes, connState, resp)
		if authErr != nil {
			k.logger.Errorf("[conn:%d] SCRAM authentication error: %v", connID, authErr)
			resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
			errMsg := authErr.Error()
			resp.ErrorMessage = &errMsg
			authenticated = false
		}

	default:
		k.logger.Errorf("[conn:%d] Unknown SASL mechanism: %s", connID, connState.scramMechanism)
		resp.ErrorCode = kerr.SaslAuthenticationFailed.Code
		errMsg := "Unknown SASL mechanism"
		resp.ErrorMessage = &errMsg
		authenticated = false
	}

	sendErr := k.sendResponse(conn, connID, correlationID, resp)
	return authenticated, sendErr
}

// getCredentialLookup returns a credential lookup function for the specified SCRAM mechanism.
func (k *kafkaServerInput) getCredentialLookup(connID uint64, mechanism string) scram.CredentialLookup {
	var credsMap map[string]scram.StoredCredentials
	if mechanism == saslMechanismScramSha512 {
		credsMap = k.saslSCRAM512Credentials
	} else {
		credsMap = k.saslSCRAM256Credentials
	}

	return func(username string) (scram.StoredCredentials, error) {
		creds, ok := credsMap[username]
		if !ok {
			k.logger.Warnf("[conn:%d] User not found: %s", connID, username)
			return scram.StoredCredentials{}, fmt.Errorf("user not found: %s", username)
		}
		return creds, nil
	}
}

// newSCRAMServer creates a new SCRAM server for the specified mechanism.
func newSCRAMServer(mechanism string, credLookup scram.CredentialLookup) (*scram.Server, error) {
	switch mechanism {
	case saslMechanismScramSha256:
		return scram.SHA256.NewServer(credLookup)
	case saslMechanismScramSha512:
		return scram.SHA512.NewServer(credLookup)
	default:
		return nil, fmt.Errorf("unsupported SCRAM mechanism: %s", mechanism)
	}
}

// handleSCRAMAuth handles SCRAM challenge-response authentication
func (k *kafkaServerInput) handleSCRAMAuth(connID uint64, authBytes []byte, connState *connectionState, resp *kmsg.SASLAuthenticateResponse) (bool, error) {
	// Trim leading control bytes (0x00/0x01) that some clients may include
	for len(authBytes) > 0 && (authBytes[0] == 0x00 || authBytes[0] == 0x01) {
		authBytes = authBytes[1:]
	}

	// Trim trailing NUL padding that some clients append
	for len(authBytes) > 0 && authBytes[len(authBytes)-1] == 0x00 {
		authBytes = authBytes[:len(authBytes)-1]
	}

	clientMessage := string(authBytes)

	// If this is the first message (no conversation yet), create one
	if connState.scramConversation == nil {
		k.logger.Debugf("[conn:%d] Starting SCRAM conversation for mechanism: %s", connID, connState.scramMechanism)

		credLookup := k.getCredentialLookup(connID, connState.scramMechanism)
		scramServer, err := newSCRAMServer(connState.scramMechanism, credLookup)
		if err != nil {
			k.logger.Errorf("[conn:%d] Failed to create SCRAM server: %v", connID, err)
			return false, fmt.Errorf("failed to create SCRAM server: %w", err)
		}

		// Start new conversation
		connState.scramConversation = scramServer.NewConversation()
	}

	// Process the client message
	serverMessage, err := connState.scramConversation.Step(clientMessage)
	if err != nil {
		k.logger.Errorf("[conn:%d] SCRAM conversation step failed: %v", connID, err)
		return false, fmt.Errorf("authentication failed: %w", err)
	}

	// Set server response
	resp.SASLAuthBytes = []byte(serverMessage)
	resp.ErrorCode = 0

	// Check if conversation is complete
	if connState.scramConversation.Done() {
		if connState.scramConversation.Valid() {
			k.logger.Debugf("[conn:%d] SCRAM authentication succeeded", connID)
			return true, nil
		}
		k.logger.Warnf("[conn:%d] SCRAM authentication failed: invalid credentials", connID)
		return false, fmt.Errorf("invalid credentials")
	}

	// Conversation continues (need more steps)
	return false, nil
}

// validatePlain validates PLAIN SASL credentials
// PLAIN format: [authzid] \0 username \0 password
func (k *kafkaServerInput) validatePlain(connID uint64, authBytes []byte) bool {
	// Split by null bytes
	parts := bytes.Split(authBytes, []byte{0})

	if len(parts) < 3 {
		k.logger.Debugf("[conn:%d] Invalid PLAIN auth format: expected at least 3 parts, got %d", connID, len(parts))
		return false
	}

	// parts[0] is authzid (authorization identity), usually empty
	// parts[1] is username (authentication identity)
	// parts[2] is password
	username := string(parts[1])
	password := string(parts[2])

	k.logger.Debugf("[conn:%d] Validating credentials for username: %s", connID, username)

	expectedPassword, exists := k.saslPlainCredentials[username]
	if !exists {
		k.logger.Debugf("[conn:%d] User not found: %s", connID, username)
		return false
	}

	// Use constant-time comparison to prevent timing attacks
	if subtle.ConstantTimeCompare([]byte(expectedPassword), []byte(password)) != 1 {
		k.logger.Debugf("[conn:%d] Password mismatch for user: %s", connID, username)
		return false
	}

	k.logger.Debugf("[conn:%d] Credentials validated successfully for user: %s", connID, username)
	return true
}

func (k *kafkaServerInput) handleApiVersionsReq(conn net.Conn, connID uint64, correlationID int32, req *kmsg.ApiVersionsRequest, resp *kmsg.ApiVersionsResponse) error {
	k.logger.Debugf("[conn:%d] Handling ApiVersions request", connID)

	resp.ErrorCode = 0

	// Advertise support for ApiVersions, Metadata, and Produce.
	// Flexible versions are supported for all APIs.
	resp.ApiKeys = []kmsg.ApiVersionsResponseApiKey{
		{ApiKey: int16(kmsg.ApiVersions), MinVersion: 0, MaxVersion: 3}, // ApiVersions (v3 flexible)
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

	k.logger.Debugf("[conn:%d] Sending ApiVersions response (SASL enabled: %v)", connID, k.saslEnabled)
	return k.sendResponse(conn, connID, correlationID, resp)
}

func (k *kafkaServerInput) handleMetadataReq(conn net.Conn, connID uint64, correlationID int32, req *kmsg.MetadataRequest, resp *kmsg.MetadataResponse) error {

	k.logger.Debugf("[conn:%d] Metadata request: topics count=%d, AllowAutoTopicCreation=%v, IncludeTopicAuthorizedOperations=%v",
		connID, len(req.Topics), req.AllowAutoTopicCreation, req.IncludeTopicAuthorizedOperations)

	// Extract requested topics
	var requestedTopics []string
	for _, t := range req.Topics {
		if t.Topic != nil {
			requestedTopics = append(requestedTopics, *t.Topic)
		}
	}

	k.logger.Debugf("[conn:%d] Requested topics: %v", connID, requestedTopics)

	// Add a single broker (ourselves)
	// Parse host and port from address
	host, portStr, err := net.SplitHostPort(k.address)
	if err != nil {
		k.logger.Errorf("[conn:%d] Failed to parse address %s: %v", connID, k.address, err)
		// Fallback to defaults
		host = "127.0.0.1"
		portStr = "9092"
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		k.logger.Errorf("[conn:%d] Failed to parse port %s: %v", connID, portStr, err)
		port = 9092
	}

	// If listening on wildcard address, use the connection's local address
	// so clients can connect back to the same interface they reached the server on
	if host == "" || host == "0.0.0.0" || host == "::" {
		if localAddr := conn.LocalAddr(); localAddr != nil {
			if localHost, _, err := net.SplitHostPort(localAddr.String()); err == nil && localHost != "" {
				k.logger.Debugf("[conn:%d] Using connection local address %s instead of wildcard %s", connID, localHost, host)
				host = localHost
			}
		}
	}

	k.logger.Debugf("[conn:%d] Broker metadata: host=%s, port=%d", connID, host, port)

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
		k.logger.Debugf("[conn:%d] Returning requested topics: %v", connID, topicsToReturn)
	} else if k.allowedTopics != nil {
		// No specific topics requested, return all allowed topics
		for topic := range k.allowedTopics {
			topicsToReturn = append(topicsToReturn, topic)
		}
		k.logger.Debugf("[conn:%d] Returning allowed topics: %v", connID, topicsToReturn)
	} else {
		k.logger.Debugf("[conn:%d] No topics to return (no filter configured, no specific topics requested)", connID)
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

	k.logger.Debugf("[conn:%d] Sending metadata response with %d topics", connID, len(resp.Topics))
	return k.sendResponse(conn, connID, correlationID, resp)
}

// buildProduceResponseTopics constructs response topics for all topics/partitions in a produce request.
func buildProduceResponseTopics(topics []kmsg.ProduceRequestTopic, errorCode int16) []kmsg.ProduceResponseTopic {
	result := make([]kmsg.ProduceResponseTopic, 0, len(topics))
	for _, topic := range topics {
		respTopic := kmsg.ProduceResponseTopic{
			Topic:      topic.Topic,
			Partitions: make([]kmsg.ProduceResponseTopicPartition, 0, len(topic.Partitions)),
		}
		for _, partition := range topic.Partitions {
			respTopic.Partitions = append(respTopic.Partitions, kmsg.ProduceResponseTopicPartition{
				Partition:      partition.Partition,
				ErrorCode:      errorCode,
				BaseOffset:     0,
				LogAppendTime:  -1,
				LogStartOffset: 0,
			})
		}
		result = append(result, respTopic)
	}
	return result
}

func (k *kafkaServerInput) handleProduceReq(conn net.Conn, connID uint64, remoteAddr string, correlationID int32, req *kmsg.ProduceRequest, resp *kmsg.ProduceResponse) error {
	k.logger.Infof("[conn:%d] Produce request: correlationID=%d, acks=%d, topics=%d", connID, correlationID, req.Acks, len(req.Topics))

	// Create context with timeout for this request
	ctx, cancel := context.WithTimeout(context.Background(), k.timeout)
	defer cancel()

	var batch service.MessageBatch

	// Iterate through topics using kmsg's typed structures
	for _, topic := range req.Topics {
		topicName := topic.Topic
		k.logger.Debugf("[conn:%d] Processing topic: %s, partitions=%d", connID, topicName, len(topic.Partitions))

		// Check if topic is allowed
		if k.allowedTopics != nil {
			if _, ok := k.allowedTopics[topicName]; !ok {
				k.logger.Warnf("[conn:%d] Rejecting produce to disallowed topic: %s", connID, topicName)
				// Return error for disallowed topic instead of silently dropping
				respTopic := kmsg.ProduceResponseTopic{
					Topic: topicName,
				}
				for _, partition := range topic.Partitions {
					respTopic.Partitions = append(respTopic.Partitions, kmsg.ProduceResponseTopicPartition{
						Partition: partition.Partition,
						ErrorCode: kerr.UnknownTopicOrPartition.Code,
					})
				}
				resp.Topics = append(resp.Topics, respTopic)
				continue
			}
		}

		// Iterate through partitions
		for i, partition := range topic.Partitions {
			k.logger.Debugf("[conn:%d] Partition %d: Records=%v, len=%d", connID, i, partition.Records != nil, len(partition.Records))
			if len(partition.Records) == 0 {
				k.logger.Debugf("[conn:%d] Skipping partition %d (empty records)", connID, i)
				continue
			}

			// Parse records from the record batch
			k.logger.Debugf("[conn:%d] About to parse record batch for partition %d", connID, i)
			messages, err := k.parseRecordBatch(connID, partition.Records, topicName, partition.Partition, remoteAddr)
			if err != nil {
				k.logger.Errorf("[conn:%d] Failed to parse record batch for topic=%s partition=%d: %v", connID, topicName, partition.Partition, err)
				continue
			}
			k.logger.Debugf("[conn:%d] Parsed %d messages from partition %d", connID, len(messages), i)

			batch = append(batch, messages...)
		}
	}

	k.logger.Debugf("[conn:%d] Total batch size: %d messages", connID, len(batch))
	// If no messages, still need to build response for all requested topics/partitions
	if len(batch) == 0 {
		k.logger.Debugf("[conn:%d] No messages to process, building success response", connID)
		resp.Topics = buildProduceResponseTopics(req.Topics, 0)
		return k.sendResponse(conn, connID, correlationID, resp)
	}

	// Send batch to pipeline
	resChan := make(chan error, 1)
	k.logger.Debugf("[conn:%d] Sending batch to pipeline, acks=%d", connID, req.Acks)
	msgChan := k.getMsgChan()
	if msgChan == nil {
		k.logger.Errorf("[conn:%d] msgChan is nil, cannot send batch", connID)
		return fmt.Errorf("server not connected")
	}
	select {
	case msgChan <- messageBatch{
		batch: batch,
		ackFn: func(ackCtx context.Context, err error) error {
			resChan <- err
			return nil
		},
		resChan: resChan,
	}:
		k.logger.Debugf("[conn:%d] Successfully sent batch to pipeline", connID)
	case <-ctx.Done():
		k.logger.Warnf("[conn:%d] Timeout sending batch to pipeline", connID)
		resp.Topics = buildProduceResponseTopics(req.Topics, kerr.RequestTimedOut.Code)
		return k.sendResponse(conn, connID, correlationID, resp)
	case <-k.shutdownCh:
		return fmt.Errorf("shutting down")
	}

	// Wait for acknowledgment if acks != 0 (acks can be -1 for "all", 1 for "leader")
	k.logger.Debugf("[conn:%d] Checking acks: req.Acks=%d", connID, req.Acks)
	if req.Acks != 0 {
		k.logger.Debugf("[conn:%d] Waiting for acknowledgment (acks != 0)", connID)
		select {
		case err := <-resChan:
			errorCode := int16(0)
			if err != nil {
				errorCode = kerr.UnknownServerError.Code
			}
			resp.Topics = buildProduceResponseTopics(req.Topics, errorCode)
		case <-ctx.Done():
			resp.Topics = buildProduceResponseTopics(req.Topics, kerr.RequestTimedOut.Code)
		case <-k.shutdownCh:
			return fmt.Errorf("shutting down")
		}
	} else {
		// When acks=0, don't wait for acknowledgment but still build response
		k.logger.Debugf("[conn:%d] acks=0, sending immediate success response", connID)
		resp.Topics = buildProduceResponseTopics(req.Topics, 0)
	}

	return k.sendResponse(conn, connID, correlationID, resp)
}

// xerialSnappyMagic is the magic header for Xerial snappy framing used by Kafka.
// Format: 0x82 "SNAPPY" 0x00
var xerialSnappyMagic = []byte{0x82, 0x53, 0x4e, 0x41, 0x50, 0x50, 0x59, 0x00}

// decodeXerialSnappy decodes Xerial-framed snappy data (used by Kafka).
// Format: 8-byte magic, 8-byte version info, then chunks of (4-byte size + snappy data).
func decodeXerialSnappy(data []byte) ([]byte, error) {
	if len(data) < 16 {
		return nil, fmt.Errorf("xerial snappy data too short: %d bytes", len(data))
	}

	// Skip magic (8 bytes) and version info (8 bytes)
	data = data[16:]

	var result []byte
	for len(data) > 0 {
		if len(data) < 4 {
			return nil, fmt.Errorf("xerial snappy: incomplete chunk size")
		}
		chunkSize := int(binary.BigEndian.Uint32(data))
		data = data[4:]

		if chunkSize < 0 || chunkSize > len(data) {
			return nil, fmt.Errorf("xerial snappy: invalid chunk size %d", chunkSize)
		}

		decoded, err := snappy.Decode(nil, data[:chunkSize])
		if err != nil {
			return nil, fmt.Errorf("xerial snappy: failed to decode chunk: %w", err)
		}
		result = append(result, decoded...)
		data = data[chunkSize:]
	}
	return result, nil
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
		// Check for Xerial snappy framing (used by Kafka Java client and others)
		if len(data) >= 8 && bytes.HasPrefix(data, xerialSnappyMagic) {
			return decodeXerialSnappy(data)
		}
		// Fall back to raw snappy
		decoded, err := snappy.Decode(nil, data)
		if err != nil {
			return nil, fmt.Errorf("failed to decode snappy: %w", err)
		}
		return decoded, nil
	case codecLZ4:
		// Try standard LZ4 frame format first
		reader := lz4.NewReader(bytes.NewReader(data))
		decoded, err := io.ReadAll(reader)
		if err != nil {
			// Some clients use raw LZ4 blocks without frame wrapper
			// Try block decompression as fallback
			blockDecoded := make([]byte, 10*len(data)) // Allocate generous buffer
			n, err2 := lz4.UncompressBlock(data, blockDecoded)
			if err2 != nil {
				return nil, fmt.Errorf("failed to decompress lz4 (frame: %v, block: %v)", err, err2)
			}
			return blockDecoded[:n], nil
		}
		return decoded, nil
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

func (k *kafkaServerInput) parseRecordBatch(connID uint64, data []byte, topic string, partition int32, remoteAddr string) (service.MessageBatch, error) {
	// Check magic byte at offset 16 to determine format
	// Offset 16 is where magic byte appears in both MessageSet and RecordBatch formats
	// Magic 0 or 1 = legacy MessageSet format
	// Magic 2 = v2 RecordBatch format
	if len(data) < 17 {
		return nil, fmt.Errorf("data too short: %d bytes", len(data))
	}

	magic := data[16]
	k.logger.Debugf("[conn:%d] Record format magic byte: %d", connID, magic)

	if magic < 2 {
		// Legacy MessageSet format (magic 0 or 1)
		return k.parseMessageSet(connID, data, topic, partition, remoteAddr)
	}

	// v2 RecordBatch format
	return k.parseRecordBatchV2(connID, data, topic, partition, remoteAddr)
}

// parseMessageSet handles legacy MessageSet format (magic 0 or 1) using kmsg types
func (k *kafkaServerInput) parseMessageSet(connID uint64, data []byte, topic string, partition int32, remoteAddr string) (service.MessageBatch, error) {
	k.logger.Debugf("[conn:%d] Parsing legacy MessageSet format", connID)

	var batch service.MessageBatch
	offset := 0

	for offset < len(data) {
		if offset+17 > len(data) { // Minimum size: 8 (offset) + 4 (size) + 4 (crc) + 1 (magic)
			k.logger.Debugf("[conn:%d] End of MessageSet at offset %d", connID, offset)
			break
		}

		// Check magic byte to determine v0 or v1
		magic := data[offset+16]

		if magic == 0 {
			// Parse as MessageV0
			msgV0 := kmsg.NewMessageV0()
			if err := msgV0.ReadFrom(data[offset:]); err != nil {
				k.logger.Warnf("[conn:%d] Failed to parse MessageV0: %v", connID, err)
				break
			}

			// Handle compression
			compression := msgV0.Attributes & 0x07
			if compression != 0 {
				decompressed, err := decompressRecords(msgV0.Value, int8(compression))
				if err != nil {
					k.logger.Warnf("[conn:%d] Failed to decompress MessageV0: %v", connID, err)
				} else {
					innerBatch, err := k.parseMessageSet(connID, decompressed, topic, partition, remoteAddr)
					if err != nil {
						k.logger.Warnf("[conn:%d] Failed to parse inner MessageSet: %v", connID, err)
					} else {
						batch = append(batch, innerBatch...)
					}
				}
			} else {
				msg := service.NewMessage(msgV0.Value)
				msg.MetaSetMut("kafka_server_topic", topic)
				msg.MetaSetMut("kafka_server_partition", int(partition))
				msg.MetaSetMut("kafka_server_offset", msgV0.Offset)
				msg.MetaSetMut("kafka_server_tombstone_message", msgV0.Value == nil)
				if msgV0.Key != nil {
					msg.MetaSetMut("kafka_server_key", string(msgV0.Key))
				}
				msg.MetaSetMut("kafka_server_client_address", remoteAddr)
				batch = append(batch, msg)
			}

			offset += 8 + 4 + int(msgV0.MessageSize) // Offset + MessageSize field + message content
		} else if magic == 1 {
			// Parse as MessageV1
			msgV1 := kmsg.NewMessageV1()
			if err := msgV1.ReadFrom(data[offset:]); err != nil {
				k.logger.Warnf("[conn:%d] Failed to parse MessageV1: %v", connID, err)
				break
			}

			// Handle compression
			compression := msgV1.Attributes & 0x07
			if compression != 0 {
				decompressed, err := decompressRecords(msgV1.Value, int8(compression))
				if err != nil {
					k.logger.Warnf("[conn:%d] Failed to decompress MessageV1: %v", connID, err)
				} else {
					innerBatch, err := k.parseMessageSet(connID, decompressed, topic, partition, remoteAddr)
					if err != nil {
						k.logger.Warnf("[conn:%d] Failed to parse inner MessageSet: %v", connID, err)
					} else {
						batch = append(batch, innerBatch...)
					}
				}
			} else {
				msg := service.NewMessage(msgV1.Value)
				msg.MetaSetMut("kafka_server_topic", topic)
				msg.MetaSetMut("kafka_server_partition", int(partition))
				msg.MetaSetMut("kafka_server_offset", msgV1.Offset)
				msg.MetaSetMut("kafka_server_tombstone_message", msgV1.Value == nil)
				if msgV1.Key != nil {
					msg.MetaSetMut("kafka_server_key", string(msgV1.Key))
				}
				timestampTime := time.Unix(msgV1.Timestamp/1000, (msgV1.Timestamp%1000)*1000000)
				msg.MetaSetMut("kafka_server_timestamp_unix", timestampTime.Unix())
				msg.MetaSetMut("kafka_server_timestamp", timestampTime.Format(time.RFC3339))
				msg.MetaSetMut("kafka_server_client_address", remoteAddr)
				batch = append(batch, msg)
			}

			offset += 8 + 4 + int(msgV1.MessageSize) // Offset + MessageSize field + message content
		} else {
			k.logger.Warnf("[conn:%d] Unexpected magic byte %d in MessageSet", connID, magic)
			break
		}
	}

	k.logger.Debugf("[conn:%d] Parsed %d messages from MessageSet", connID, len(batch))
	return batch, nil
}

// parseRecordBatchV2 handles v2 RecordBatch format (magic 2)
func (k *kafkaServerInput) parseRecordBatchV2(connID uint64, data []byte, topic string, partition int32, remoteAddr string) (service.MessageBatch, error) {
	// Use kmsg.RecordBatch to parse the batch header
	recordBatch := kmsg.RecordBatch{}

	err := recordBatch.ReadFrom(data)

	if err != nil {
		return nil, fmt.Errorf("failed to parse record batch: %w", err)
	}

	k.logger.Debugf("[conn:%d] RecordBatch has %d records, Attributes=0x%x, Records len=%d", connID, recordBatch.NumRecords, recordBatch.Attributes, len(recordBatch.Records))

	// Check for compression (lower 3 bits of Attributes)
	compression := int8(recordBatch.Attributes & compressionCodecMask)

	// Decompress records if needed
	recordsData := recordBatch.Records
	if compression != codecNone {
		k.logger.Debugf("[conn:%d] Decompressing records with codec %d", connID, compression)
		decompressed, err := decompressRecords(recordsData, compression)
		if err != nil {
			return nil, fmt.Errorf("failed to decompress records: %w", err)
		}
		recordsData = decompressed
		k.logger.Debugf("[conn:%d] Decompressed %d bytes to %d bytes", connID, len(recordBatch.Records), len(recordsData))
	}

	// Pre-allocate batch with expected capacity
	batch := make(service.MessageBatch, 0, recordBatch.NumRecords)

	// Parse individual records from the Records byte array
	offset := 0

	for i := 0; i < int(recordBatch.NumRecords); i++ {
		k.logger.Debugf("[conn:%d] Parsing record %d/%d, offset=%d, recordsDataLen=%d", connID, i+1, recordBatch.NumRecords, offset, len(recordsData))
		if offset >= len(recordsData) {
			k.logger.Warnf("[conn:%d] Reached end of data while parsing record %d/%d", connID, i, recordBatch.NumRecords)
			break
		}

		// Parse the record - ReadFrom expects data that INCLUDES the length varint
		record := kmsg.NewRecord()
		k.logger.Debugf("[conn:%d] Parsing record %d from offset %d", connID, i, offset)

		recordErr := record.ReadFrom(recordsData[offset:])

		if recordErr != nil {
			k.logger.Warnf("[conn:%d] Failed to parse record %d: %v", connID, i, recordErr)
			// Cannot reliably skip this record since the length is unknown
			break
		}

		// Calculate how many bytes the record consumed: length varint + record data
		// The record.Length field contains the length of data AFTER the varint
		recordLen32, lenBytesConsumed := kbin.Varint(recordsData[offset:])
		if lenBytesConsumed == 0 {
			k.logger.Warnf("[conn:%d] Failed to read varint length for record %d after parsing", connID, i)
			break
		}
		totalBytesConsumed := lenBytesConsumed + int(recordLen32)
		k.logger.Debugf("[conn:%d] Record %d consumed %d bytes (varint: %d, data: %d)", connID, i, totalBytesConsumed, lenBytesConsumed, recordLen32)

		offset += totalBytesConsumed

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

func (k *kafkaServerInput) sendResponse(conn net.Conn, connID uint64, correlationID int32, msg kmsg.Response) error {
	buf := kbin.AppendInt32(nil, correlationID)

	// For flexible responses (EXCEPT ApiVersions key 18), add response header TAG_BUFFER
	// This matches the kfake implementation in franz-go
	if msg.IsFlexible() && msg.Key() != int16(kmsg.ApiVersions) {
		buf = append(buf, 0) // Empty TAG_BUFFER (0 tags)
	}

	// AppendTo generates the response body
	bufBeforeAppend := len(buf)
	buf = msg.AppendTo(buf)

	k.logger.Debugf("[conn:%d] Sending response: correlationID=%d, flexible=%v, key=%d, total_size=%d, body_size=%d", connID, correlationID, msg.IsFlexible(), msg.Key(), len(buf), len(buf)-bufBeforeAppend)

	return k.writeResponse(connID, conn, buf)
}

func (k *kafkaServerInput) writeResponse(connID uint64, conn net.Conn, data []byte) error {
	// Set write deadline to prevent hanging on slow clients
	if err := conn.SetWriteDeadline(time.Now().Add(k.timeout)); err != nil {
		k.logger.Errorf("[conn:%d] Failed to set write deadline: %v", connID, err)
		return err
	}

	// Write size
	size := int32(len(data))
	if err := binary.Write(conn, binary.BigEndian, size); err != nil {
		k.logger.Errorf("[conn:%d] Failed to write response size: %v", connID, err)
		return err
	}

	// Write data
	n, err := conn.Write(data)
	if err != nil {
		k.logger.Errorf("[conn:%d] Failed to write response data: %v", connID, err)
		return err
	}
	k.logger.Debugf("[conn:%d] Wrote response: %d bytes", connID, n)
	return nil
}

// getMsgChan safely retrieves the message channel
func (k *kafkaServerInput) getMsgChan() chan messageBatch {
	k.chanMu.RLock()
	defer k.chanMu.RUnlock()
	return k.msgChan
}

// setMsgChan safely sets the message channel
func (k *kafkaServerInput) setMsgChan(ch chan messageBatch) {
	k.chanMu.Lock()
	defer k.chanMu.Unlock()
	k.msgChan = ch
}

func (k *kafkaServerInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	msgChan := k.getMsgChan()
	if msgChan == nil {
		return nil, nil, service.ErrNotConnected
	}

	select {
	case mb, open := <-msgChan:
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
	// Protect against double close with atomic swap
	if k.shutdownDone.Swap(true) {
		return nil // Already closed
	}

	k.shutSig.TriggerSoftStop()

	// Use sync.Once to ensure channel is only closed once
	k.shutdownOnce.Do(func() {
		close(k.shutdownCh)
	})

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
	case <-time.After(shutdownGracePeriod):
		k.logger.Warn("Timeout waiting for connections to close")
	}

	k.shutSig.TriggerHasStopped()

	return nil
}
