package kafka

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service"
	"github.com/warpstreamlabs/bento/public/service/integration"

	// Import all standard Bento components
	_ "github.com/warpstreamlabs/bento/public/components/pure"
)

// getHostAddress returns the address to use from inside a Docker container to reach the host.
// On Linux, we use host.docker.internal which requires --add-host flag.
// On macOS/Windows, host.docker.internal is available by default.
func getHostAddress() string {
	return "host.docker.internal"
}

func TestIntegrationKafkaServer(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	// On Windows, Docker Desktop uses named pipes by default
	// dockertest.NewPool("") should auto-detect this via DOCKER_HOST env var
	// or fall back to the default Docker socket
	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to Docker: %v", err)
	}

	// Verify we can actually connect to Docker
	if err := pool.Client.Ping(); err != nil {
		t.Skipf("Could not ping Docker: %v", err)
	}
	pool.MaxWait = time.Minute

	t.Run("basic_no_auth", func(t *testing.T) {
		t.Parallel()
		testKafkaServerBasicNoAuth(t, pool)
	})

	t.Run("multiple_messages", func(t *testing.T) {
		t.Parallel()
		testKafkaServerMultipleMessages(t, pool)
	})

	t.Run("sasl_plain", func(t *testing.T) {
		t.Parallel()
		testKafkaServerSASLPlain(t, pool)
	})

	t.Run("sasl_plain_wrong_password", func(t *testing.T) {
		t.Parallel()
		testKafkaServerSASLPlainWrongPassword(t, pool)
	})

	t.Run("sasl_scram_sha256", func(t *testing.T) {
		t.Parallel()
		testKafkaServerSASLScram256(t, pool)
	})

	t.Run("sasl_scram_sha512", func(t *testing.T) {
		t.Parallel()
		testKafkaServerSASLScram512(t, pool)
	})

	t.Run("sasl_scram_wrong_password", func(t *testing.T) {
		t.Parallel()
		testKafkaServerSASLScramWrongPassword(t, pool)
	})

	t.Run("message_with_key", func(t *testing.T) {
		t.Parallel()
		testKafkaServerMessageWithKey(t, pool)
	})

	t.Run("idempotent_producer", func(t *testing.T) {
		t.Parallel()
		testKafkaServerIdempotentProducer(t, pool)
	})
}

func getFreePort(t *testing.T) int {
	port, err := integration.GetFreePort()
	require.NoError(t, err)
	return port
}

// waitForTCPReady polls until the given TCP address is accepting connections.
func waitForTCPReady(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s to accept connections", addr)
}

// kafkaDockerClient wraps a Docker container running Kafka tools
type kafkaDockerClient struct {
	pool     *dockertest.Pool
	resource *dockertest.Resource
	t        *testing.T
}

// newKafkaDockerClient creates a new Docker container with Kafka CLI tools
func newKafkaDockerClient(t *testing.T, pool *dockertest.Pool) *kafkaDockerClient {
	t.Helper()

	// Use apache/kafka image which includes CLI tools
	runOpts := &dockertest.RunOptions{
		Repository: "apache/kafka",
		Tag:        "4.1.1",
		Cmd:        []string{"sleep", "infinity"}, // Keep container running
	}

	// On Linux, we need to add host.docker.internal mapping
	if runtime.GOOS == "linux" {
		runOpts.ExtraHosts = []string{"host.docker.internal:host-gateway"}
	}

	resource, err := pool.RunWithOptions(runOpts, func(config *docker.HostConfig) {
		config.AutoRemove = true
		config.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		t.Skipf("Could not start Kafka Docker container: %v", err)
	}

	// Set expiration
	_ = resource.Expire(300)

	client := &kafkaDockerClient{
		pool:     pool,
		resource: resource,
		t:        t,
	}

	// Wait for container to be ready by executing a command inside it
	err = pool.Retry(func() error {
		_, _, execErr := client.execInContainer([]string{"echo", "ready"})
		return execErr
	})
	if err != nil {
		t.Skipf("Docker container did not become ready: %v", err)
	}

	return client
}

// Close cleans up the Docker container
func (c *kafkaDockerClient) Close() {
	if c.resource != nil {
		_ = c.pool.Purge(c.resource)
	}
}

// execInContainer runs a command inside the Kafka container
func (c *kafkaDockerClient) execInContainer(cmd []string) (string, string, error) {
	var stdout, stderr bytes.Buffer

	exitCode, err := c.resource.Exec(cmd, dockertest.ExecOptions{
		StdOut: &stdout,
		StdErr: &stderr,
	})
	if err != nil {
		return stdout.String(), stderr.String(), err
	}
	if exitCode != 0 {
		return stdout.String(), stderr.String(), fmt.Errorf("command exited with code %d: %s", exitCode, stderr.String())
	}
	return stdout.String(), stderr.String(), nil
}

// produceMessage sends a message using kafka-console-producer.sh
func (c *kafkaDockerClient) produceMessage(brokerAddr, topic, value string) error {
	// Use echo to pipe message to kafka-console-producer.sh
	// Disable idempotence since our kafka_server doesn't support INIT_PRODUCER_ID
	// Add timeout to prevent hanging
	// Use base64 encoding to handle special characters in the message
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))
	cmd := []string{
		"bash", "-c",
		fmt.Sprintf(`cat > /tmp/producer.properties << 'PROPEOF'
enable.idempotence=false
request.timeout.ms=5000
delivery.timeout.ms=10000
acks=1
retries=0
PROPEOF
timeout 30 bash -c "echo %s | base64 -d | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server %s --topic %s --producer.config /tmp/producer.properties" 2>&1`,
			encodedValue, brokerAddr, topic),
	}

	c.t.Logf("Producing message to %s topic=%s", brokerAddr, topic)
	stdout, stderr, err := c.execInContainer(cmd)
	if err != nil {
		c.t.Logf("Producer stdout: %s", stdout)
		c.t.Logf("Producer stderr: %s", stderr)
		return err
	}
	c.t.Logf("Producer completed successfully, stdout: %s", stdout)
	return nil
}

// produceMessageWithKey sends a message with a key using kafka-console-producer.sh
func (c *kafkaDockerClient) produceMessageWithKey(brokerAddr, topic, key, value string) error {
	// Use base64 encoding to handle special characters
	encodedKey := base64.StdEncoding.EncodeToString([]byte(key))
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))

	cmd := []string{
		"bash", "-c",
		fmt.Sprintf(`cat > /tmp/producer.properties << 'PROPEOF'
enable.idempotence=false
request.timeout.ms=5000
delivery.timeout.ms=10000
acks=1
retries=0
PROPEOF
timeout 30 bash -c "printf '%%s\t%%s\n' \"\$(echo %s | base64 -d)\" \"\$(echo %s | base64 -d)\" | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server %s --topic %s --property parse.key=true --property 'key.separator=	' --producer.config /tmp/producer.properties" 2>&1`,
			encodedKey, encodedValue, brokerAddr, topic),
	}

	c.t.Logf("Producing message with key to %s topic=%s", brokerAddr, topic)
	stdout, stderr, err := c.execInContainer(cmd)
	if err != nil {
		c.t.Logf("Producer stdout: %s", stdout)
		c.t.Logf("Producer stderr: %s", stderr)
		return err
	}
	c.t.Logf("Producer completed successfully, stdout: %s", stdout)
	return nil
}

// produceMessageIdempotent sends a message using an idempotent producer (enable.idempotence=true)
func (c *kafkaDockerClient) produceMessageIdempotent(brokerAddr, topic, value string) error {
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))
	cmd := []string{
		"bash", "-c",
		fmt.Sprintf(`cat > /tmp/producer.properties << 'PROPEOF'
enable.idempotence=true
request.timeout.ms=5000
delivery.timeout.ms=10000
acks=all
PROPEOF
timeout 30 bash -c "echo %s | base64 -d | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server %s --topic %s --producer.config /tmp/producer.properties" 2>&1`,
			encodedValue, brokerAddr, topic),
	}

	c.t.Logf("Producing message with idempotent producer to %s topic=%s", brokerAddr, topic)
	stdout, stderr, err := c.execInContainer(cmd)
	if err != nil {
		c.t.Logf("Idempotent producer stdout: %s", stdout)
		c.t.Logf("Idempotent producer stderr: %s", stderr)
		return err
	}
	c.t.Logf("Idempotent producer completed successfully, stdout: %s", stdout)
	return nil
}

// produceMultipleMessages sends multiple messages using kafka-console-producer.sh
func (c *kafkaDockerClient) produceMultipleMessages(brokerAddr, topic string, messages []string) error {
	// Join messages with newlines and base64 encode
	input := strings.Join(messages, "\n")
	encodedInput := base64.StdEncoding.EncodeToString([]byte(input))

	cmd := []string{
		"bash", "-c",
		fmt.Sprintf(`cat > /tmp/producer.properties << 'PROPEOF'
enable.idempotence=false
request.timeout.ms=5000
delivery.timeout.ms=10000
acks=1
retries=0
PROPEOF
timeout 30 bash -c "echo %s | base64 -d | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server %s --topic %s --producer.config /tmp/producer.properties" 2>&1`,
			encodedInput, brokerAddr, topic),
	}

	c.t.Logf("Producing %d messages to %s topic=%s", len(messages), brokerAddr, topic)
	stdout, stderr, err := c.execInContainer(cmd)
	if err != nil {
		c.t.Logf("Producer stdout: %s", stdout)
		c.t.Logf("Producer stderr: %s", stderr)
		return err
	}
	c.t.Logf("Producer completed successfully, stdout: %s", stdout)
	return nil
}

// produceWithSASLPlain sends a message using SASL PLAIN authentication
func (c *kafkaDockerClient) produceWithSASLPlain(brokerAddr, topic, value, username, password string) error {
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))

	// Create a properties file inside the container to avoid quoting issues
	cmd := []string{
		"bash", "-c",
		fmt.Sprintf(`cat > /tmp/plain.properties << 'PROPEOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";
enable.idempotence=false
request.timeout.ms=5000
delivery.timeout.ms=10000
acks=1
retries=0
PROPEOF
timeout 30 bash -c "echo %s | base64 -d | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server %s --topic %s --producer.config /tmp/plain.properties" 2>&1`,
			username, password, encodedValue, brokerAddr, topic),
	}

	c.t.Logf("Producing SASL PLAIN message to %s topic=%s user=%s", brokerAddr, topic, username)
	stdout, stderr, err := c.execInContainer(cmd)
	if err != nil {
		c.t.Logf("Producer stdout: %s", stdout)
		c.t.Logf("Producer stderr: %s", stderr)
		return err
	}
	c.t.Logf("Producer completed successfully, stdout: %s", stdout)
	return nil
}

// produceWithSASLPlainExpectFailure sends a message using SASL PLAIN authentication and expects failure
func (c *kafkaDockerClient) produceWithSASLPlainExpectFailure(brokerAddr, topic, value, username, password string) error {
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))

	// Create a properties file inside the container to avoid quoting issues
	cmd := []string{
		"bash", "-c",
		fmt.Sprintf(`cat > /tmp/plain.properties << 'PROPEOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s";
enable.idempotence=false
request.timeout.ms=5000
delivery.timeout.ms=8000
acks=1
retries=0
PROPEOF
timeout 10 bash -c "echo %s | base64 -d | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server %s --topic %s --producer.config /tmp/plain.properties 2>&1" || echo "TIMEOUT_OR_ERROR"`,
			username, password, encodedValue, brokerAddr, topic),
	}

	c.t.Logf("Producing SASL PLAIN message (expect failure) to %s topic=%s user=%s", brokerAddr, topic, username)
	stdout, _, err := c.execInContainer(cmd)
	c.t.Logf("Producer output: %s", stdout)
	if err != nil {
		// Container command failed - this is expected for auth failure
		return err
	}
	// Check if output contains authentication error indicators
	if strings.Contains(stdout, "SaslAuthenticationException") ||
		strings.Contains(stdout, "Authentication failed") ||
		strings.Contains(stdout, "TIMEOUT_OR_ERROR") {
		return fmt.Errorf("authentication failed: %s", stdout)
	}
	return nil
}

// produceWithSASLScram sends a message using SASL SCRAM authentication
func (c *kafkaDockerClient) produceWithSASLScram(brokerAddr, topic, value, username, password, mechanism string) error {
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))

	// Create a properties file inside the container to avoid quoting issues
	cmd := []string{
		"bash", "-c",
		fmt.Sprintf(`cat > /tmp/scram.properties << 'PROPEOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=%s
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";
enable.idempotence=false
request.timeout.ms=5000
delivery.timeout.ms=10000
acks=1
retries=0
PROPEOF
timeout 30 bash -c "echo %s | base64 -d | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server %s --topic %s --producer.config /tmp/scram.properties" 2>&1`,
			mechanism, username, password, encodedValue, brokerAddr, topic),
	}

	c.t.Logf("Producing SASL SCRAM message to %s topic=%s user=%s mechanism=%s", brokerAddr, topic, username, mechanism)
	stdout, stderr, err := c.execInContainer(cmd)
	if err != nil {
		c.t.Logf("Producer stdout: %s", stdout)
		c.t.Logf("Producer stderr: %s", stderr)
		return err
	}
	c.t.Logf("Producer completed successfully, stdout: %s", stdout)
	return nil
}

// produceWithSASLScramExpectFailure sends a message using SASL SCRAM authentication and expects failure
func (c *kafkaDockerClient) produceWithSASLScramExpectFailure(brokerAddr, topic, value, username, password, mechanism string) error {
	encodedValue := base64.StdEncoding.EncodeToString([]byte(value))

	// Create a properties file inside the container to avoid quoting issues
	cmd := []string{
		"bash", "-c",
		fmt.Sprintf(`cat > /tmp/scram.properties << 'PROPEOF'
security.protocol=SASL_PLAINTEXT
sasl.mechanism=%s
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";
enable.idempotence=false
request.timeout.ms=5000
delivery.timeout.ms=8000
acks=1
retries=0
PROPEOF
timeout 10 bash -c "echo %s | base64 -d | /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server %s --topic %s --producer.config /tmp/scram.properties 2>&1" || echo "TIMEOUT_OR_ERROR"`,
			mechanism, username, password, encodedValue, brokerAddr, topic),
	}

	c.t.Logf("Producing SASL SCRAM message (expect failure) to %s topic=%s user=%s mechanism=%s", brokerAddr, topic, username, mechanism)
	stdout, _, err := c.execInContainer(cmd)
	c.t.Logf("Producer output: %s", stdout)
	if err != nil {
		return err
	}
	// Check if output contains authentication error indicators
	if strings.Contains(stdout, "SaslAuthenticationException") ||
		strings.Contains(stdout, "Authentication failed") ||
		strings.Contains(stdout, "TIMEOUT_OR_ERROR") {
		return fmt.Errorf("authentication failed: %s", stdout)
	}
	return nil
}

// receivedMessage holds a captured message from kafka_server
type receivedMessage struct {
	Topic   string
	Key     string
	Value   string
	Headers map[string]string
}

// messageCapture captures messages received by kafka_server
type messageCapture struct {
	messages []receivedMessage
	mu       sync.Mutex
}

func (mc *messageCapture) add(msg receivedMessage) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.messages = append(mc.messages, msg)
}

func (mc *messageCapture) get() []receivedMessage {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	result := make([]receivedMessage, len(mc.messages))
	copy(result, mc.messages)
	return result
}

func (mc *messageCapture) waitForMessages(count int, timeout time.Duration) []receivedMessage {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msgs := mc.get()
		if len(msgs) >= count {
			return msgs
		}
		time.Sleep(50 * time.Millisecond)
	}
	return mc.get()
}

// runKafkaServerTestWithCapture runs a kafka_server and captures received messages.
// port is the local port the kafka_server listens on, used to wait for readiness.
func runKafkaServerTestWithCapture(t *testing.T, port int, configYAML string, testFn func(ctx context.Context, capture *messageCapture)) {
	t.Helper()

	capture := &messageCapture{}

	builder := service.NewStreamBuilder()
	err := builder.SetYAML(configYAML)
	require.NoError(t, err)

	// Add a consumer function to capture messages
	require.NoError(t, builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
		value, _ := msg.AsBytes()
		key, _ := msg.MetaGet("kafka_server_key")
		topic, _ := msg.MetaGet("kafka_server_topic")

		headers := make(map[string]string)
		_ = msg.MetaWalk(func(k, v string) error {
			headers[k] = v
			return nil
		})

		capture.add(receivedMessage{
			Topic:   topic,
			Key:     key,
			Value:   string(value),
			Headers: headers,
		})
		return nil
	}))

	stream, err := builder.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = stream.Run(ctx)
	}()

	// Wait for the server to start accepting connections
	waitForTCPReady(t, fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)

	// Run the test
	testFn(ctx, capture)

	// Stop the stream
	cancel()
	wg.Wait()
}

// runKafkaServerTest runs a kafka_server and executes a test function (legacy without capture)
func runKafkaServerTest(t *testing.T, port int, configYAML string, testFn func(ctx context.Context)) {
	runKafkaServerTestWithCapture(t, port, configYAML, func(ctx context.Context, _ *messageCapture) {
		testFn(ctx)
	})
}

func testKafkaServerBasicNoAuth(t *testing.T, pool *dockertest.Pool) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	capture := &messageCapture{}
	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
output:
  drop: {}
`, port, hostAddr)

	client := newKafkaDockerClient(t, pool)
	defer client.Close()

	builder := service.NewStreamBuilder()
	err := builder.SetYAML(config)
	require.NoError(t, err)

	// Add a consumer function to capture messages (in addition to drop)
	require.NoError(t, builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
		value, _ := msg.AsBytes()
		key, _ := msg.MetaGet("kafka_server_key")
		topic, _ := msg.MetaGet("kafka_server_topic")

		t.Logf("Consumer received message: topic=%s, key=%s, value=%s", topic, key, string(value))

		capture.add(receivedMessage{
			Topic: topic,
			Key:   key,
			Value: string(value),
		})
		return nil
	}))

	stream, err := builder.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = stream.Run(ctx)
	}()

	// Wait for the server to start accepting connections
	waitForTCPReady(t, fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)

	// Produce a message
	err = client.produceMessage(hostAddr, "test-topic", `{"message": "hello from docker"}`)
	require.NoError(t, err)

	// Verify the message was received
	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1, "Expected exactly 1 message")
	assert.Equal(t, "test-topic", msgs[0].Topic)
	assert.Equal(t, `{"message": "hello from docker"}`, msgs[0].Value)
	t.Logf("Basic no-auth message received: topic=%s, value=%s", msgs[0].Topic, msgs[0].Value)

	// Stop the stream
	cancel()
	wg.Wait()
}

func testKafkaServerMultipleMessages(t *testing.T, pool *dockertest.Pool) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
`, port, hostAddr)

	client := newKafkaDockerClient(t, pool)
	defer client.Close()

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		messages := make([]string, 10)
		for i := 0; i < 10; i++ {
			messages[i] = fmt.Sprintf(`{"index": %d}`, i)
		}

		err := client.produceMultipleMessages(hostAddr, "multi-topic", messages)
		require.NoError(t, err)

		// Verify all messages were received
		msgs := capture.waitForMessages(10, 10*time.Second)
		require.Len(t, msgs, 10, "Expected exactly 10 messages")
		for i, msg := range msgs {
			assert.Equal(t, "multi-topic", msg.Topic)
			assert.Equal(t, fmt.Sprintf(`{"index": %d}`, i), msg.Value)
		}
		t.Logf("Multiple messages received: count=%d", len(msgs))
	})
}

func testKafkaServerSASLPlain(t *testing.T, pool *dockertest.Pool) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
    sasl:
      - mechanism: PLAIN
        username: "testuser"
        password: "testpass"
`, port, hostAddr)

	client := newKafkaDockerClient(t, pool)
	defer client.Close()

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLPlain(hostAddr, "auth-topic", `{"auth": "PLAIN"}`, "testuser", "testpass")
		require.NoError(t, err)

		// Verify the message was received
		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1, "Expected exactly 1 message")
		assert.Equal(t, "auth-topic", msgs[0].Topic)
		assert.Equal(t, `{"auth": "PLAIN"}`, msgs[0].Value)
		t.Logf("SASL PLAIN message received: topic=%s, value=%s", msgs[0].Topic, msgs[0].Value)
	})
}

func testKafkaServerSASLPlainWrongPassword(t *testing.T, pool *dockertest.Pool) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
    sasl:
      - mechanism: PLAIN
        username: "testuser"
        password: "testpass"
`, port, hostAddr)

	client := newKafkaDockerClient(t, pool)
	defer client.Close()

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLPlainExpectFailure(hostAddr, "should-fail-topic", `{"should": "fail"}`, "testuser", "wrongpassword")
		assert.Error(t, err)
		t.Logf("SASL PLAIN wrong password correctly rejected: %v", err)

		// Verify no messages were received (auth failed)
		msgs := capture.waitForMessages(1, 2*time.Second)
		assert.Len(t, msgs, 0, "Expected no messages due to auth failure")
	})
}

func testKafkaServerSASLScram256(t *testing.T, pool *dockertest.Pool) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
    sasl:
      - mechanism: SCRAM-SHA-256
        username: "scramuser"
        password: "scrampass"
`, port, hostAddr)

	client := newKafkaDockerClient(t, pool)
	defer client.Close()

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLScram(hostAddr, "scram-topic", `{"auth": "SCRAM-SHA-256"}`, "scramuser", "scrampass", "SCRAM-SHA-256")
		require.NoError(t, err)

		// Verify the message was received
		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1, "Expected exactly 1 message")
		assert.Equal(t, "scram-topic", msgs[0].Topic)
		assert.Equal(t, `{"auth": "SCRAM-SHA-256"}`, msgs[0].Value)
		t.Logf("SASL SCRAM-SHA-256 message received: topic=%s, value=%s", msgs[0].Topic, msgs[0].Value)
	})
}

func testKafkaServerSASLScram512(t *testing.T, pool *dockertest.Pool) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
    sasl:
      - mechanism: SCRAM-SHA-512
        username: "scramuser"
        password: "scrampass"
`, port, hostAddr)

	client := newKafkaDockerClient(t, pool)
	defer client.Close()

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLScram(hostAddr, "scram512-topic", `{"auth": "SCRAM-SHA-512"}`, "scramuser", "scrampass", "SCRAM-SHA-512")
		require.NoError(t, err)

		// Verify the message was received
		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1, "Expected exactly 1 message")
		assert.Equal(t, "scram512-topic", msgs[0].Topic)
		assert.Equal(t, `{"auth": "SCRAM-SHA-512"}`, msgs[0].Value)
		t.Logf("SASL SCRAM-SHA-512 message received: topic=%s, value=%s", msgs[0].Topic, msgs[0].Value)
	})
}

func testKafkaServerSASLScramWrongPassword(t *testing.T, pool *dockertest.Pool) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
    sasl:
      - mechanism: SCRAM-SHA-256
        username: "scramuser"
        password: "scrampass"
`, port, hostAddr)

	client := newKafkaDockerClient(t, pool)
	defer client.Close()

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		err := client.produceWithSASLScramExpectFailure(hostAddr, "should-fail-topic", `{"should": "fail"}`, "scramuser", "wrongpassword", "SCRAM-SHA-256")
		assert.Error(t, err)
		t.Logf("SASL SCRAM wrong password correctly rejected: %v", err)

		// Verify no messages were received (auth failed)
		msgs := capture.waitForMessages(1, 2*time.Second)
		assert.Len(t, msgs, 0, "Expected no messages due to auth failure")
	})
}

func testKafkaServerMessageWithKey(t *testing.T, pool *dockertest.Pool) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
`, port, hostAddr)

	client := newKafkaDockerClient(t, pool)
	defer client.Close()

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		err := client.produceMessageWithKey(hostAddr, "key-topic", "my-key", `{"test": "with_key"}`)
		require.NoError(t, err)

		// Verify the message with key was received
		msgs := capture.waitForMessages(1, 5*time.Second)
		require.Len(t, msgs, 1, "Expected exactly 1 message")
		assert.Equal(t, "key-topic", msgs[0].Topic)
		assert.Equal(t, "my-key", msgs[0].Key)
		assert.Equal(t, `{"test": "with_key"}`, msgs[0].Value)
		t.Logf("Message with key received: topic=%s, key=%s, value=%s", msgs[0].Topic, msgs[0].Key, msgs[0].Value)
	})
}

// TestIntegrationKafkaServerMultipleUsers tests multiple SASL users
func TestIntegrationKafkaServerMultipleUsers(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	pool, err := dockertest.NewPool("")
	if err != nil {
		t.Skipf("Could not connect to Docker: %v", err)
	}
	if err := pool.Client.Ping(); err != nil {
		t.Skipf("Could not ping Docker: %v", err)
	}
	pool.MaxWait = time.Minute

	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
    sasl:
      - mechanism: PLAIN
        username: "user1"
        password: "pass1"
      - mechanism: PLAIN
        username: "user2"
        password: "pass2"
`, port, hostAddr)

	client := newKafkaDockerClient(t, pool)
	defer client.Close()

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		// Test user1
		err := client.produceWithSASLPlain(hostAddr, "user1-topic", `{"user": "user1"}`, "user1", "pass1")
		require.NoError(t, err)
		t.Log("User1 authenticated and sent message successfully")

		// Test user2
		err = client.produceWithSASLPlain(hostAddr, "user2-topic", `{"user": "user2"}`, "user2", "pass2")
		require.NoError(t, err)
		t.Log("User2 authenticated and sent message successfully")

		// Verify both messages were received
		msgs := capture.waitForMessages(2, 10*time.Second)
		require.Len(t, msgs, 2, "Expected exactly 2 messages")

		// Find each message
		user1Msg := msgs[0]
		user2Msg := msgs[1]
		if user1Msg.Topic != "user1-topic" {
			user1Msg, user2Msg = user2Msg, user1Msg
		}

		assert.Equal(t, "user1-topic", user1Msg.Topic)
		assert.Equal(t, `{"user": "user1"}`, user1Msg.Value)
		assert.Equal(t, "user2-topic", user2Msg.Topic)
		assert.Equal(t, `{"user": "user2"}`, user2Msg.Value)
		t.Logf("Both users' messages received successfully")
	})
}
func testKafkaServerIdempotentProducer(t *testing.T, pool *dockertest.Pool) {
	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	capture := &messageCapture{}
	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "10s"
    idempotent_write: true
output:
  drop: {}
`, port, hostAddr)

	client := newKafkaDockerClient(t, pool)
	defer client.Close()

	builder := service.NewStreamBuilder()
	err := builder.SetYAML(config)
	require.NoError(t, err)

	// Add a consumer function to capture messages
	require.NoError(t, builder.AddConsumerFunc(func(ctx context.Context, msg *service.Message) error {
		value, _ := msg.AsBytes()
		key, _ := msg.MetaGet("kafka_server_key")
		topic, _ := msg.MetaGet("kafka_server_topic")

		t.Logf("Consumer received message: topic=%s, key=%s, value=%s", topic, key, string(value))

		capture.add(receivedMessage{
			Topic: topic,
			Key:   key,
			Value: string(value),
		})
		return nil
	}))

	stream, err := builder.Build()
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = stream.Run(ctx)
	}()

	// Wait for the server to start accepting connections
	waitForTCPReady(t, fmt.Sprintf("127.0.0.1:%d", port), 5*time.Second)

	// Produce a message using idempotent producer
	err = client.produceMessageIdempotent(hostAddr, "idempotent-topic", `{"message": "hello from idempotent producer"}`)
	require.NoError(t, err)

	// Verify the message was received
	msgs := capture.waitForMessages(1, 5*time.Second)
	require.Len(t, msgs, 1, "Expected exactly 1 message")
	assert.Equal(t, "idempotent-topic", msgs[0].Topic)
	assert.Equal(t, `{"message": "hello from idempotent producer"}`, msgs[0].Value)
	t.Logf("Idempotent producer message received: topic=%s, value=%s", msgs[0].Topic, msgs[0].Value)

	// Stop the stream
	cancel()
	wg.Wait()
}
