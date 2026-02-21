package kafka

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/require"

	"github.com/warpstreamlabs/bento/public/service/integration"
)

func TestIntegrationKafkaServerPerfTest(t *testing.T) {
	integration.CheckSkip(t)
	t.Parallel()

	const numRecords = 10000
	const recordSizeBytes = 100

	port := getFreePort(t)
	hostAddr := fmt.Sprintf("%s:%d", getHostAddress(), port)

	config := fmt.Sprintf(`
input:
  kafka_server:
    address: "0.0.0.0:%d"
    advertised_address: "%s"
    timeout: "30s"
output:
  drop: {}
`, port, hostAddr)

	runKafkaServerTestWithCapture(t, port, config, func(ctx context.Context, capture *messageCapture) {
		pool := newDockerPool(t)

		runOpts := &dockertest.RunOptions{
			Repository: "apache/kafka",
			Tag:        "4.1.1",
			Cmd:        []string{"sleep", "infinity"},
		}

		if runtime.GOOS == "linux" {
			runOpts.ExtraHosts = []string{"host.docker.internal:host-gateway"}
		}

		resource, err := pool.RunWithOptions(runOpts, func(config *docker.HostConfig) {
			config.AutoRemove = true
			config.RestartPolicy = docker.RestartPolicy{Name: "no"}
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = pool.Purge(resource) })
		_ = resource.Expire(300)

		// Wait for the container to be ready.
		err = pool.Retry(func() error {
			_, execErr := resource.Exec([]string{"echo", "ready"}, dockertest.ExecOptions{})
			return execErr
		})
		require.NoError(t, err)

		// Run kafka-producer-perf-test.sh against the bento kafka_server.
		cmd := []string{
			"/opt/kafka/bin/kafka-producer-perf-test.sh",
			"--topic", "perf-test-topic",
			"--num-records", fmt.Sprintf("%d", numRecords),
			"--record-size", fmt.Sprintf("%d", recordSizeBytes),
			"--throughput", "-1",
			"--producer-props",
			fmt.Sprintf("bootstrap.servers=%s", hostAddr),
			"acks=1",
		}

		var stdout, stderr bytes.Buffer
		t.Logf("Running kafka-producer-perf-test.sh: %v", cmd)
		exitCode, err := resource.Exec(cmd, dockertest.ExecOptions{
			StdOut: &stdout,
			StdErr: &stderr,
		})
		t.Logf("kafka-producer-perf-test.sh stdout:\n%s", stdout.String())
		if stderr.Len() > 0 {
			t.Logf("kafka-producer-perf-test.sh stderr:\n%s", stderr.String())
		}
		require.NoError(t, err, "kafka-producer-perf-test.sh exec failed")
		require.Equal(t, 0, exitCode, "kafka-producer-perf-test.sh exited with non-zero code")

		// Wait for all messages to arrive through bento.
		msgs := capture.waitForMessages(numRecords, 60*time.Second)
		require.Len(t, msgs, numRecords, "expected all %d records from kafka-producer-perf-test.sh to arrive through bento", numRecords)

		for _, msg := range msgs {
			require.Equal(t, "perf-test-topic", msg.Topic)
			require.Len(t, msg.Value, recordSizeBytes, "each record should be exactly %d bytes", recordSizeBytes)
		}

		t.Logf("Successfully received all %d messages from kafka-producer-perf-test.sh through bento", numRecords)
	})
}
