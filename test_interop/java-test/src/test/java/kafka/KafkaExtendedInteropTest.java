package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Extended interoperability tests for Bento kafka_server input.
 *
 * These tests cover additional scenarios beyond basic producer tests:
 * - Multiple topics
 * - SASL SCRAM-SHA-512 authentication
 * - Batch message production
 * - Async message production
 * - Edge cases and error handling
 *
 * Required Bento servers:
 * - Port 19200: No authentication
 * - Port 19203: SASL SCRAM-SHA-512 (scram512user/scram512pass)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaExtendedInteropTest {

    private static final int TIMEOUT_SECONDS = 10;

    // ==================== Multiple Topics Tests ====================

    @Test
    @Order(1)
    @DisplayName("Multiple Topics: Sequential send to different topics")
    void testMultipleTopicsSequential() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19200");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String[] topics = {"topic-alpha", "topic-beta", "topic-gamma", "topic-delta"};
            List<RecordMetadata> results = new ArrayList<>();

            for (int i = 0; i < topics.length; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    topics[i],
                    "multi-topic-key-" + i,
                    "{\"test\": \"multi_topic\", \"topic\": \"" + topics[i] + "\", \"index\": " + i + "}"
                );
                results.add(producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }

            assertEquals(topics.length, results.size());
            for (int i = 0; i < topics.length; i++) {
                assertEquals(topics[i], results.get(i).topic());
            }
            System.out.println("Multiple Topics: " + results.size() + " topics tested successfully");
        }
    }

    @Test
    @Order(2)
    @DisplayName("Multiple Topics: Round-robin messages across topics")
    void testMultipleTopicsRoundRobin() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19200");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String[] topics = {"round-robin-a", "round-robin-b", "round-robin-c"};
            int messagesPerTopic = 3;
            List<RecordMetadata> results = new ArrayList<>();

            for (int round = 0; round < messagesPerTopic; round++) {
                for (String topic : topics) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                        topic,
                        "rr-key-" + round,
                        "{\"round\": " + round + ", \"topic\": \"" + topic + "\"}"
                    );
                    results.add(producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
                }
            }

            assertEquals(topics.length * messagesPerTopic, results.size());
            System.out.println("Multiple Topics: Round-robin " + results.size() + " messages sent successfully");
        }
    }

    // ==================== SASL SCRAM-SHA-512 Authentication Tests ====================

    @Test
    @Order(10)
    @DisplayName("SASL SCRAM-SHA-512: Basic authenticated message")
    void testScram512BasicAuth() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19203");
        configureSaslScram512(props, "scram512user", "scram512pass");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "scram512-topic",
                "scram512-key",
                "{\"test\": \"scram512_message\", \"mechanism\": \"SCRAM-SHA-512\"}"
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            assertEquals("scram512-topic", metadata.topic());
            System.out.println("SASL SCRAM-SHA-512: Authenticated message sent successfully");
        }
    }

    @Test
    @Order(11)
    @DisplayName("SASL SCRAM-SHA-512: Multiple authenticated messages")
    void testScram512MultipleMessages() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19203");
        configureSaslScram512(props, "scram512user", "scram512pass");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<RecordMetadata> results = new ArrayList<>();

            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "scram512-multi-topic",
                    "scram512-key-" + i,
                    "{\"index\": " + i + ", \"auth\": \"SCRAM-SHA-512\"}"
                );
                results.add(producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }

            assertEquals(5, results.size());
            System.out.println("SASL SCRAM-SHA-512: " + results.size() + " authenticated messages sent successfully");
        }
    }

    @Test
    @Order(12)
    @DisplayName("SASL SCRAM-SHA-512: Message with headers")
    void testScram512WithHeaders() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19203");
        configureSaslScram512(props, "scram512user", "scram512pass");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<Header> headers = Arrays.asList(
                new RecordHeader("auth-mechanism", "SCRAM-SHA-512".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("client-type", "java-kafka-client".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("test-id", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))
            );

            ProducerRecord<String, String> record = new ProducerRecord<>(
                "scram512-headers-topic",
                null,
                "scram512-header-key",
                "{\"test\": \"scram512_with_headers\"}",
                headers
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("SASL SCRAM-SHA-512: Message with headers sent successfully");
        }
    }

    @Test
    @Order(13)
    @DisplayName("SASL SCRAM-SHA-512: Wrong password should fail")
    void testScram512WrongPassword() {
        Properties props = createBaseProperties("127.0.0.1:19203");
        configureSaslScram512(props, "scram512user", "wrongpassword");

        assertThrows(Exception.class, () -> {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "should-fail-topic",
                    "fail-key",
                    "should not be sent"
                );
                producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        });
        System.out.println("SASL SCRAM-SHA-512: Wrong password correctly rejected");
    }

    @Test
    @Order(14)
    @DisplayName("SASL SCRAM-SHA-512: Wrong username should fail")
    void testScram512WrongUsername() {
        Properties props = createBaseProperties("127.0.0.1:19203");
        configureSaslScram512(props, "wronguser", "scram512pass");

        assertThrows(Exception.class, () -> {
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "should-fail-topic",
                    "fail-key",
                    "should not be sent"
                );
                producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
        });
        System.out.println("SASL SCRAM-SHA-512: Wrong username correctly rejected");
    }

    // ==================== Batch/Async Message Tests ====================

    @Test
    @Order(20)
    @DisplayName("Batch: High-throughput message batch")
    void testHighThroughputBatch() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19200");
        // Configure for batching
        props.put(ProducerConfig.LINGER_MS_CONFIG, 50);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 32768);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<Future<RecordMetadata>> futures = new ArrayList<>();
            int messageCount = 100;

            // Send messages asynchronously
            for (int i = 0; i < messageCount; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "batch-topic",
                    "batch-key-" + i,
                    "{\"batch\": true, \"index\": " + i + ", \"timestamp\": " + System.currentTimeMillis() + "}"
                );
                futures.add(producer.send(record));
            }

            // Wait for all to complete
            int successCount = 0;
            for (Future<RecordMetadata> future : futures) {
                RecordMetadata metadata = future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                if (metadata != null) {
                    successCount++;
                }
            }

            assertEquals(messageCount, successCount);
            System.out.println("Batch: " + successCount + " messages sent in batch mode");
        }
    }

    @Test
    @Order(21)
    @DisplayName("Async: Concurrent sends from multiple threads")
    void testConcurrentSends() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19200");
        int threadCount = 5;
        int messagesPerThread = 10;
        CountDownLatch latch = new CountDownLatch(threadCount);
        List<Exception> errors = Collections.synchronizedList(new ArrayList<>());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < messagesPerThread; i++) {
                            ProducerRecord<String, String> record = new ProducerRecord<>(
                                "concurrent-topic",
                                "thread-" + threadId + "-key-" + i,
                                "{\"thread\": " + threadId + ", \"message\": " + i + "}"
                            );
                            producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                        }
                    } catch (Exception e) {
                        errors.add(e);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(60, TimeUnit.SECONDS), "Threads did not complete in time");
            executor.shutdown();

            assertTrue(errors.isEmpty(), "Concurrent sends had errors: " + errors);
            System.out.println("Async: " + (threadCount * messagesPerThread) + " concurrent messages sent successfully");
        }
    }

    @Test
    @Order(22)
    @DisplayName("Async: Fire-and-forget with callback")
    void testFireAndForgetWithCallback() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19200");
        CountDownLatch latch = new CountDownLatch(10);
        List<RecordMetadata> successes = Collections.synchronizedList(new ArrayList<>());
        List<Exception> failures = Collections.synchronizedList(new ArrayList<>());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "callback-topic",
                    "callback-key-" + i,
                    "{\"callback\": true, \"index\": " + i + "}"
                );

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        failures.add(exception);
                    } else {
                        successes.add(metadata);
                    }
                    latch.countDown();
                });
            }

            assertTrue(latch.await(30, TimeUnit.SECONDS), "Callbacks did not complete in time");
            assertTrue(failures.isEmpty(), "Fire-and-forget had failures: " + failures);
            assertEquals(10, successes.size());
            System.out.println("Async: Fire-and-forget with callbacks completed successfully");
        }
    }

    // ==================== Edge Cases Tests ====================

    @Test
    @Order(30)
    @DisplayName("Edge Case: Empty value message")
    void testEmptyValue() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19200");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "empty-value-topic",
                "empty-value-key",
                ""
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Edge Case: Empty value message sent successfully");
        }
    }

    @Test
    @Order(31)
    @DisplayName("Edge Case: Null value message (tombstone)")
    void testNullValue() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19200");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "null-value-topic",
                "tombstone-key",
                null  // null value (tombstone)
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Edge Case: Null value (tombstone) message sent successfully");
        }
    }

    @Test
    @Order(32)
    @DisplayName("Edge Case: Unicode characters in message")
    void testUnicodeMessage() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19200");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String unicodeContent = "{\"test\": \"unicode\", \"emoji\": \"🎉🚀💻\", " +
                "\"chinese\": \"中文测试\", \"arabic\": \"اختبار\", \"russian\": \"тест\"}";

            ProducerRecord<String, String> record = new ProducerRecord<>(
                "unicode-topic",
                "unicode-key-测试",
                unicodeContent
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Edge Case: Unicode message sent successfully");
        }
    }

    @Test
    @Order(33)
    @DisplayName("Edge Case: Very long key")
    void testLongKey() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19200");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // Create a 1KB key
            StringBuilder keyBuilder = new StringBuilder();
            for (int i = 0; i < 1000; i++) {
                keyBuilder.append("k");
            }
            String longKey = keyBuilder.toString();

            ProducerRecord<String, String> record = new ProducerRecord<>(
                "long-key-topic",
                longKey,
                "{\"test\": \"long_key\", \"key_length\": 1000}"
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Edge Case: Long key (1KB) message sent successfully");
        }
    }

    @Test
    @Order(34)
    @DisplayName("Edge Case: Binary-like content as string")
    void testBinaryLikeContent() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19200");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // Create content with special characters
            String binaryLikeContent = "{\"test\": \"binary_like\", \"data\": \"\\u0000\\u0001\\u0002\\u0003\"}";

            ProducerRecord<String, String> record = new ProducerRecord<>(
                "binary-topic",
                "binary-key",
                binaryLikeContent
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Edge Case: Binary-like content sent successfully");
        }
    }

    @Test
    @Order(35)
    @DisplayName("Edge Case: Multiple headers with same name")
    void testDuplicateHeaders() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19200");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // Kafka allows multiple headers with the same name
            List<Header> headers = Arrays.asList(
                new RecordHeader("tag", "value1".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("tag", "value2".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("tag", "value3".getBytes(StandardCharsets.UTF_8))
            );

            ProducerRecord<String, String> record = new ProducerRecord<>(
                "duplicate-headers-topic",
                null,
                "dup-header-key",
                "{\"test\": \"duplicate_headers\"}",
                headers
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Edge Case: Multiple headers with same name sent successfully");
        }
    }

    // ==================== Helper Methods ====================

    private Properties createBaseProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        return props;
    }

    private void configureSaslScram512(Properties props, String username, String password) {
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required " +
            "username=\"" + username + "\" " +
            "password=\"" + password + "\";");
    }
}
