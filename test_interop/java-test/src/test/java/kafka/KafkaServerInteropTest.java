package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for Bento kafka_server input using the official Apache Kafka Java client.
 *
 * These tests verify interoperability between the Bento kafka_server and Java Kafka producers.
 *
 * Required: Start Bento servers before running tests:
 * - Port 19200: No authentication
 * - Port 19201: SASL PLAIN (testuser/testpass)
 * - Port 19202: SASL SCRAM-SHA-256 (scramuser/scrampass)
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaServerInteropTest {

    private static final int TIMEOUT_SECONDS = 10;

    // ==================== No Authentication Tests ====================

    @Test
    @Order(1)
    @DisplayName("No Auth: Basic message send")
    void testNoAuthBasicSend() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19200");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "test-topic",
                "test-key",
                "{\"test\": \"java_noauth_message\"}"
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            assertEquals("test-topic", metadata.topic());
            assertEquals(0, metadata.partition());
            System.out.println("No Auth: Message sent successfully to " + metadata.topic());
        }
    }

    @Test
    @Order(2)
    @DisplayName("No Auth: Multiple messages")
    void testNoAuthMultipleMessages() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19200");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<RecordMetadata> results = new ArrayList<>();

            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "multi-topic",
                    "key-" + i,
                    "{\"index\": " + i + ", \"source\": \"java\"}"
                );
                results.add(producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }

            assertEquals(5, results.size());
            for (RecordMetadata metadata : results) {
                assertEquals("multi-topic", metadata.topic());
            }
            System.out.println("No Auth: " + results.size() + " messages sent successfully");
        }
    }

    @Test
    @Order(3)
    @DisplayName("No Auth: Message with headers")
    void testNoAuthMessageWithHeaders() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19200");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<Header> headers = Arrays.asList(
                new RecordHeader("source", "java-client".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("version", "1.0".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("correlation-id", UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8))
            );

            ProducerRecord<String, String> record = new ProducerRecord<>(
                "headers-topic",
                null, // partition
                "header-key",
                "{\"test\": \"message_with_headers\"}",
                headers
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            assertEquals("headers-topic", metadata.topic());
            System.out.println("No Auth: Message with headers sent successfully");
        }
    }

    @Test
    @Order(4)
    @DisplayName("No Auth: Null key message")
    void testNoAuthNullKey() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19200");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "null-key-topic",
                null, // null key
                "{\"test\": \"null_key_message\"}"
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("No Auth: Null key message sent successfully");
        }
    }

    @Test
    @Order(5)
    @DisplayName("No Auth: Large message (~100KB)")
    void testNoAuthLargeMessage() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19200");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // Create ~100KB message
            StringBuilder sb = new StringBuilder();
            sb.append("{\"test\": \"large_message\", \"data\": \"");
            for (int i = 0; i < 100000; i++) {
                sb.append("x");
            }
            sb.append("\"}");

            ProducerRecord<String, String> record = new ProducerRecord<>(
                "large-topic",
                "large-key",
                sb.toString()
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("No Auth: Large message (~100KB) sent successfully");
        }
    }

    // ==================== SASL PLAIN Authentication Tests ====================

    @Test
    @Order(10)
    @DisplayName("SASL PLAIN: Authenticated message send")
    void testSaslPlainAuth() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19201");
        configureSaslPlain(props, "testuser", "testpass");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "plain-auth-topic",
                "plain-key",
                "{\"test\": \"sasl_plain_message\", \"user\": \"testuser\"}"
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            assertEquals("plain-auth-topic", metadata.topic());
            System.out.println("SASL PLAIN: Authenticated message sent successfully");
        }
    }

    @Test
    @Order(11)
    @DisplayName("SASL PLAIN: Multiple authenticated messages")
    void testSaslPlainMultipleMessages() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19201");
        configureSaslPlain(props, "testuser", "testpass");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 3; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "plain-multi-topic",
                    "plain-key-" + i,
                    "{\"index\": " + i + ", \"auth\": \"PLAIN\"}"
                );
                RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertNotNull(metadata);
            }
            System.out.println("SASL PLAIN: Multiple authenticated messages sent successfully");
        }
    }

    @Test
    @Order(12)
    @DisplayName("SASL PLAIN: Wrong password should fail")
    void testSaslPlainWrongPassword() {
        Properties props = createBaseProperties("127.0.0.1:19201");
        configureSaslPlain(props, "testuser", "wrongpassword");

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
        System.out.println("SASL PLAIN: Wrong password correctly rejected");
    }

    // ==================== SASL SCRAM-SHA-256 Authentication Tests ====================

    @Test
    @Order(20)
    @DisplayName("SASL SCRAM-SHA-256: Authenticated message send")
    void testSaslScramAuth() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19202");
        configureSaslScram256(props, "scramuser", "scrampass");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "scram-auth-topic",
                "scram-key",
                "{\"test\": \"sasl_scram_message\", \"mechanism\": \"SCRAM-SHA-256\"}"
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            assertEquals("scram-auth-topic", metadata.topic());
            System.out.println("SASL SCRAM-SHA-256: Authenticated message sent successfully");
        }
    }

    @Test
    @Order(21)
    @DisplayName("SASL SCRAM-SHA-256: Multiple authenticated messages")
    void testSaslScramMultipleMessages() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19202");
        configureSaslScram256(props, "scramuser", "scrampass");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 0; i < 3; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "scram-multi-topic",
                    "scram-key-" + i,
                    "{\"index\": " + i + ", \"auth\": \"SCRAM-SHA-256\"}"
                );
                RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                assertNotNull(metadata);
            }
            System.out.println("SASL SCRAM-SHA-256: Multiple authenticated messages sent successfully");
        }
    }

    @Test
    @Order(22)
    @DisplayName("SASL SCRAM-SHA-256: Message with headers")
    void testSaslScramWithHeaders() throws Exception {
        Properties props = createBaseProperties("127.0.0.1:19202");
        configureSaslScram256(props, "scramuser", "scrampass");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<Header> headers = Arrays.asList(
                new RecordHeader("auth-method", "SCRAM-SHA-256".getBytes(StandardCharsets.UTF_8)),
                new RecordHeader("client", "java-kafka-client".getBytes(StandardCharsets.UTF_8))
            );

            ProducerRecord<String, String> record = new ProducerRecord<>(
                "scram-headers-topic",
                null,
                "scram-header-key",
                "{\"test\": \"scram_with_headers\"}",
                headers
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("SASL SCRAM-SHA-256: Message with headers sent successfully");
        }
    }

    @Test
    @Order(23)
    @DisplayName("SASL SCRAM-SHA-256: Wrong password should fail")
    void testSaslScramWrongPassword() {
        Properties props = createBaseProperties("127.0.0.1:19202");
        configureSaslScram256(props, "scramuser", "wrongpassword");

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
        System.out.println("SASL SCRAM-SHA-256: Wrong password correctly rejected");
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

    private void configureSaslPlain(Properties props, String username, String password) {
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"" + username + "\" " +
            "password=\"" + password + "\";");
    }

    private void configureSaslScram256(Properties props, String username, String password) {
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required " +
            "username=\"" + username + "\" " +
            "password=\"" + password + "\";");
    }
}
