import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Standalone integration test for Bento kafka_server using Apache Kafka Java client.
 *
 * Usage:
 *   1. Download kafka-clients JAR and dependencies
 *   2. Compile: javac -cp "libs/*" KafkaInteropTest.java
 *   3. Run: java -cp "libs/*:." KafkaInteropTest
 *
 * Or use the provided run script.
 */
public class KafkaInteropTest {

    private static final int TIMEOUT_SECONDS = 10;
    private static int passed = 0;
    private static int failed = 0;

    public static void main(String[] args) {
        System.out.println("=".repeat(60));
        System.out.println("Bento kafka_server Java Interoperability Tests");
        System.out.println("Using Apache Kafka Java Client");
        System.out.println("=".repeat(60));

        // Wait for servers to be ready
        System.out.println("\nWaiting 2 seconds for Bento servers to be ready...");
        try { Thread.sleep(2000); } catch (InterruptedException e) {}

        // Run tests
        testNoAuthBasicSend();
        testNoAuthMultipleMessages();
        testNoAuthMessageWithHeaders();
        testNoAuthNullKey();
        testNoAuthLargeMessage();

        testSaslPlainAuth();
        testSaslPlainMultipleMessages();
        testSaslPlainWrongPassword();

        testSaslScramAuth();
        testSaslScramMultipleMessages();
        testSaslScramWithHeaders();
        testSaslScramWrongPassword();

        // Summary
        System.out.println("\n" + "=".repeat(60));
        System.out.println("TEST SUMMARY");
        System.out.println("=".repeat(60));
        System.out.println("Passed: " + passed);
        System.out.println("Failed: " + failed);
        System.out.println("Total:  " + (passed + failed));
        System.out.println("\n" + (failed == 0 ? "ALL TESTS PASSED!" : "SOME TESTS FAILED!"));

        System.exit(failed == 0 ? 0 : 1);
    }

    // ==================== No Authentication Tests ====================

    private static void testNoAuthBasicSend() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("TEST: No Auth - Basic message send");
        System.out.println("=".repeat(60));

        try {
            Properties props = createBaseProperties("127.0.0.1:19200");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "test-topic", "test-key", "{\"test\": \"java_noauth_message\"}"
                );
                RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                System.out.println("  Sent to " + metadata.topic() + ", partition=" + metadata.partition());
                System.out.println("SUCCESS: No Auth basic send passed");
                passed++;
            }
        } catch (Exception e) {
            System.out.println("FAILED: " + e.getMessage());
            failed++;
        }
    }

    private static void testNoAuthMultipleMessages() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("TEST: No Auth - Multiple messages");
        System.out.println("=".repeat(60));

        try {
            Properties props = createBaseProperties("127.0.0.1:19200");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                for (int i = 0; i < 5; i++) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                        "multi-topic", "key-" + i, "{\"index\": " + i + ", \"source\": \"java\"}"
                    );
                    RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    System.out.println("  Sent message " + (i+1) + " to " + metadata.topic());
                }
                System.out.println("SUCCESS: No Auth multiple messages passed");
                passed++;
            }
        } catch (Exception e) {
            System.out.println("FAILED: " + e.getMessage());
            failed++;
        }
    }

    private static void testNoAuthMessageWithHeaders() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("TEST: No Auth - Message with headers");
        System.out.println("=".repeat(60));

        try {
            Properties props = createBaseProperties("127.0.0.1:19200");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                List<Header> headers = Arrays.asList(
                    new RecordHeader("source", "java-client".getBytes(StandardCharsets.UTF_8)),
                    new RecordHeader("version", "1.0".getBytes(StandardCharsets.UTF_8))
                );
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "headers-topic", null, "header-key", "{\"test\": \"message_with_headers\"}", headers
                );
                RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                System.out.println("  Sent message with headers to " + metadata.topic());
                System.out.println("SUCCESS: No Auth message with headers passed");
                passed++;
            }
        } catch (Exception e) {
            System.out.println("FAILED: " + e.getMessage());
            failed++;
        }
    }

    private static void testNoAuthNullKey() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("TEST: No Auth - Null key message");
        System.out.println("=".repeat(60));

        try {
            Properties props = createBaseProperties("127.0.0.1:19200");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "null-key-topic", null, "{\"test\": \"null_key_message\"}"
                );
                RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                System.out.println("  Sent message with null key to " + metadata.topic());
                System.out.println("SUCCESS: No Auth null key passed");
                passed++;
            }
        } catch (Exception e) {
            System.out.println("FAILED: " + e.getMessage());
            failed++;
        }
    }

    private static void testNoAuthLargeMessage() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("TEST: No Auth - Large message (~100KB)");
        System.out.println("=".repeat(60));

        try {
            Properties props = createBaseProperties("127.0.0.1:19200");
            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                StringBuilder sb = new StringBuilder();
                sb.append("{\"test\": \"large_message\", \"data\": \"");
                for (int i = 0; i < 100000; i++) sb.append("x");
                sb.append("\"}");

                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "large-topic", "large-key", sb.toString()
                );
                RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                System.out.println("  Sent large message to " + metadata.topic());
                System.out.println("SUCCESS: No Auth large message passed");
                passed++;
            }
        } catch (Exception e) {
            System.out.println("FAILED: " + e.getMessage());
            failed++;
        }
    }

    // ==================== SASL PLAIN Authentication Tests ====================

    private static void testSaslPlainAuth() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("TEST: SASL PLAIN - Authenticated message");
        System.out.println("=".repeat(60));

        try {
            Properties props = createBaseProperties("127.0.0.1:19201");
            configureSaslPlain(props, "testuser", "testpass");

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "plain-auth-topic", "plain-key", "{\"test\": \"sasl_plain_message\"}"
                );
                RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                System.out.println("  Sent to " + metadata.topic() + ", partition=" + metadata.partition());
                System.out.println("SUCCESS: SASL PLAIN auth passed");
                passed++;
            }
        } catch (Exception e) {
            System.out.println("FAILED: " + e.getMessage());
            failed++;
        }
    }

    private static void testSaslPlainMultipleMessages() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("TEST: SASL PLAIN - Multiple messages");
        System.out.println("=".repeat(60));

        try {
            Properties props = createBaseProperties("127.0.0.1:19201");
            configureSaslPlain(props, "testuser", "testpass");

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                for (int i = 0; i < 3; i++) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                        "plain-multi-topic", "plain-key-" + i, "{\"index\": " + i + ", \"auth\": \"PLAIN\"}"
                    );
                    RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    System.out.println("  Sent message " + (i+1) + " to " + metadata.topic());
                }
                System.out.println("SUCCESS: SASL PLAIN multiple messages passed");
                passed++;
            }
        } catch (Exception e) {
            System.out.println("FAILED: " + e.getMessage());
            failed++;
        }
    }

    private static void testSaslPlainWrongPassword() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("TEST: SASL PLAIN - Wrong password (expect failure)");
        System.out.println("=".repeat(60));

        try {
            Properties props = createBaseProperties("127.0.0.1:19201");
            configureSaslPlain(props, "testuser", "wrongpassword");

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "should-fail-topic", "fail-key", "should not be sent"
                );
                producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                System.out.println("FAILED: Message was sent (should have failed)");
                failed++;
            }
        } catch (Exception e) {
            System.out.println("EXPECTED FAILURE: " + e.getClass().getSimpleName());
            System.out.println("SUCCESS: Wrong password correctly rejected");
            passed++;
        }
    }

    // ==================== SASL SCRAM-SHA-256 Authentication Tests ====================

    private static void testSaslScramAuth() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("TEST: SASL SCRAM-SHA-256 - Authenticated message");
        System.out.println("=".repeat(60));

        try {
            Properties props = createBaseProperties("127.0.0.1:19202");
            configureSaslScram256(props, "scramuser", "scrampass");

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "scram-auth-topic", "scram-key", "{\"test\": \"sasl_scram_message\"}"
                );
                RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                System.out.println("  Sent to " + metadata.topic() + ", partition=" + metadata.partition());
                System.out.println("SUCCESS: SASL SCRAM-SHA-256 auth passed");
                passed++;
            }
        } catch (Exception e) {
            System.out.println("FAILED: " + e.getMessage());
            e.printStackTrace();
            failed++;
        }
    }

    private static void testSaslScramMultipleMessages() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("TEST: SASL SCRAM-SHA-256 - Multiple messages");
        System.out.println("=".repeat(60));

        try {
            Properties props = createBaseProperties("127.0.0.1:19202");
            configureSaslScram256(props, "scramuser", "scrampass");

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                for (int i = 0; i < 3; i++) {
                    ProducerRecord<String, String> record = new ProducerRecord<>(
                        "scram-multi-topic", "scram-key-" + i, "{\"index\": " + i + ", \"auth\": \"SCRAM-SHA-256\"}"
                    );
                    RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                    System.out.println("  Sent message " + (i+1) + " to " + metadata.topic());
                }
                System.out.println("SUCCESS: SASL SCRAM-SHA-256 multiple messages passed");
                passed++;
            }
        } catch (Exception e) {
            System.out.println("FAILED: " + e.getMessage());
            failed++;
        }
    }

    private static void testSaslScramWithHeaders() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("TEST: SASL SCRAM-SHA-256 - Message with headers");
        System.out.println("=".repeat(60));

        try {
            Properties props = createBaseProperties("127.0.0.1:19202");
            configureSaslScram256(props, "scramuser", "scrampass");

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                List<Header> headers = Arrays.asList(
                    new RecordHeader("auth-method", "SCRAM-SHA-256".getBytes(StandardCharsets.UTF_8)),
                    new RecordHeader("client", "java-kafka-client".getBytes(StandardCharsets.UTF_8))
                );
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "scram-headers-topic", null, "scram-header-key", "{\"test\": \"scram_with_headers\"}", headers
                );
                RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                System.out.println("  Sent message with headers to " + metadata.topic());
                System.out.println("SUCCESS: SASL SCRAM-SHA-256 with headers passed");
                passed++;
            }
        } catch (Exception e) {
            System.out.println("FAILED: " + e.getMessage());
            failed++;
        }
    }

    private static void testSaslScramWrongPassword() {
        System.out.println("\n" + "=".repeat(60));
        System.out.println("TEST: SASL SCRAM-SHA-256 - Wrong password (expect failure)");
        System.out.println("=".repeat(60));

        try {
            Properties props = createBaseProperties("127.0.0.1:19202");
            configureSaslScram256(props, "scramuser", "wrongpassword");

            try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "should-fail-topic", "fail-key", "should not be sent"
                );
                producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                System.out.println("FAILED: Message was sent (should have failed)");
                failed++;
            }
        } catch (Exception e) {
            System.out.println("EXPECTED FAILURE: " + e.getClass().getSimpleName());
            System.out.println("SUCCESS: Wrong password correctly rejected");
            passed++;
        }
    }

    // ==================== Helper Methods ====================

    private static Properties createBaseProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1"); // Use acks=1 instead of all
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false); // Disable idempotence (requires INIT_PRODUCER_ID)
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        return props;
    }

    private static void configureSaslPlain(Properties props, String username, String password) {
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"" + username + "\" " +
            "password=\"" + password + "\";");
    }

    private static void configureSaslScram256(Properties props, String username, String password) {
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "SCRAM-SHA-256");
        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required " +
            "username=\"" + username + "\" " +
            "password=\"" + password + "\";");
    }
}
