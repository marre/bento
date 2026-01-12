package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Binary serialization interoperability tests for Bento kafka_server input.
 *
 * Tests byte array (binary) message handling to ensure Bento correctly processes
 * raw binary data from Java Kafka producers.
 *
 * Required: Start Bento server on port 19200 (no auth) before running tests.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaBinaryInteropTest {

    private static final String BOOTSTRAP_SERVER = "127.0.0.1:19200";
    private static final int TIMEOUT_SECONDS = 10;

    // ==================== Basic Binary Tests ====================

    @Test
    @Order(1)
    @DisplayName("Binary: Simple byte array message")
    void testSimpleBinaryMessage() throws Exception {
        Properties props = createByteArrayProducerProperties();

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            byte[] key = "binary-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = "Simple binary message content".getBytes(StandardCharsets.UTF_8);

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("binary-topic", key, value);

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            assertEquals("binary-topic", metadata.topic());
            System.out.println("Binary: Simple byte array message sent successfully");
        }
    }

    @Test
    @Order(2)
    @DisplayName("Binary: Raw bytes with non-printable characters")
    void testNonPrintableBinaryMessage() throws Exception {
        Properties props = createByteArrayProducerProperties();

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            byte[] key = "raw-key".getBytes(StandardCharsets.UTF_8);
            // Create byte array with non-printable characters
            byte[] value = new byte[256];
            for (int i = 0; i < 256; i++) {
                value[i] = (byte) i;
            }

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("binary-raw-topic", key, value);

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Binary: Non-printable byte array message sent successfully");
        }
    }

    @Test
    @Order(3)
    @DisplayName("Binary: Protocol buffer-like structure")
    void testProtobufLikeMessage() throws Exception {
        Properties props = createByteArrayProducerProperties();

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            // Simulate a simple binary protocol structure
            ByteBuffer buffer = ByteBuffer.allocate(100);
            buffer.put((byte) 0x01);       // Version
            buffer.putInt(12345);          // Message ID
            buffer.putLong(System.currentTimeMillis()); // Timestamp
            buffer.putShort((short) 42);   // Type
            buffer.put("payload".getBytes(StandardCharsets.UTF_8)); // Payload

            byte[] key = "proto-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = Arrays.copyOf(buffer.array(), buffer.position());

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("binary-proto-topic", key, value);

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Binary: Protocol buffer-like message sent successfully");
        }
    }

    @Test
    @Order(4)
    @DisplayName("Binary: Large binary payload")
    void testLargeBinaryPayload() throws Exception {
        Properties props = createByteArrayProducerProperties();

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            byte[] key = "large-binary-key".getBytes(StandardCharsets.UTF_8);
            // Create 100KB binary payload with pattern
            byte[] value = new byte[100 * 1024];
            for (int i = 0; i < value.length; i++) {
                value[i] = (byte) (i % 256);
            }

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("binary-large-topic", key, value);

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Binary: Large binary payload (100KB) sent successfully");
        }
    }

    // ==================== Mixed Serialization Tests ====================

    @Test
    @Order(10)
    @DisplayName("Mixed: String key with binary value")
    void testStringKeyBinaryValue() throws Exception {
        Properties props = createMixedProducerProperties();

        try (KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props)) {
            String key = "string-key-for-binary";
            byte[] value = new byte[] {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};

            ProducerRecord<String, byte[]> record = new ProducerRecord<>("mixed-topic", key, value);

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            assertEquals("mixed-topic", metadata.topic());
            System.out.println("Mixed: String key with binary value sent successfully");
        }
    }

    @Test
    @Order(11)
    @DisplayName("Mixed: Binary key with string value")
    void testBinaryKeyStringValue() throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15000);

        try (KafkaProducer<byte[], String> producer = new KafkaProducer<>(props)) {
            byte[] key = new byte[] {0x10, 0x20, 0x30, 0x40};
            String value = "{\"message\": \"JSON value with binary key\"}";

            ProducerRecord<byte[], String> record = new ProducerRecord<>("mixed-binary-key-topic", key, value);

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Mixed: Binary key with string value sent successfully");
        }
    }

    // ==================== Edge Case Binary Tests ====================

    @Test
    @Order(20)
    @DisplayName("Binary Edge: Empty byte array")
    void testEmptyBinaryMessage() throws Exception {
        Properties props = createByteArrayProducerProperties();

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            byte[] key = "empty-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = new byte[0]; // Empty array

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("binary-empty-topic", key, value);

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Binary Edge: Empty byte array sent successfully");
        }
    }

    @Test
    @Order(21)
    @DisplayName("Binary Edge: Single byte message")
    void testSingleByteBinaryMessage() throws Exception {
        Properties props = createByteArrayProducerProperties();

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            byte[] key = "single-byte-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = new byte[] {0x42}; // Single byte

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("binary-single-topic", key, value);

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Binary Edge: Single byte message sent successfully");
        }
    }

    @Test
    @Order(22)
    @DisplayName("Binary Edge: All zeros")
    void testAllZerosBinaryMessage() throws Exception {
        Properties props = createByteArrayProducerProperties();

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            byte[] key = "zeros-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = new byte[1024]; // All zeros by default

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("binary-zeros-topic", key, value);

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Binary Edge: All zeros message sent successfully");
        }
    }

    @Test
    @Order(23)
    @DisplayName("Binary Edge: All 0xFF bytes")
    void testAllOnesBinaryMessage() throws Exception {
        Properties props = createByteArrayProducerProperties();

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            byte[] key = "ones-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = new byte[1024];
            Arrays.fill(value, (byte) 0xFF);

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("binary-ones-topic", key, value);

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Binary Edge: All 0xFF bytes message sent successfully");
        }
    }

    @Test
    @Order(24)
    @DisplayName("Binary Edge: Null key with binary value")
    void testNullKeyBinaryValue() throws Exception {
        Properties props = createByteArrayProducerProperties();

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            byte[] value = "Binary value with null key".getBytes(StandardCharsets.UTF_8);

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("binary-null-key-topic", null, value);

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Binary Edge: Null key with binary value sent successfully");
        }
    }

    @Test
    @Order(25)
    @DisplayName("Binary Edge: Binary with headers")
    void testBinaryWithHeaders() throws Exception {
        Properties props = createByteArrayProducerProperties();

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            byte[] key = "binary-headers-key".getBytes(StandardCharsets.UTF_8);
            byte[] value = new byte[] {0x01, 0x02, 0x03, 0x04};

            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("binary-headers-topic", key, value);
            record.headers().add(new RecordHeader("content-type", "application/octet-stream".getBytes()));
            record.headers().add(new RecordHeader("binary-header", new byte[] {0xDE, 0xAD, 0xBE, 0xEF}));

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Binary Edge: Binary message with headers sent successfully");
        }
    }

    // ==================== Binary Batch Tests ====================

    @Test
    @Order(30)
    @DisplayName("Binary Batch: Multiple binary messages")
    void testBinaryBatch() throws Exception {
        Properties props = createByteArrayProducerProperties();
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
            List<RecordMetadata> results = new ArrayList<>();

            for (int i = 0; i < 10; i++) {
                byte[] key = ("batch-key-" + i).getBytes(StandardCharsets.UTF_8);
                ByteBuffer buffer = ByteBuffer.allocate(32);
                buffer.putInt(i);
                buffer.putLong(System.currentTimeMillis());
                buffer.put((byte) (i * 10));

                byte[] value = Arrays.copyOf(buffer.array(), buffer.position());

                ProducerRecord<byte[], byte[]> record = new ProducerRecord<>("binary-batch-topic", key, value);
                results.add(producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }

            assertEquals(10, results.size());
            System.out.println("Binary Batch: " + results.size() + " binary messages sent successfully");
        }
    }

    // ==================== Helper Methods ====================

    private Properties createByteArrayProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        return props;
    }

    private Properties createMixedProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        return props;
    }
}
