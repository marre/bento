package kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Compression interoperability tests for Bento kafka_server input.
 *
 * Tests various compression codecs to ensure Bento can receive compressed messages
 * from Java Kafka producers.
 *
 * Required: Start Bento server on port 19300 (no auth) before running tests.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class KafkaCompressionInteropTest {

    private static final String BOOTSTRAP_SERVER = "127.0.0.1:19300";
    private static final int TIMEOUT_SECONDS = 10;

    // ==================== GZIP Compression Tests ====================

    @Test
    @Order(1)
    @DisplayName("GZIP: Single message compression")
    void testGzipSingleMessage() throws Exception {
        Properties props = createProducerProperties("gzip");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "compressed-gzip",
                "gzip-key",
                "{\"codec\": \"gzip\", \"test\": \"single_message\", \"data\": \"" + generateData(500) + "\"}"
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            assertEquals("compressed-gzip", metadata.topic());
            System.out.println("GZIP: Single message sent successfully");
        }
    }

    @Test
    @Order(2)
    @DisplayName("GZIP: Multiple messages batch")
    void testGzipMultipleMessages() throws Exception {
        Properties props = createProducerProperties("gzip");
        // Enable batching
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<RecordMetadata> results = new ArrayList<>();

            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "compressed-gzip-batch",
                    "gzip-key-" + i,
                    "{\"codec\": \"gzip\", \"index\": " + i + ", \"data\": \"" + generateData(200) + "\"}"
                );
                results.add(producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }

            assertEquals(5, results.size());
            System.out.println("GZIP: " + results.size() + " batched messages sent successfully");
        }
    }

    @Test
    @Order(3)
    @DisplayName("GZIP: Large message compression")
    void testGzipLargeMessage() throws Exception {
        Properties props = createProducerProperties("gzip");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            // ~50KB message - GZIP should compress this significantly
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "compressed-gzip-large",
                "gzip-large-key",
                "{\"codec\": \"gzip\", \"test\": \"large_message\", \"data\": \"" + generateData(50000) + "\"}"
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("GZIP: Large message (~50KB) sent successfully");
        }
    }

    // ==================== Snappy Compression Tests ====================

    @Test
    @Order(10)
    @DisplayName("Snappy: Single message compression")
    void testSnappySingleMessage() throws Exception {
        Properties props = createProducerProperties("snappy");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "compressed-snappy",
                "snappy-key",
                "{\"codec\": \"snappy\", \"test\": \"single_message\", \"data\": \"" + generateData(500) + "\"}"
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            assertEquals("compressed-snappy", metadata.topic());
            System.out.println("Snappy: Single message sent successfully");
        }
    }

    @Test
    @Order(11)
    @DisplayName("Snappy: Multiple messages batch")
    void testSnappyMultipleMessages() throws Exception {
        Properties props = createProducerProperties("snappy");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<RecordMetadata> results = new ArrayList<>();

            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "compressed-snappy-batch",
                    "snappy-key-" + i,
                    "{\"codec\": \"snappy\", \"index\": " + i + ", \"data\": \"" + generateData(200) + "\"}"
                );
                results.add(producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }

            assertEquals(5, results.size());
            System.out.println("Snappy: " + results.size() + " batched messages sent successfully");
        }
    }

    @Test
    @Order(12)
    @DisplayName("Snappy: Large message compression")
    void testSnappyLargeMessage() throws Exception {
        Properties props = createProducerProperties("snappy");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "compressed-snappy-large",
                "snappy-large-key",
                "{\"codec\": \"snappy\", \"test\": \"large_message\", \"data\": \"" + generateData(50000) + "\"}"
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("Snappy: Large message (~50KB) sent successfully");
        }
    }

    // ==================== LZ4 Compression Tests ====================

    @Test
    @Order(20)
    @DisplayName("LZ4: Single message compression")
    void testLz4SingleMessage() throws Exception {
        Properties props = createProducerProperties("lz4");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "compressed-lz4",
                "lz4-key",
                "{\"codec\": \"lz4\", \"test\": \"single_message\", \"data\": \"" + generateData(500) + "\"}"
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            assertEquals("compressed-lz4", metadata.topic());
            System.out.println("LZ4: Single message sent successfully");
        }
    }

    @Test
    @Order(21)
    @DisplayName("LZ4: Multiple messages batch")
    void testLz4MultipleMessages() throws Exception {
        Properties props = createProducerProperties("lz4");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<RecordMetadata> results = new ArrayList<>();

            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "compressed-lz4-batch",
                    "lz4-key-" + i,
                    "{\"codec\": \"lz4\", \"index\": " + i + ", \"data\": \"" + generateData(200) + "\"}"
                );
                results.add(producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }

            assertEquals(5, results.size());
            System.out.println("LZ4: " + results.size() + " batched messages sent successfully");
        }
    }

    @Test
    @Order(22)
    @DisplayName("LZ4: Large message compression")
    void testLz4LargeMessage() throws Exception {
        Properties props = createProducerProperties("lz4");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "compressed-lz4-large",
                "lz4-large-key",
                "{\"codec\": \"lz4\", \"test\": \"large_message\", \"data\": \"" + generateData(50000) + "\"}"
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("LZ4: Large message (~50KB) sent successfully");
        }
    }

    // ==================== ZSTD Compression Tests ====================

    @Test
    @Order(30)
    @DisplayName("ZSTD: Single message compression")
    void testZstdSingleMessage() throws Exception {
        Properties props = createProducerProperties("zstd");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "compressed-zstd",
                "zstd-key",
                "{\"codec\": \"zstd\", \"test\": \"single_message\", \"data\": \"" + generateData(500) + "\"}"
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            assertEquals("compressed-zstd", metadata.topic());
            System.out.println("ZSTD: Single message sent successfully");
        }
    }

    @Test
    @Order(31)
    @DisplayName("ZSTD: Multiple messages batch")
    void testZstdMultipleMessages() throws Exception {
        Properties props = createProducerProperties("zstd");
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            List<RecordMetadata> results = new ArrayList<>();

            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(
                    "compressed-zstd-batch",
                    "zstd-key-" + i,
                    "{\"codec\": \"zstd\", \"index\": " + i + ", \"data\": \"" + generateData(200) + "\"}"
                );
                results.add(producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }

            assertEquals(5, results.size());
            System.out.println("ZSTD: " + results.size() + " batched messages sent successfully");
        }
    }

    @Test
    @Order(32)
    @DisplayName("ZSTD: Large message compression")
    void testZstdLargeMessage() throws Exception {
        Properties props = createProducerProperties("zstd");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "compressed-zstd-large",
                "zstd-large-key",
                "{\"codec\": \"zstd\", \"test\": \"large_message\", \"data\": \"" + generateData(50000) + "\"}"
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            System.out.println("ZSTD: Large message (~50KB) sent successfully");
        }
    }

    // ==================== No Compression Baseline Test ====================

    @Test
    @Order(40)
    @DisplayName("No Compression: Baseline message")
    void testNoCompression() throws Exception {
        Properties props = createProducerProperties("none");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            ProducerRecord<String, String> record = new ProducerRecord<>(
                "no-compression",
                "baseline-key",
                "{\"codec\": \"none\", \"test\": \"baseline\", \"data\": \"" + generateData(500) + "\"}"
            );

            RecordMetadata metadata = producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            assertNotNull(metadata);
            assertEquals("no-compression", metadata.topic());
            System.out.println("No Compression: Baseline message sent successfully");
        }
    }

    // ==================== Helper Methods ====================

    private Properties createProducerProperties(String compressionType) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10000);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 15000);
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000);
        return props;
    }

    private String generateData(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append('x');
        }
        return sb.toString();
    }
}
