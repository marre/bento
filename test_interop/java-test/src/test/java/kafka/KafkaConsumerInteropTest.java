package kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Consumer interoperability tests for Kafka.
 *
 * These tests verify that Java Kafka consumers can correctly interact with
 * Kafka-compatible systems. These tests require a real Kafka broker.
 *
 * Set environment variable KAFKA_BROKER=host:port to enable these tests.
 * Example: KAFKA_BROKER=localhost:9092
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@EnabledIfEnvironmentVariable(named = "KAFKA_BROKER", matches = ".+")
public class KafkaConsumerInteropTest {

    private static final int TIMEOUT_SECONDS = 30;
    private static final String KAFKA_BROKER = System.getenv("KAFKA_BROKER");

    // ==================== Basic Consumer Tests ====================

    @Test
    @Order(1)
    @DisplayName("Consumer: Produce and consume single message")
    void testProduceAndConsumeSingle() throws Exception {
        String topic = "test-consumer-single-" + UUID.randomUUID();
        String testValue = "{\"test\": \"consumer_single\", \"id\": \"" + UUID.randomUUID() + "\"}";

        // Produce a message
        try (KafkaProducer<String, String> producer = createProducer()) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "test-key", testValue);
            producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            producer.flush();
        }

        // Consume the message
        try (KafkaConsumer<String, String> consumer = createConsumer("test-group-single")) {
            consumer.subscribe(Collections.singletonList(topic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(TIMEOUT_SECONDS));

            assertFalse(records.isEmpty(), "Should have received at least one message");

            ConsumerRecord<String, String> received = records.iterator().next();
            assertEquals("test-key", received.key());
            assertEquals(testValue, received.value());
            System.out.println("Consumer: Single message consumed successfully");
        }
    }

    @Test
    @Order(2)
    @DisplayName("Consumer: Produce and consume multiple messages")
    void testProduceAndConsumeMultiple() throws Exception {
        String topic = "test-consumer-multiple-" + UUID.randomUUID();
        int messageCount = 10;
        List<String> sentValues = new ArrayList<>();

        // Produce messages
        try (KafkaProducer<String, String> producer = createProducer()) {
            for (int i = 0; i < messageCount; i++) {
                String value = "{\"test\": \"consumer_multiple\", \"index\": " + i + "}";
                sentValues.add(value);
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key-" + i, value);
                producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            producer.flush();
        }

        // Consume messages
        try (KafkaConsumer<String, String> consumer = createConsumer("test-group-multiple")) {
            consumer.subscribe(Collections.singletonList(topic));

            List<String> receivedValues = new ArrayList<>();
            long deadline = System.currentTimeMillis() + (TIMEOUT_SECONDS * 1000);

            while (receivedValues.size() < messageCount && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    receivedValues.add(record.value());
                }
            }

            assertEquals(messageCount, receivedValues.size(), "Should have received all messages");
            assertTrue(receivedValues.containsAll(sentValues), "All sent values should be received");
            System.out.println("Consumer: " + receivedValues.size() + " messages consumed successfully");
        }
    }

    @Test
    @Order(3)
    @DisplayName("Consumer: Messages with headers")
    void testConsumeWithHeaders() throws Exception {
        String topic = "test-consumer-headers-" + UUID.randomUUID();

        // Produce message with headers
        try (KafkaProducer<String, String> producer = createProducer()) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "header-key", "{\"test\": \"with_headers\"}");
            record.headers().add("custom-header", "custom-value".getBytes());
            record.headers().add("source", "java-producer".getBytes());
            producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            producer.flush();
        }

        // Consume and verify headers
        try (KafkaConsumer<String, String> consumer = createConsumer("test-group-headers")) {
            consumer.subscribe(Collections.singletonList(topic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(TIMEOUT_SECONDS));

            assertFalse(records.isEmpty(), "Should have received message");

            ConsumerRecord<String, String> received = records.iterator().next();
            assertNotNull(received.headers().lastHeader("custom-header"));
            assertEquals("custom-value", new String(received.headers().lastHeader("custom-header").value()));
            System.out.println("Consumer: Message with headers consumed successfully");
        }
    }

    @Test
    @Order(4)
    @DisplayName("Consumer: Commit offsets manually")
    void testManualOffsetCommit() throws Exception {
        String topic = "test-consumer-commit-" + UUID.randomUUID();
        String groupId = "test-group-commit-" + UUID.randomUUID();

        // Produce messages
        try (KafkaProducer<String, String> producer = createProducer()) {
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key-" + i, "{\"index\": " + i + "}");
                producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            producer.flush();
        }

        // First consumer - consume and commit
        try (KafkaConsumer<String, String> consumer = createConsumerManualCommit(groupId)) {
            consumer.subscribe(Collections.singletonList(topic));

            int consumed = 0;
            long deadline = System.currentTimeMillis() + (TIMEOUT_SECONDS * 1000);

            while (consumed < 5 && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    consumed++;
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }

            assertEquals(5, consumed, "Should have consumed all messages");
        }

        // Second consumer with same group - should not receive already committed messages
        try (KafkaConsumer<String, String> consumer = createConsumerManualCommit(groupId)) {
            consumer.subscribe(Collections.singletonList(topic));

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            assertTrue(records.isEmpty(), "Should not receive already committed messages");
            System.out.println("Consumer: Manual offset commit verified successfully");
        }
    }

    @Test
    @Order(5)
    @DisplayName("Consumer: Seek to beginning")
    void testSeekToBeginning() throws Exception {
        String topic = "test-consumer-seek-" + UUID.randomUUID();

        // Produce messages
        try (KafkaProducer<String, String> producer = createProducer()) {
            for (int i = 0; i < 3; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key-" + i, "{\"index\": " + i + "}");
                producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            producer.flush();
        }

        // First read
        try (KafkaConsumer<String, String> consumer = createConsumer("test-group-seek-" + UUID.randomUUID())) {
            consumer.subscribe(Collections.singletonList(topic));

            // Consume all
            List<String> firstRead = new ArrayList<>();
            long deadline = System.currentTimeMillis() + (TIMEOUT_SECONDS * 1000);
            while (firstRead.size() < 3 && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    firstRead.add(record.value());
                }
            }

            assertEquals(3, firstRead.size(), "Should have consumed all messages");

            // Seek to beginning
            consumer.seekToBeginning(consumer.assignment());

            // Read again
            List<String> secondRead = new ArrayList<>();
            deadline = System.currentTimeMillis() + (TIMEOUT_SECONDS * 1000);
            while (secondRead.size() < 3 && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    secondRead.add(record.value());
                }
            }

            assertEquals(3, secondRead.size(), "Should have re-consumed all messages after seek");
            System.out.println("Consumer: Seek to beginning verified successfully");
        }
    }

    // ==================== Consumer Group Tests ====================

    @Test
    @Order(10)
    @DisplayName("Consumer Group: Multiple consumers in same group")
    void testConsumerGroup() throws Exception {
        String topic = "test-consumer-group-" + UUID.randomUUID();
        String groupId = "shared-group-" + UUID.randomUUID();
        int messageCount = 20;

        // Produce messages
        try (KafkaProducer<String, String> producer = createProducer()) {
            for (int i = 0; i < messageCount; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key-" + i, "{\"index\": " + i + "}");
                producer.send(record).get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            }
            producer.flush();
        }

        // Create two consumers in the same group
        List<String> consumer1Messages = Collections.synchronizedList(new ArrayList<>());
        List<String> consumer2Messages = Collections.synchronizedList(new ArrayList<>());

        Thread t1 = new Thread(() -> {
            try (KafkaConsumer<String, String> consumer = createConsumer(groupId)) {
                consumer.subscribe(Collections.singletonList(topic));
                long deadline = System.currentTimeMillis() + (TIMEOUT_SECONDS * 1000);
                while (System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> record : records) {
                        consumer1Messages.add(record.value());
                    }
                }
            }
        });

        Thread t2 = new Thread(() -> {
            try (KafkaConsumer<String, String> consumer = createConsumer(groupId)) {
                consumer.subscribe(Collections.singletonList(topic));
                long deadline = System.currentTimeMillis() + (TIMEOUT_SECONDS * 1000);
                while (System.currentTimeMillis() < deadline) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                    for (ConsumerRecord<String, String> record : records) {
                        consumer2Messages.add(record.value());
                    }
                }
            }
        });

        t1.start();
        t2.start();
        t1.join();
        t2.join();

        int totalReceived = consumer1Messages.size() + consumer2Messages.size();
        assertEquals(messageCount, totalReceived, "All messages should be consumed across the group");
        System.out.println("Consumer Group: Messages distributed - Consumer1: " + consumer1Messages.size() + ", Consumer2: " + consumer2Messages.size());
    }

    // ==================== Helper Methods ====================

    private KafkaProducer<String, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(props);
    }

    private KafkaConsumer<String, String> createConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        return new KafkaConsumer<>(props);
    }

    private KafkaConsumer<String, String> createConsumerManualCommit(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(props);
    }
}
