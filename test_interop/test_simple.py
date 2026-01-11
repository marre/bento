#!/usr/bin/env python3
"""
Simple interoperability test for Bento kafka_server input.
Uses kafka-python-ng with explicit API version to ensure proper protocol.
"""

import json
import time
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


def test_no_auth():
    """Test basic connectivity without authentication."""
    print("\n" + "=" * 60)
    print("TEST: No Authentication")
    print("=" * 60)

    try:
        # Use explicit API version to ensure proper protocol flow
        producer = KafkaProducer(
            bootstrap_servers="127.0.0.1:19200",
            value_serializer=lambda v: json.dumps(v).encode("utf-8") if isinstance(v, dict) else v,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            request_timeout_ms=10000,
            api_version=(2, 5, 0),  # Kafka 2.5.0 API
        )

        # Send test messages
        messages = [
            {"test": "noauth_1", "source": "python"},
            {"test": "noauth_2", "source": "python"},
            {"test": "noauth_3", "source": "python"},
        ]

        for i, msg in enumerate(messages):
            future = producer.send(
                "test-topic",
                key=f"key-{i}",
                value=msg,
                headers=[("source", b"python"), ("idx", str(i).encode())],
            )
            result = future.get(timeout=10)
            print(f"  Sent message {i+1}: topic={result.topic}, partition={result.partition}")

        producer.flush()
        producer.close()
        print("SUCCESS: No auth test passed")
        return True

    except Exception as e:
        print(f"FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_multiple_topics():
    """Test sending to multiple topics."""
    print("\n" + "=" * 60)
    print("TEST: Multiple Topics")
    print("=" * 60)

    try:
        producer = KafkaProducer(
            bootstrap_servers="127.0.0.1:19200",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            api_version=(2, 5, 0),
        )

        topics = ["topic-a", "topic-b", "topic-c"]
        for topic in topics:
            future = producer.send(topic, value={"topic": topic})
            result = future.get(timeout=10)
            print(f"  Sent to {topic}: partition={result.partition}")

        producer.flush()
        producer.close()
        print("SUCCESS: Multiple topics test passed")
        return True

    except Exception as e:
        print(f"FAILED: {e}")
        return False


def test_large_message():
    """Test sending a larger message."""
    print("\n" + "=" * 60)
    print("TEST: Large Message (~100KB)")
    print("=" * 60)

    try:
        producer = KafkaProducer(
            bootstrap_servers="127.0.0.1:19200",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            api_version=(2, 5, 0),
            max_request_size=1048576,
        )

        large_data = {"test": "large", "data": "x" * 100000}
        future = producer.send("large-topic", value=large_data)
        result = future.get(timeout=10)
        print(f"  Sent large message: topic={result.topic}")

        producer.flush()
        producer.close()
        print("SUCCESS: Large message test passed")
        return True

    except Exception as e:
        print(f"FAILED: {e}")
        return False


def test_null_key_and_tombstone():
    """Test null key and tombstone (null value)."""
    print("\n" + "=" * 60)
    print("TEST: Null Key and Tombstone")
    print("=" * 60)

    try:
        producer = KafkaProducer(
            bootstrap_servers="127.0.0.1:19200",
            value_serializer=lambda v: json.dumps(v).encode("utf-8") if v else None,
            api_version=(2, 5, 0),
        )

        # Null key
        future = producer.send("test-topic", key=None, value={"test": "null_key"})
        result = future.get(timeout=10)
        print(f"  Sent with null key: topic={result.topic}")

        # Tombstone (null value)
        future = producer.send("test-topic", key=b"tombstone-key", value=None)
        result = future.get(timeout=10)
        print(f"  Sent tombstone: topic={result.topic}")

        producer.flush()
        producer.close()
        print("SUCCESS: Null key and tombstone test passed")
        return True

    except Exception as e:
        print(f"FAILED: {e}")
        return False


def test_compression():
    """Test with different compression codecs."""
    print("\n" + "=" * 60)
    print("TEST: Compression (gzip, snappy, lz4)")
    print("=" * 60)

    codecs = ["gzip", "snappy", "lz4"]
    success = True

    for codec in codecs:
        try:
            producer = KafkaProducer(
                bootstrap_servers="127.0.0.1:19200",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version=(2, 5, 0),
                compression_type=codec,
            )

            # Send multiple messages to create a batch (compression works on batches)
            for i in range(3):
                future = producer.send(
                    f"compressed-{codec}",
                    value={"test": f"compressed_{codec}", "data": "x" * 1000, "idx": i},
                )
                result = future.get(timeout=10)

            producer.flush()
            producer.close()
            print(f"  {codec}: OK")

        except Exception as e:
            print(f"  {codec}: FAILED - {e}")
            success = False

    if success:
        print("SUCCESS: Compression test passed")
    else:
        print("PARTIAL: Some compression tests failed")
    return success


def test_batch_messages():
    """Test sending a batch of messages."""
    print("\n" + "=" * 60)
    print("TEST: Batch Messages (20 messages)")
    print("=" * 60)

    try:
        producer = KafkaProducer(
            bootstrap_servers="127.0.0.1:19200",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            api_version=(2, 5, 0),
            linger_ms=100,  # Allow batching
            batch_size=16384,
        )

        futures = []
        for i in range(20):
            future = producer.send(
                "batch-topic",
                key=f"batch-{i}".encode(),
                value={"batch_idx": i, "test": "batch"},
            )
            futures.append(future)

        # Wait for all
        for i, f in enumerate(futures):
            result = f.get(timeout=10)

        producer.flush()
        producer.close()
        print(f"  Sent 20 messages successfully")
        print("SUCCESS: Batch messages test passed")
        return True

    except Exception as e:
        print(f"FAILED: {e}")
        return False


def main():
    """Run all tests."""
    print("Bento kafka_server Interoperability Tests")
    print("Using kafka-python-ng with explicit API version")
    print("=" * 60)

    print("\nWaiting 1 second for servers...")
    time.sleep(1)

    results = {}

    results["no_auth"] = test_no_auth()
    results["multiple_topics"] = test_multiple_topics()
    results["large_message"] = test_large_message()
    results["null_key_tombstone"] = test_null_key_and_tombstone()
    results["compression"] = test_compression()
    results["batch"] = test_batch_messages()

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for name, result in results.items():
        status = "PASS" if result else "FAIL"
        print(f"  {name}: {status}")

    print(f"\nTotal: {passed}/{total} tests passed")

    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
