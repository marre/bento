#!/usr/bin/env python3
"""
Interoperability test for Bento kafka_server input.

Tests:
1. No authentication - basic connectivity
2. SASL PLAIN authentication
3. SASL SCRAM-SHA-256 authentication
4. Invalid credentials (should fail)
5. Multiple messages and message headers
"""

import json
import time
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable


def create_producer(bootstrap_servers, sasl_mechanism=None, username=None, password=None):
    """Create a Kafka producer with optional SASL authentication."""
    config = {
        "bootstrap_servers": bootstrap_servers,
        "value_serializer": lambda v: json.dumps(v).encode("utf-8") if isinstance(v, dict) else v,
        "key_serializer": lambda k: k.encode("utf-8") if k else None,
        "request_timeout_ms": 10000,
        "api_version_auto_timeout_ms": 10000,
        "metadata_max_age_ms": 5000,
    }

    if sasl_mechanism:
        config["security_protocol"] = "SASL_PLAINTEXT"
        config["sasl_mechanism"] = sasl_mechanism
        config["sasl_plain_username"] = username
        config["sasl_plain_password"] = password

    return KafkaProducer(**config)


def test_no_auth():
    """Test basic connectivity without authentication."""
    print("\n" + "=" * 60)
    print("TEST: No Authentication")
    print("=" * 60)

    try:
        producer = create_producer("127.0.0.1:19200")

        # Send test messages
        messages = [
            {"test": "noauth_message_1", "value": 100},
            {"test": "noauth_message_2", "value": 200},
            {"test": "noauth_message_3", "value": 300},
        ]

        for i, msg in enumerate(messages):
            future = producer.send(
                "test-topic",
                key=f"key-{i}",
                value=msg,
                headers=[("source", b"python-test"), ("index", str(i).encode())],
            )
            result = future.get(timeout=10)
            print(f"  Sent message {i+1}: topic={result.topic}, partition={result.partition}")

        producer.flush()
        producer.close()
        print("SUCCESS: No auth test passed")
        return True

    except Exception as e:
        print(f"FAILED: {e}")
        return False


def test_sasl_plain():
    """Test SASL PLAIN authentication."""
    print("\n" + "=" * 60)
    print("TEST: SASL PLAIN Authentication")
    print("=" * 60)

    try:
        producer = create_producer(
            "127.0.0.1:19201",
            sasl_mechanism="PLAIN",
            username="testuser",
            password="testpass",
        )

        # Send test messages
        messages = [
            {"test": "plain_auth_message_1", "user": "testuser"},
            {"test": "plain_auth_message_2", "user": "testuser"},
        ]

        for i, msg in enumerate(messages):
            future = producer.send(
                "authenticated-topic",
                key=f"plain-key-{i}",
                value=msg,
                headers=[("auth", b"PLAIN"), ("user", b"testuser")],
            )
            result = future.get(timeout=10)
            print(f"  Sent message {i+1}: topic={result.topic}, partition={result.partition}")

        producer.flush()
        producer.close()
        print("SUCCESS: SASL PLAIN test passed")
        return True

    except Exception as e:
        print(f"FAILED: {e}")
        return False


def test_sasl_plain_wrong_password():
    """Test SASL PLAIN with wrong password (should fail)."""
    print("\n" + "=" * 60)
    print("TEST: SASL PLAIN Wrong Password (expect failure)")
    print("=" * 60)

    try:
        producer = create_producer(
            "127.0.0.1:19201",
            sasl_mechanism="PLAIN",
            username="testuser",
            password="wrongpassword",
        )

        future = producer.send("test-topic", value=b"should not work")
        result = future.get(timeout=10)
        producer.close()
        print("UNEXPECTED: Message was sent (should have failed)")
        return False

    except Exception as e:
        print(f"EXPECTED FAILURE: {e}")
        print("SUCCESS: Wrong password correctly rejected")
        return True


def test_sasl_scram():
    """Test SASL SCRAM-SHA-256 authentication."""
    print("\n" + "=" * 60)
    print("TEST: SASL SCRAM-SHA-256 Authentication")
    print("=" * 60)

    try:
        producer = create_producer(
            "127.0.0.1:19202",
            sasl_mechanism="SCRAM-SHA-256",
            username="scramuser",
            password="scrampass",
        )

        # Send test messages
        messages = [
            {"test": "scram_message_1", "mechanism": "SCRAM-SHA-256"},
            {"test": "scram_message_2", "mechanism": "SCRAM-SHA-256"},
        ]

        for i, msg in enumerate(messages):
            future = producer.send(
                "scram-topic",
                key=f"scram-key-{i}",
                value=msg,
                headers=[("auth", b"SCRAM-SHA-256")],
            )
            result = future.get(timeout=10)
            print(f"  Sent message {i+1}: topic={result.topic}, partition={result.partition}")

        producer.flush()
        producer.close()
        print("SUCCESS: SASL SCRAM-SHA-256 test passed")
        return True

    except Exception as e:
        print(f"FAILED: {e}")
        return False


def test_multiple_topics():
    """Test sending to multiple topics."""
    print("\n" + "=" * 60)
    print("TEST: Multiple Topics")
    print("=" * 60)

    try:
        producer = create_producer("127.0.0.1:19200")

        topics = ["topic-a", "topic-b", "topic-c"]
        for topic in topics:
            future = producer.send(
                topic,
                key="multi-topic-key",
                value={"topic": topic, "test": "multi_topic"},
            )
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
    print("TEST: Large Message")
    print("=" * 60)

    try:
        producer = create_producer("127.0.0.1:19200")

        # Create a ~100KB message
        large_data = {
            "test": "large_message",
            "data": "x" * 100000,
            "size": "~100KB",
        }

        future = producer.send("large-topic", key="large-key", value=large_data)
        result = future.get(timeout=10)
        print(f"  Sent large message: topic={result.topic}, partition={result.partition}")

        producer.flush()
        producer.close()
        print("SUCCESS: Large message test passed")
        return True

    except Exception as e:
        print(f"FAILED: {e}")
        return False


def test_null_key():
    """Test sending message with null key."""
    print("\n" + "=" * 60)
    print("TEST: Null Key")
    print("=" * 60)

    try:
        producer = create_producer("127.0.0.1:19200")

        future = producer.send(
            "test-topic",
            key=None,
            value={"test": "null_key_message"},
        )
        result = future.get(timeout=10)
        print(f"  Sent message with null key: topic={result.topic}")

        producer.flush()
        producer.close()
        print("SUCCESS: Null key test passed")
        return True

    except Exception as e:
        print(f"FAILED: {e}")
        return False


def main():
    """Run all tests."""
    print("Bento kafka_server Interoperability Tests")
    print("Using kafka-python-ng library")
    print("=" * 60)

    # Give Bento servers time to start
    print("\nWaiting 2 seconds for Bento servers to be ready...")
    time.sleep(2)

    results = {}

    # Run tests
    results["no_auth"] = test_no_auth()
    results["sasl_plain"] = test_sasl_plain()
    results["sasl_plain_wrong_pw"] = test_sasl_plain_wrong_password()
    results["sasl_scram"] = test_sasl_scram()
    results["multiple_topics"] = test_multiple_topics()
    results["large_message"] = test_large_message()
    results["null_key"] = test_null_key()

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
