#!/usr/bin/env python3
"""
SASL authentication test using kafka-python-ng library.
"""

import json
import time
import sys
from kafka import KafkaProducer
from kafka.errors import KafkaError


def test_sasl_plain():
    """Test SASL PLAIN authentication."""
    print("\n" + "=" * 60)
    print("TEST: SASL PLAIN Authentication")
    print("=" * 60)

    try:
        producer = KafkaProducer(
            bootstrap_servers="127.0.0.1:19401",
            value_serializer=lambda v: json.dumps(v).encode("utf-8") if isinstance(v, dict) else v,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            request_timeout_ms=10000,
            api_version=(2, 5, 0),
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username="testuser",
            sasl_plain_password="testpass",
        )

        # Send test messages
        messages = [
            {"test": "sasl_plain_1", "user": "testuser"},
            {"test": "sasl_plain_2", "user": "testuser"},
        ]

        for i, msg in enumerate(messages):
            future = producer.send(
                "authenticated-topic",
                key=f"plain-key-{i}",
                value=msg,
            )
            result = future.get(timeout=10)
            print(f"  Sent message {i+1}: topic={result.topic}, partition={result.partition}")

        producer.flush()
        producer.close()
        print("SUCCESS: SASL PLAIN test passed")
        return True

    except Exception as e:
        print(f"FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_sasl_plain_wrong_password():
    """Test SASL PLAIN with wrong password (should fail)."""
    print("\n" + "=" * 60)
    print("TEST: SASL PLAIN Wrong Password (expect failure)")
    print("=" * 60)

    try:
        producer = KafkaProducer(
            bootstrap_servers="127.0.0.1:19401",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5000,
            api_version=(2, 5, 0),
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_plain_username="testuser",
            sasl_plain_password="wrongpassword",
        )

        future = producer.send("test-topic", value={"test": "should_fail"})
        result = future.get(timeout=5)
        producer.close()
        print("UNEXPECTED: Message was sent (should have failed)")
        return False

    except Exception as e:
        print(f"EXPECTED FAILURE: {e}")
        print("SUCCESS: Wrong password correctly rejected")
        return True


def test_sasl_scram_sha256():
    """Test SASL SCRAM-SHA-256 authentication."""
    print("\n" + "=" * 60)
    print("TEST: SASL SCRAM-SHA-256 Authentication")
    print("=" * 60)

    try:
        producer = KafkaProducer(
            bootstrap_servers="127.0.0.1:19402",
            value_serializer=lambda v: json.dumps(v).encode("utf-8") if isinstance(v, dict) else v,
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            request_timeout_ms=10000,
            api_version=(2, 5, 0),
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username="scramuser",
            sasl_plain_password="scrampass",
        )

        # Send test messages
        messages = [
            {"test": "scram_sha256_1", "mechanism": "SCRAM-SHA-256"},
            {"test": "scram_sha256_2", "mechanism": "SCRAM-SHA-256"},
        ]

        for i, msg in enumerate(messages):
            future = producer.send(
                "scram-topic",
                key=f"scram-key-{i}",
                value=msg,
            )
            result = future.get(timeout=10)
            print(f"  Sent message {i+1}: topic={result.topic}, partition={result.partition}")

        producer.flush()
        producer.close()
        print("SUCCESS: SASL SCRAM-SHA-256 test passed")
        return True

    except Exception as e:
        print(f"FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_sasl_scram_wrong_password():
    """Test SASL SCRAM-SHA-256 with wrong password (should fail)."""
    print("\n" + "=" * 60)
    print("TEST: SASL SCRAM-SHA-256 Wrong Password (expect failure)")
    print("=" * 60)

    try:
        producer = KafkaProducer(
            bootstrap_servers="127.0.0.1:19402",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5000,
            api_version=(2, 5, 0),
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-256",
            sasl_plain_username="scramuser",
            sasl_plain_password="wrongpassword",
        )

        future = producer.send("test-topic", value={"test": "should_fail"})
        result = future.get(timeout=5)
        producer.close()
        print("UNEXPECTED: Message was sent (should have failed)")
        return False

    except Exception as e:
        print(f"EXPECTED FAILURE: {e}")
        print("SUCCESS: Wrong password correctly rejected")
        return True


def main():
    """Run all SASL tests."""
    print("Bento kafka_server SASL Authentication Tests")
    print("Using kafka-python-ng library")
    print("=" * 60)

    print("\nWaiting 1 second for servers...")
    time.sleep(1)

    results = {}

    results["sasl_plain"] = test_sasl_plain()
    results["sasl_plain_wrong_pw"] = test_sasl_plain_wrong_password()
    results["sasl_scram_sha256"] = test_sasl_scram_sha256()
    results["sasl_scram_wrong_pw"] = test_sasl_scram_wrong_password()

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
