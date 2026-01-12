#!/usr/bin/env python3
"""
SASL authentication test using confluent-kafka library.
"""

import json
import time
import sys
from confluent_kafka import Producer, KafkaException


def delivery_callback(err, msg):
    """Delivery report callback."""
    if err:
        print(f"  Delivery failed: {err}")
    else:
        print(f"  Delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")


def test_sasl_plain(port, username, password):
    """Test SASL PLAIN authentication."""
    print(f"\nTesting SASL PLAIN on port {port}")
    print(f"  Username: {username}")
    print("=" * 50)

    try:
        config = {
            "bootstrap.servers": f"127.0.0.1:{port}",
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "PLAIN",
            "sasl.username": username,
            "sasl.password": password,
            "socket.timeout.ms": 10000,
            "message.timeout.ms": 10000,
        }

        producer = Producer(config)

        # Send test message
        msg = json.dumps({"test": "sasl_plain", "user": username}).encode()

        delivered = [False]
        error = [None]

        def callback(err, m):
            if err:
                error[0] = err
                print(f"  Error: {err}")
            else:
                delivered[0] = True
                print(f"  Delivered to {m.topic()} [{m.partition()}]")

        producer.produce("sasl-test-topic", value=msg, callback=callback)
        producer.flush(timeout=10)

        if error[0]:
            print(f"FAILED: {error[0]}")
            return False
        if not delivered[0]:
            print("FAILED: Message not delivered")
            return False

        print("SUCCESS: SASL PLAIN test passed")
        return True

    except Exception as e:
        print(f"FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_sasl_scram(port, username, password, mechanism="SCRAM-SHA-256"):
    """Test SASL SCRAM authentication."""
    print(f"\nTesting SASL {mechanism} on port {port}")
    print(f"  Username: {username}")
    print("=" * 50)

    try:
        config = {
            "bootstrap.servers": f"127.0.0.1:{port}",
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": mechanism,
            "sasl.username": username,
            "sasl.password": password,
            "socket.timeout.ms": 10000,
            "message.timeout.ms": 10000,
        }

        producer = Producer(config)

        # Send test message
        msg = json.dumps({"test": f"sasl_{mechanism.lower()}", "user": username}).encode()

        delivered = [False]
        error = [None]

        def callback(err, m):
            if err:
                error[0] = err
                print(f"  Error: {err}")
            else:
                delivered[0] = True
                print(f"  Delivered to {m.topic()} [{m.partition()}]")

        producer.produce("sasl-scram-topic", value=msg, callback=callback)
        producer.flush(timeout=10)

        if error[0]:
            print(f"FAILED: {error[0]}")
            return False
        if not delivered[0]:
            print("FAILED: Message not delivered")
            return False

        print(f"SUCCESS: SASL {mechanism} test passed")
        return True

    except Exception as e:
        print(f"FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_wrong_password(port, username, wrong_password):
    """Test that wrong password is rejected."""
    print(f"\nTesting wrong password rejection on port {port}")
    print("=" * 50)

    try:
        config = {
            "bootstrap.servers": f"127.0.0.1:{port}",
            "security.protocol": "SASL_PLAINTEXT",
            "sasl.mechanism": "PLAIN",
            "sasl.username": username,
            "sasl.password": wrong_password,
            "socket.timeout.ms": 5000,
            "message.timeout.ms": 5000,
        }

        producer = Producer(config)
        msg = b"should not be delivered"

        error = [None]

        def callback(err, m):
            if err:
                error[0] = err

        producer.produce("test-topic", value=msg, callback=callback)
        producer.flush(timeout=5)

        if error[0]:
            print(f"EXPECTED: Got error: {error[0]}")
            print("SUCCESS: Wrong password correctly rejected")
            return True
        else:
            print("UNEXPECTED: Message was delivered with wrong password")
            return False

    except Exception as e:
        print(f"EXPECTED: Got exception: {e}")
        print("SUCCESS: Wrong password correctly rejected")
        return True


def main():
    print("SASL Authentication Tests using confluent-kafka")
    print("=" * 50)

    time.sleep(1)

    results = {}

    # Test SASL PLAIN (using port 19401)
    results["sasl_plain"] = test_sasl_plain(19401, "testuser", "testpass")

    # Test wrong password
    results["wrong_password"] = test_wrong_password(19401, "testuser", "wrongpass")

    # Test SASL SCRAM-SHA-256 (using port 19402)
    results["sasl_scram256"] = test_sasl_scram(19402, "scramuser", "scrampass", "SCRAM-SHA-256")

    # Summary
    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)
    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for name, result in results.items():
        status = "PASS" if result else "FAIL"
        print(f"  {name}: {status}")

    print(f"\nTotal: {passed}/{total} tests passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    sys.exit(main())
