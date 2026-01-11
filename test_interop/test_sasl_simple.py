#!/usr/bin/env python3
"""Simple SASL PLAIN test."""
import json
from confluent_kafka import Producer

def main():
    print("Testing SASL PLAIN on port 19401")

    config = {
        "bootstrap.servers": "127.0.0.1:19401",
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "testuser",
        "sasl.password": "testpass",
        "socket.timeout.ms": 10000,
        "message.timeout.ms": 10000,
        "debug": "all",
    }

    producer = Producer(config)
    msg = json.dumps({"test": "sasl_plain", "user": "testuser"}).encode()

    delivered = [False]
    error = [None]

    def callback(err, m):
        if err:
            error[0] = err
            print(f"Error: {err}")
        else:
            delivered[0] = True
            print(f"Delivered to {m.topic()} [{m.partition()}]")

    producer.produce("sasl-test-topic", value=msg, callback=callback)
    producer.flush(timeout=10)

    if error[0]:
        print(f"FAILED: {error[0]}")
        return 1
    if not delivered[0]:
        print("FAILED: Message not delivered")
        return 1

    print("SUCCESS")
    return 0

if __name__ == "__main__":
    exit(main())
