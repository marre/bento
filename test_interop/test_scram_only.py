#!/usr/bin/env python3
"""SASL SCRAM test."""
import json
from confluent_kafka import Producer

def main():
    print("Testing SASL SCRAM-SHA-256 on port 19402")

    config = {
        "bootstrap.servers": "127.0.0.1:19402",
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "SCRAM-SHA-256",
        "sasl.username": "scramuser",
        "sasl.password": "scrampass",
        "socket.timeout.ms": 10000,
        "message.timeout.ms": 10000,
    }

    producer = Producer(config)
    msg = json.dumps({"test": "sasl_scram", "user": "scramuser"}).encode()

    delivered = [False]
    error = [None]

    def callback(err, m):
        if err:
            error[0] = err
            print(f"Error: {err}")
        else:
            delivered[0] = True
            print(f"Delivered to {m.topic()} [{m.partition()}]")

    producer.produce("sasl-scram-topic", value=msg, callback=callback)
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
