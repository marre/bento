#!/usr/bin/env python3
"""Focused compression test."""
import json
import time
from kafka import KafkaProducer

PORT = 19300

def test_compression():
    """Test all compression codecs."""
    print("Compression Test on port", PORT)
    print("=" * 50)

    codecs = ["gzip", "snappy", "lz4"]
    results = {}

    for codec in codecs:
        print(f"\nTesting {codec}...")
        try:
            producer = KafkaProducer(
                bootstrap_servers=f"127.0.0.1:{PORT}",
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                api_version=(2, 5, 0),
                compression_type=codec,
                linger_ms=10,  # Allow batching
            )

            # Send multiple messages
            for i in range(3):
                future = producer.send(
                    f"compressed-{codec}",
                    key=f"{codec}-{i}".encode(),
                    value={"codec": codec, "idx": i, "data": "x" * 500},
                )
                result = future.get(timeout=10)
                print(f"  Sent {codec} message {i}: partition={result.partition}")

            producer.flush()
            producer.close()
            results[codec] = "PASS"
            print(f"  {codec}: OK")

        except Exception as e:
            results[codec] = f"FAIL: {e}"
            print(f"  {codec}: FAILED - {e}")

    print("\n" + "=" * 50)
    print("SUMMARY:")
    for codec, status in results.items():
        print(f"  {codec}: {status}")

    return all("PASS" in str(v) for v in results.values())

if __name__ == "__main__":
    time.sleep(1)
    success = test_compression()
    exit(0 if success else 1)
