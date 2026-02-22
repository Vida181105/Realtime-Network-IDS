#!/usr/bin/env python3
"""
kafka_producer.py
-----------------
Reads the preprocessed test split (parquet) and streams records to Kafka,
simulating real-time network traffic ingestion.

No Spark — uses pandas directly so it doesn't compete with the streaming
consumer for Spark worker cores.

Copy test split from HDFS once:
    docker exec -it namenode bash -c "hdfs dfs -get \
      /user/spark/ids/processed/splits_stratified/test \
      /tmp/test_split"
    docker cp namenode:/tmp/test_split ./workspace/dataset/test_split

Usage:
    python3 /opt/work/scripts/kafka_producer.py
    python3 /opt/work/scripts/kafka_producer.py --delay 0 --max-records 10000
"""

import time
import json
import argparse
import glob
import random

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_SERVERS  = "kafka:9092"
KAFKA_TOPIC    = "network-traffic"
LOCAL_TEST_DIR = "/opt/work/dataset/test_split"
LABEL_COL      = "label_idx"

# Correct mapping from label_mapping CSV
ATTACK_NAMES = {
    0:  "Benign",
    1:  "DDOS attack-HOIC",
    2:  "DDoS attacks-LOIC-HTTP",
    3:  "DoS attacks-Hulk",
    4:  "Bot",
    5:  "FTP-BruteForce",
    6:  "SSH-Bruteforce",
    7:  "Infilteration",
    8:  "DoS attacks-SlowHTTPTest",
    9:  "DoS attacks-GoldenEye",
    10: "DoS attacks-Slowloris",
    11: "DDOS attack-LOIC-UDP",
    12: "Brute Force -Web",
    13: "Brute Force -XSS",
    14: "SQL Injection",
}


def main(delay: float, max_records: int):
    parquet_files = sorted(
        glob.glob(f"{LOCAL_TEST_DIR}/**/*.parquet", recursive=True) +
        glob.glob(f"{LOCAL_TEST_DIR}/*.parquet")
    )
    random.shuffle(parquet_files)
    if not parquet_files:
        print(f"ERROR: No parquet files found at {LOCAL_TEST_DIR}")
        print("Copy from HDFS first — see script header.")
        return

    print(f"Found {len(parquet_files)} parquet file(s)")

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks=1,
        retries=3,
        linger_ms=10,
    )
    rate_str = f"~{1/delay:.0f} rec/s" if delay > 0 else "max speed"
    print(f"Connected to Kafka  : {KAFKA_SERVERS}")
    print(f"Topic               : {KAFKA_TOPIC}")
    print(f"Delay               : {delay}s ({rate_str})")
    print(f"Max records         : {max_records if max_records > 0 else 'all'}")
    print()

    sent    = 0
    failed  = 0
    t_start = time.time()

    for pf in parquet_files:
        df = pd.read_parquet(pf)

        for _, row in df.iterrows():
            if max_records > 0 and sent >= max_records:
                break

            record = {}
            for k, v in row.items():
                if pd.isna(v):
                    record[k] = None
                elif hasattr(v, "item"):
                    record[k] = v.item()
                else:
                    record[k] = v

            # Fix: explicitly cast label_idx to int — pandas reads as float
            # which JSON serialises as NaN for some rows
            if LABEL_COL in record and record[LABEL_COL] is not None:
                record[LABEL_COL] = int(record[LABEL_COL])

            record["label_name"] = ATTACK_NAMES.get(record.get(LABEL_COL), "Unknown")

            try:
                producer.send(KAFKA_TOPIC, value=record)
                sent += 1
            except KafkaError as e:
                print(f"  Send error: {e}")
                failed += 1

            if sent % 5000 == 0 and sent > 0:
                elapsed = time.time() - t_start
                print(f"  Sent: {sent:>8,}  |  Failed: {failed}  |  "
                      f"Rate: {sent/elapsed:.0f} rec/s")

            if delay > 0:
                time.sleep(delay)

        if max_records > 0 and sent >= max_records:
            break

    producer.flush()
    producer.close()

    elapsed = time.time() - t_start
    print(f"\nDone.")
    print(f"  Sent  : {sent:,}")
    print(f"  Failed: {failed}")
    print(f"  Time  : {elapsed:.1f}s")
    print(f"  Rate  : {sent/elapsed:.0f} rec/s")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay",       type=float, default=0.005)
    parser.add_argument("--max-records", type=int,   default=0)
    args = parser.parse_args()
    main(args.delay, args.max_records)