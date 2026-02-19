# Real-Time Network Intrusion Detection System
### Apache Spark + Deep Learning on CSE-CIC-IDS2018

Replication and extension of *"Apache Spark and Deep Learning Models for High-Performance Network Intrusion Detection Using CSE-CIC-IDS2018"*.

**Novelty addition:** Real-time detection pipeline using Apache Kafka + Spark Structured Streaming.

---

## Stack

| Service | Purpose |
|---|---|
| HDFS (namenode + datanode) | Distributed storage for 6.89GB dataset |
| Apache Spark (standalone) | Distributed ML training |
| Apache Kafka (KRaft mode) | Real-time stream ingestion |
| MongoDB | Results storage |
| JupyterLab | Development environment |

All services run in Docker on a single machine (developed on MacBook M3 Pro, 8GB RAM).

---

## Dataset

**CSE-CIC-IDS2018** — 16.23M rows, 84 features, 14 attack types + benign traffic.

Download from: https://www.unb.ca/cic/datasets/ids-2018.html

Place CSV files in `workspace/dataset/raw/` before loading to HDFS.

---

## Web UIs

| UI | URL |
|---|---|
| JupyterLab | http://localhost:8888 |
| HDFS NameNode | http://localhost:9870 |
| Spark Master | http://localhost:8080 |
| Spark Jobs | http://localhost:4040 (active during jobs) |

---

## Notebooks

| Notebook | Description |
|---|---|
| `batch_preprocessing.ipynb` | Full preprocessing pipeline: cleaning → encoding → balancing → stratified splits → RF feature selection |

---

## Pipeline

```
Raw CSVs (HDFS)
    → Clean (drop inf, fix nulls, remove bad rows)
    → Encode (StringIndexer on Label)
    → Balance (undersample Benign to reduce class imbalance)
    → Stratified Train/Val/Test split (70/15/15)
    → RF Feature Importance (200 trees, top features selected)
    → Parquet (saved back to HDFS)
```

---

## Key Findings (Preprocessing)

Top features by Random Forest importance:
1. Init Fwd Win Byts (0.121)
2. Fwd Seg Size Min (0.058)
3. Timestamp Unix (0.057)
4. Dst Port (0.054)
5. Subflow Fwd Byts (0.045)

---
