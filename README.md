# Real-Time Network Intrusion Detection System
### Apache Spark + Deep Learning on CSE-CIC-IDS2018

## Project Description

Network intrusion detection is a critical component of modern cybersecurity infrastructure. Traditional signature-based IDS systems struggle to detect novel attacks and do not scale well to high-throughput network environments. This project explores the use of distributed computing and machine learning to build a scalable, high-performance IDS capable of both batch and real-time detection.

Using the CSE-CIC-IDS2018 dataset — one of the most comprehensive publicly available network traffic datasets, containing over 16 million flow records across 14 attack categories — we train Spark MLlib Logistic Regression, CNN, and LSTM models using Apache Spark's distributed processing framework.

The core novelty of this project: a real-time detection layer is implemented using Apache Kafka as a message broker and Spark Structured Streaming as the consumer. Network flow records are streamed through Kafka topics, classified by the trained model in near real-time, and results are persisted to MongoDB for monitoring and analysis. This end-to-end architecture mirrors how an IDS would function in a production environment.

The entire stack runs on a single machine using Docker, demonstrating that distributed system concepts — HDFS for fault-tolerant storage, Spark for parallel in-memory computation, Kafka for decoupled stream ingestion — can be studied and validated without access to a multi-node cluster. All design decisions prioritize reproducibility and resource efficiency for constrained hardware environments.

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
| `batch_preprocessing.ipynb` | Full preprocessing pipeline: cleaning → outlier correction → balancing → encoding → stratified splits → RF feature selection |
| `SparkLR.ipynb` | Spark Logistic Regression — Model 1 of 3, with class weights, oversampling, and hyperparameter tuning |

---

## Pipeline

```
Raw CSVs (HDFS)
    → parquet_by_file       (per-file CSV → Parquet conversion)
    → parquet_merged        (all 10 files unioned)
    → parquet_clean         (drop inf/NaN, median imputation, remove bad label rows)
    → parquet_clean_clipped (outlier correction: 99.5th percentile cap, both tails)
    → parquet_balanced      (undersample Benign: 13.5M → 3M; oversample rare classes)
    → parquet_encoded       (StringIndexer: Label string → label_idx integer 0–14)
    → parquet_time          (Timestamp → timestamp_unix epoch seconds)
    → splits_stratified     (stratified 70/15/15 train/val/test split)
    → RF Feature Importance (200 trees → top 30 features selected)
```

---

## Key Findings (Preprocessing)

Top features by Random Forest importance (post outlier correction, `timestamp_unix` excluded from model training — see below):
1. Init Fwd Win Byts (0.123)
2. Dst Port (0.058)
3. Fwd Seg Size Min (0.055)
4. Fwd Pkt Len Max (0.047)
5. Fwd Header Len (0.042)

---

## Model Results

### Model 1 — Spark Logistic Regression (`SparkLR.ipynb`)

- `timestamp_unix` excluded — it encodes the day/time that specific attacks were captured, not any real network flow characteristic. A model relying on it would fail in deployment. Excluding it gives a more honest and generalisable baseline.
- Top 30 RF features used — extended feature set after outlier correction shifted importance scores.
- Class weights added — the dataset's extreme imbalance (Benign 2.1M vs SQL Injection 56 samples) causes LR to ignore minority classes without weighting.
- In-training oversampling — tiny classes (< 5,000 train samples) oversampled to 5,000 to provide sufficient gradient signal.
- Hyperparameters tuned via grid search: `regParam=0.01`, `elasticNetParam=0.5`, `maxIter=150`.

**Test set results:**

| Metric | Ours | Paper |
|--------|------|-------|
| Accuracy | 0.8487 | 0.999 |
| Weighted F1 | 0.8351 | ~0.99 |
| Weighted Precision | 0.8442 | ~1.00 |
| Weighted Recall | 0.8487 | ~1.00 |

**Per-class analysis:** 10/15 classes achieve recall > 0.70. Infiltration (recall 0.038) and web-based attacks (Brute Force-Web/XSS, SQL Injection) show poor recall due to linear inseparability from Benign traffic — these classes share overlapping flow feature distributions that a linear decision boundary cannot resolve. This directly motivates the CNN and LSTM models.