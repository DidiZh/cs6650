# Client Part 2 — Concurrent Load & Metrics

## Overview
A concurrent WebSocket load client with metrics.  
Outputs to `../results`:
- `main_summary.csv` — overall metrics (p50/p95/p99/p999/mean/avg_inflight, TPS, totals)
- `detail.csv` — per-ack latency records
- `throughput_10s.csv` — acks per 10-second bucket

## Requirements
- JDK 17
- Maven 3.9+

## Build
mvn -q -DskipTests clean package

## Run & Quick Verify
# Warmup (local): 4 threads, 2,000 total, 5 rooms
java -cp target/client-part2-0.0.1-SNAPSHOT-shaded.jar \
com.chatflow.client2.app.MainApp ws://localhost:8080/chat 4 2000 5 5 100

# Main (local): 16 threads, 20,000 total, 10 rooms
java -cp target/client-part2-0.0.1-SNAPSHOT-shaded.jar \
com.chatflow.client2.app.MainApp ws://localhost:8080/chat 16 20000 10 5 100

# Check summary CSV (last two rows)
column -t -s, ../results/main_summary.csv | tail -n 2
