#!/bin/bash

echo "=========================================="
echo "  500K Message Test (Optimized)"
echo "  Start: $(date)"
echo "=========================================="

# 清空队列
echo "Clearing queues..."
for i in {1..20}; do
  docker exec rabbit rabbitmqadmin -u guest -p guest purge queue name=room.$i 2>/dev/null
done

echo ""
echo "Starting test..."
START_TIME=$(date +%s)

# 发送 500K 消息
./fast_publish.sh 500000

SEND_END=$(date +%s)
SEND_DURATION=$((SEND_END - START_TIME))
SEND_RATE=$((500000 / SEND_DURATION))

echo ""
echo "All 500,000 messages sent!"
echo "Send time: ${SEND_DURATION}s"
echo "Send rate: ${SEND_RATE} msg/s"
echo ""
echo "Waiting for queue to drain..."

# 等待队列清空
for i in {1..60}; do
  STATS=$(curl -s -u guest:guest http://localhost:15672/api/overview)
  REMAINING=$(echo $STATS | jq '.queue_totals.messages_ready + .queue_totals.messages_unacknowledged')
  ACK_RATE=$(echo $STATS | jq '.message_stats.ack_details.rate // 0')
  
  echo "[$(date +%H:%M:%S)] Remaining: $REMAINING | ACK rate: $ACK_RATE msg/s"
  
  if [ "$REMAINING" -eq 0 ]; then
    echo ""
    echo "✅ All messages consumed!"
    break
  fi
  
  sleep 5
done

FINAL_TIME=$(date +%s)
TOTAL_DURATION=$((FINAL_TIME - START_TIME))
CONSUME_DURATION=$((FINAL_TIME - SEND_END))
THROUGHPUT=$((500000 / TOTAL_DURATION))

echo ""
echo "=========================================="
echo "  FINAL RESULTS"
echo "=========================================="
echo "Total messages:     500,000"
echo "Send time:          ${SEND_DURATION}s (${SEND_RATE} msg/s)"
echo "Consume time:       ${CONSUME_DURATION}s"
echo "Total time:         ${TOTAL_DURATION}s"
echo "Overall throughput: ${THROUGHPUT} msg/s"
echo "Completed at:       $(date)"
echo "=========================================="

# 保存结果
echo "messages,send_time,send_rate,consume_time,total_time,throughput" > results_500k_optimized.csv
echo "500000,$SEND_DURATION,$SEND_RATE,$CONSUME_DURATION,$TOTAL_DURATION,$THROUGHPUT" >> results_500k_optimized.csv

echo ""
echo "Results saved to: results_500k_optimized.csv"
