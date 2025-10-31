#!/bin/bash

# 创建发送脚本
cat > /tmp/send_message.sh << 'EOF'
#!/bin/bash
MSG_ID=$1
ROOM=$((MSG_ID % 20 + 1))
docker exec rabbit rabbitmqadmin -u guest -p guest publish \
  exchange=chat.exchange routing_key=room.$ROOM \
  payload='{"messageId":"'$MSG_ID'","roomId":"'$ROOM'","userId":"123","username":"user","message":"test","timestamp":"2025-10-31T15:20:00Z","messageType":"TEXT"}' \
  payload_encoding=string >/dev/null 2>&1
EOF
chmod +x /tmp/send_message.sh

echo "=========================================="
echo "  500K Message Performance Test"
echo "  Start: $(date)"
echo "=========================================="

START_TIME=$(date +%s)

for batch in {1..5}; do
  BATCH_START=$(date +%s)
  start=$(( (batch-1) * 100000 + 1 ))
  end=$(( batch * 100000 ))
  
  echo "[$batch/5] Sending $start to $end..."
  seq $start $end | xargs -n1 -P64 /tmp/send_message.sh
  
  BATCH_END=$(date +%s)
  BATCH_DURATION=$((BATCH_END - BATCH_START))
  BATCH_RATE=$((100000 / BATCH_DURATION))
  
  echo "  Batch $batch: ${BATCH_DURATION}s (~${BATCH_RATE} msg/s)"
  
  REMAINING=$(curl -s -u guest:guest http://localhost:15672/api/overview | jq '.queue_totals.messages_ready + .queue_totals.messages_unacknowledged')
  echo "  Queue depth: $REMAINING"
  
  [ $batch -lt 5 ] && sleep 10
done

SEND_END=$(date +%s)
SEND_DURATION=$((SEND_END - START_TIME))
SEND_RATE=$((500000 / SEND_DURATION))

echo ""
echo "All sent! Time: ${SEND_DURATION}s (${SEND_RATE} msg/s)"
echo "Waiting for consumption..."

for i in {1..60}; do
  STATS=$(curl -s -u guest:guest http://localhost:15672/api/overview)
  REMAINING=$(echo $STATS | jq '.queue_totals.messages_ready + .queue_totals.messages_unacknowledged')
  ACK_RATE=$(echo $STATS | jq '.message_stats.ack_details.rate // 0')
  
  echo "[$(date +%H:%M:%S)] Remaining: $REMAINING | ACK: $ACK_RATE/s"
  
  [ "$REMAINING" -eq 0 ] && break
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
echo "=========================================="

echo "messages,send_time,consume_time,total_time,throughput" > results_500k_single.csv
echo "500000,$SEND_DURATION,$CONSUME_DURATION,$TOTAL_DURATION,$THROUGHPUT" >> results_500k_single.csv
echo "Results saved to: results_500k_single.csv"
