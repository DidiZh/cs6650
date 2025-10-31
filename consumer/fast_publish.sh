#!/bin/bash

TOTAL=${1:-100000}
BATCH_SIZE=1000

echo "Fast publishing $TOTAL messages in batches of $BATCH_SIZE"
START=$(date +%s)

for batch_start in $(seq 1 $BATCH_SIZE $TOTAL); do
  batch_end=$((batch_start + BATCH_SIZE - 1))
  if [ $batch_end -gt $TOTAL ]; then
    batch_end=$TOTAL
  fi
  
  # 生成一个批次的消息
  {
    for i in $(seq $batch_start $batch_end); do
      room=$((i % 20 + 1))
      echo "exchange=chat.exchange"
      echo "routing_key=room.$room"
      echo "payload={\"messageId\":\"$i\",\"roomId\":\"$room\",\"userId\":\"123\",\"username\":\"user\",\"message\":\"test\",\"timestamp\":\"2025-10-31T16:00:00Z\",\"messageType\":\"TEXT\"}"
      echo ""
    done
  } | docker exec -i rabbit rabbitmqadmin -u guest -p guest publish batch >/dev/null 2>&1
  
  if [ $((batch_start % 10000)) -eq 1 ]; then
    CURRENT=$(curl -s -u guest:guest http://localhost:15672/api/overview | jq '.queue_totals.messages_ready + .queue_totals.messages_unacknowledged')
    echo "[$batch_start/$TOTAL] Queue depth: $CURRENT"
  fi
done

END=$(date +%s)
DURATION=$((END - START))
RATE=$((TOTAL / DURATION))

echo ""
echo "Sent $TOTAL messages in ${DURATION}s (~${RATE} msg/s)"
