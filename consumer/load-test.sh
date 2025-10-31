#!/bin/bash

echo "=========================================="
echo "  RabbitMQ Load Test"
echo "=========================================="
echo ""

# 配置
TOTAL_MESSAGES=3000
BATCH_SIZE=100
PARALLEL_SENDERS=20

echo "Configuration:"
echo "  Total Messages: $TOTAL_MESSAGES"
echo "  Batch Size: $BATCH_SIZE"
echo "  Parallel Senders: $PARALLEL_SENDERS"
echo ""

# 计算批次数
TOTAL_BATCHES=$((TOTAL_MESSAGES / BATCH_SIZE))

echo "Starting test..."
echo "Time: $(date +%H:%M:%S)"
echo ""

START_TIME=$(date +%s)

# 并发发送
for batch_num in $(seq 1 $TOTAL_BATCHES); do
  for sender in $(seq 1 $PARALLEL_SENDERS); do
    {
      start_id=$((($batch_num - 1) * $BATCH_SIZE + ($sender - 1) * ($BATCH_SIZE / $PARALLEL_SENDERS) + 1))
      end_id=$(($start_id + ($BATCH_SIZE / $PARALLEL_SENDERS) - 1))
      
      for msg_id in $(seq $start_id $end_id); do
        docker exec rabbit rabbitmqadmin -u guest -p guest publish \
          exchange=chat.exchange routing_key=room.1 \
          payload="{\"messageId\":\"msg$msg_id\",\"roomId\":\"1\",\"userId\":\"$((msg_id % 1000))\",\"username\":\"user$msg_id\",\"message\":\"Load test message $msg_id\",\"timestamp\":\"2025-10-30T23:00:00Z\",\"messageType\":\"TEXT\"}" \
          payload_encoding=string >/dev/null 2>&1
      done
    } &
  done
  
  # 等待这一批完成
  wait
  
  messages_sent=$(($batch_num * $BATCH_SIZE))
  progress=$((messages_sent * 100 / TOTAL_MESSAGES))
  printf "[%s] Progress: %d%% (%d/%d messages)\n" \
         "$(date +%H:%M:%S)" "$progress" "$messages_sent" "$TOTAL_MESSAGES"
done

END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))

echo ""
echo "=========================================="
echo "Test completed!"
echo "  Total time: ${DURATION} seconds"
echo "  Messages sent: $TOTAL_MESSAGES"
echo "  Avg rate: $((TOTAL_MESSAGES / DURATION)) msg/sec"
echo "=========================================="
