#!/bin/bash
echo "Starting load test..."
START=$(date +%s)

for i in $(seq 1 2000); do
  docker exec rabbit rabbitmqadmin -u guest -p guest publish \
    exchange=chat.exchange routing_key=room.1 \
    payload='{"messageId":"'$i'","roomId":"1","userId":"123","username":"user","message":"msg","timestamp":"2025-10-30T23:00:00Z","messageType":"TEXT"}' \
    payload_encoding=string >/dev/null 2>&1 &
  
  if [ $((i % 100)) -eq 0 ]; then
    wait
    echo "Sent $i messages"
  fi
done

wait
END=$(date +%s)
DURATION=$((END - START))
echo "All done! Time: ${DURATION}s"
