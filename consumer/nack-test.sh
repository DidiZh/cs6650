#!/bin/bash
echo "Sending 10 test messages that will fail..."
for i in $(seq 1 10); do
  docker exec rabbit rabbitmqadmin -u guest -p guest publish \
    exchange=chat.exchange routing_key=room.1 \
    payload='{"messageId":"fail'$i'","roomId":"1","userId":"123","username":"testuser","message":"This will fail","timestamp":"2025-10-30T23:00:00Z","messageType":"TEXT"}' \
    payload_encoding=string
  echo "Sent message $i"
  sleep 0.5
done
echo "Done sending messages"
