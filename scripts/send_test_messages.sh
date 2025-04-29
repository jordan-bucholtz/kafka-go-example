#!/bin/bash

# Script to send test messages to Kafka for the Shovel service
# Usage: ./scripts/send_test_messages.sh [number_of_messages]

# Set default number of messages
NUM_MESSAGES=${1:-5}
TOPIC="test-topic"

echo "Sending $NUM_MESSAGES test messages to topic: $TOPIC"

# Check if running in Docker environment or locally
if [ -f /.dockerenv ]; then
  # Inside Docker
  KAFKA_HOST="kafka:9092"
else
  # Local environment
  KAFKA_HOST="localhost:9092"
fi

# Generate and send messages
for i in $(seq 1 $NUM_MESSAGES); do
  MESSAGE_ID="msg-$(date +%s)-$i"
  MESSAGE_CONTENT="Test message $i at $(date)"
  
  MESSAGE="{\"id\":\"$MESSAGE_ID\",\"content\":\"$MESSAGE_CONTENT\",\"metadata\":{\"source\":\"test-script\",\"type\":\"test\",\"tags\":[\"automated\",\"test\"],\"attributes\":{\"sequence\":\"$i\"}}}"
  
  echo "Sending message $i: $MESSAGE_ID"
  echo "$MESSAGE" | docker-compose exec -T kafka kafka-console-producer.sh --broker-list $KAFKA_HOST --topic $TOPIC
  
  # Add small delay between messages
  sleep 0.5
done

echo "Successfully sent $NUM_MESSAGES messages to Kafka topic: $TOPIC"