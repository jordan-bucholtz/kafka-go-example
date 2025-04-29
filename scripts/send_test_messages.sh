#!/bin/bash

# Script to send test messages to Kafka for the Shovel service
# Usage: ./scripts/send_test_messages.sh [number_of_messages]

# Set default number of messages
NUM_MESSAGES=${1:-5}
TOPIC="test-topic"

timestamp() {
  date +"%Y-%m-%d %H:%M:%S"
}

log_info() {
  echo "$(timestamp) [INFO] $1"
}

log_info "Starting test message generator"
log_info "Sending $NUM_MESSAGES test messages to topic: $TOPIC"

# Check if running in Docker environment or locally
if [ -f /.dockerenv ]; then
  # Inside Docker
  KAFKA_HOST="kafka:9092"
  log_info "Running in Docker environment, using Kafka at $KAFKA_HOST"
else
  # Local environment
  KAFKA_HOST="localhost:9092"
  log_info "Running in local environment, using Kafka at $KAFKA_HOST"
fi

# Generate and send messages
success_count=0
for i in $(seq 1 $NUM_MESSAGES); do
  MESSAGE_ID="msg-$(date +%s)-$i"
  TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  MESSAGE_CONTENT="Test message $i at $TIMESTAMP"
  
  MESSAGE="{\"id\":\"$MESSAGE_ID\",\"content\":\"$MESSAGE_CONTENT\",\"metadata\":{\"source\":\"test-script\",\"type\":\"test\",\"tags\":[\"automated\",\"test\"],\"attributes\":{\"sequence\":\"$i\"}},\"timestamp\":\"$TIMESTAMP\"}"
  
  log_info "Sending message $i/$NUM_MESSAGES (ID: $MESSAGE_ID)"
  
  if echo "$MESSAGE" | docker-compose exec -T kafka kafka-console-producer.sh --broker-list $KAFKA_HOST --topic $TOPIC --property "parse.key=true" --property "key.separator=:" --property "key.serializer=org.apache.kafka.common.serialization.StringSerializer" <<< "$MESSAGE_ID:$MESSAGE"; then
    log_info "Message $i sent successfully"
    ((success_count++))
  else
    log_info "Failed to send message $i"
  fi
  
  # Add small delay between messages
  sleep 0.5
done

log_info "Operation completed: sent $success_count/$NUM_MESSAGES messages to Kafka topic: $TOPIC"