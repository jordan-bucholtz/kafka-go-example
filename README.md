# Shovel - Kafka Data Transfer Microservice

Shovel is a Go microservice that consumes messages from Kafka topics and reliably processes them with at-least-once delivery guarantees. It follows the pattern of consuming messages, processing them, and only committing offsets after successful processing.

## Features

- Reliable Kafka message consumption with explicit offset management
- Configurable retry mechanism for failed message processing
- Simple extensible architecture for different storage backends
- Robust error handling and graceful shutdowns
- Docker and Docker Compose support for easy local development

## Project Structure

```
├── cmd/
│   └── app/            # Application entry points
│       └── main.go     # Main application entry
├── internal/           # Private application code
│   ├── config/         # Configuration handling
│   ├── kafka/          # Kafka consumer implementation
│   ├── service/        # Business logic
│   └── storage/        # Storage implementations
├── pkg/                # Public libraries
│   └── models/         # Data models
├── scripts/            # Scripts for development, CI/CD
├── Dockerfile          # Docker build instructions
├── docker-compose.yml  # Local development environment
├── Makefile            # Build automation
├── config.json         # Application configuration
└── go.mod              # Go module definition
```

## Quick Start

### Running with Docker Compose (Recommended)

The easiest way to get started is using Docker Compose, which will set up both the Kafka cluster and the Shovel service:

```bash
# Start all services (Zookeeper, Kafka, Kafka UI, and Shovel)
docker-compose up -d

# Follow the logs from the Shovel service
docker-compose logs -f shovel

# To stop all services
docker-compose down
```

The Docker Compose setup includes:
- Zookeeper (required by Kafka)
- Kafka broker
- Kafka UI (accessible at http://localhost:8080)
- Kafka topic creation service
- Shovel microservice

### Sending Test Messages

To send test messages to Kafka for Shovel to consume:

```bash
# Access the Kafka container
docker-compose exec kafka bash

# Send a test message using the kafka-console-producer
kafka-console-producer.sh --broker-list localhost:9092 --topic test-topic

# Then type JSON messages in this format:
{"id":"msg1","content":"Test message","metadata":{"source":"test"}}
# Press Ctrl+D to exit
```

Alternatively, use the Kafka UI at http://localhost:8080 to send messages.

## Local Development

### Prerequisites

- Go 1.24 or higher
- Docker and Docker Compose (for local Kafka setup)

### Building and Running Locally

```bash
# Download dependencies
make deps

# Create a sample config file
make config

# Build the service
make build

# Run the service (requires Kafka to be running)
make run
```

### Configuration

The service can be configured using a JSON configuration file or environment variables:

```json
{
  "kafka": {
    "brokers": ["localhost:9092"],
    "topic": "test-topic",
    "consumerGroup": "test-consumer-group"
  },
  "storage": {
    "type": "file",
    "filePath": "data/output"
  },
  "server": {
    "port": 8080
  }
}
```

## Message Processing Flow

1. Shovel consumes messages from the configured Kafka topic
2. Each message is logged to stdout for visibility
3. Messages are validated and processed according to business logic
4. On successful processing, the message offset is committed to Kafka
5. On failure, processing is retried up to a configurable number of times

## Extending Shovel

Shovel is designed to be extended in the following ways:

### Adding New Storage Backends

Implement the `Repository` interface in `internal/storage/storage.go` and add a new case to the `NewStorage` factory function.

### Customizing Message Processing

Modify the `ProcessMessage` function in `internal/service/service.go` to implement your specific business logic.

### Adding Metrics and Monitoring

Consider adding Prometheus metrics or other monitoring solutions to track processing rates, errors, and latencies.

## License

[Your License]

## Contributing

[Your contribution guidelines]