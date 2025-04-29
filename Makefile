.PHONY: build test run clean deps docker-build docker-run

# Build variables
BINARY_NAME=kafka-service
BUILD_DIR=./build

# Go variables
GOPATH=$(shell go env GOPATH)
GOBIN=$(GOPATH)/bin

# App variables
CONFIG_PATH=./config.json

build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	@go build -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/app

run: build
	@echo "Running $(BINARY_NAME)..."
	@CONFIG_PATH=$(CONFIG_PATH) $(BUILD_DIR)/$(BINARY_NAME)

test:
	@echo "Running tests..."
	@go test -v ./...

clean:
	@echo "Cleaning up..."
	@rm -rf $(BUILD_DIR)

deps:
	@echo "Downloading dependencies..."
	@go mod tidy

lint:
	@echo "Linting code..."
	@if [ -z "$(shell which golangci-lint)" ]; then \
		echo "Installing golangci-lint..."; \
		go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest; \
	fi
	@golangci-lint run

docker-build:
	@echo "Building Docker image..."
	@docker build -t $(BINARY_NAME) .

docker-run:
	@echo "Running Docker container..."
	@docker run -p 8080:8080 --name $(BINARY_NAME) $(BINARY_NAME)

docker-compose-up:
	@echo "Starting containers with Docker Compose..."
	@docker-compose up -d

docker-compose-down:
	@echo "Stopping containers with Docker Compose..."
	@docker-compose down

# Create a sample config file
config:
	@echo "Creating sample config file..."
	@echo '{\n  "kafka": {\n    "brokers": ["localhost:9092"],\n    "topic": "test-topic",\n    "consumerGroup": "test-consumer-group"\n  },\n  "storage": {\n    "type": "file",\n    "filePath": "data/output"\n  },\n  "server": {\n    "port": 8080\n  }\n}' > $(CONFIG_PATH)

# Help command
help:
	@echo "Available commands:"
	@echo "  make build           - Build the application"
	@echo "  make run             - Run the application"
	@echo "  make test            - Run tests"
	@echo "  make clean           - Clean build artifacts"
	@echo "  make deps            - Download dependencies"
	@echo "  make lint            - Lint the code"
	@echo "  make docker-build    - Build Docker image"
	@echo "  make docker-run      - Run Docker container"
	@echo "  make config          - Create sample config file"
	@echo "  make help            - Show this help message"