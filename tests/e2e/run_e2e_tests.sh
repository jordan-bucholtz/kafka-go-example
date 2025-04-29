#!/bin/bash

# Run end-to-end tests for the Shovel application
# This script installs dependencies, runs tests, and provides human-readable output

set -e

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}======================================${NC}"
echo -e "${YELLOW}    Shovel E2E Test Runner           ${NC}"
echo -e "${YELLOW}======================================${NC}"

# Navigate to project root
PROJECT_ROOT=$(git rev-parse --show-toplevel 2>/dev/null || cd ../.. && pwd)
cd "$PROJECT_ROOT"

echo -e "\n${YELLOW}Installing test dependencies...${NC}"

# Add testify for assertions
go get -u github.com/stretchr/testify

# Add testcontainers-go for Docker container management in tests
go get -u github.com/testcontainers/testcontainers-go

# Run the e2e tests with verbose output and increased timeout
echo -e "\n${YELLOW}Running end-to-end tests...${NC}"
cd tests/e2e
go test -v -timeout 5m ./...

# Check test result
if [ $? -eq 0 ]; then
  echo -e "\n${GREEN}✓ E2E tests passed successfully!${NC}"
  echo -e "${GREEN}The Shovel application correctly processed messages from Kafka and stored them.${NC}"
else
  echo -e "\n${RED}✗ E2E tests failed.${NC}"
  echo -e "${RED}Check the logs above for details on the failure.${NC}"
  exit 1
fi

echo -e "\n${YELLOW}======================================${NC}"
echo -e "${YELLOW}    E2E Tests Completed               ${NC}"
echo -e "${YELLOW}======================================${NC}"