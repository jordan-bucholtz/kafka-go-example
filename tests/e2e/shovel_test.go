package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	tcKafka "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/user/golang-test-kafka/internal/config"
	"github.com/user/golang-test-kafka/internal/kafka"
	"github.com/user/golang-test-kafka/internal/logging"
	"github.com/user/golang-test-kafka/internal/service"
	"github.com/user/golang-test-kafka/internal/storage"
	"github.com/user/golang-test-kafka/pkg/models"
)

const (
	testTopic       = "test-topic"
	testGroup       = "test-consumer-group"
	messageCount    = 10
	waitTimeout     = 60 * time.Second
	zkPort          = "2181"
	kafkaPort       = "9092"
	storageBasePath = "./test-data"
)

// TestShovelE2E is a comprehensive end-to-end test of the Shovel application
func TestShovelE2E(t *testing.T) {
	// Setup test context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), waitTimeout)
	defer cancel()

	// Create a logger for testing - use development mode for clearer test output
	logger, err := logging.NewDevelopmentLogger()
	require.NoError(t, err, "Failed to create logger")
	defer logger.Sync()

	logger.Info("Starting end-to-end test for Shovel application")

	// Clean test storage directory before and after test
	cleanTestStorage(t)
	defer cleanTestStorage(t)

	// Start Kafka container with testcontainers Kafka module
	kafkaContainer, kafkaBootstrapServer := setupKafkaTestcontainer(t, ctx, logger)
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			logger.Error("Failed to terminate kafka container", "error", err)
		}
	}()

	// Create the test topic
	createTestTopic(t, ctx, kafkaBootstrapServer, logger)

	// Create test messages
	testMessages := generateTestMessages(messageCount)
	logger.Info("Generated test messages", "count", messageCount)

	// Produce messages to Kafka
	produceTestMessages(t, ctx, kafkaBootstrapServer, testMessages, logger)

	// Create and start the Shovel application
	stopShovel := startShovelApplication(t, logger, kafkaBootstrapServer)
	defer stopShovel()

	// Wait for messages to be processed
	waitForMessagesProcessed(t, testMessages, logger)

	// Verify all messages were correctly stored
	verifyMessageStorage(t, testMessages, logger)

	// Log success message
	logger.Info("✅ All messages were successfully consumed from Kafka and stored correctly")
}

// setupKafkaTestcontainer creates a Kafka container for testing using the testcontainers Kafka module
func setupKafkaTestcontainer(t *testing.T, ctx context.Context, logger *logging.Logger) (*tcKafka.KafkaContainer, string) {
	logger.Info("Setting up Kafka container using testcontainers Kafka module...")

	// Start Kafka container using the dedicated module
	kafkaContainer, err := tcKafka.Run(ctx, "confluentinc/confluent-local:7.5.0",
		tcKafka.WithClusterID("test-cluster"),
	)
	require.NoError(t, err, "Failed to start Kafka container")

	// Get bootstrap server address
	bootstrapServers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err, "Failed to get bootstrap servers")

	bootstrapServer := bootstrapServers[0] // Using the first broker as bootstrap server

	logger.Info("Kafka container started", "bootstrapServer", bootstrapServer)

	return kafkaContainer, bootstrapServer
}

// createTestTopic creates the test topic in Kafka
func createTestTopic(t *testing.T, ctx context.Context, bootstrapServer string, logger *logging.Logger) {
	logger.Info("Creating test topic", "topic", testTopic, "bootstrapServer", bootstrapServer)

	// Create admin client
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	admin, err := sarama.NewClusterAdmin([]string{bootstrapServer}, config)
	require.NoError(t, err, "Failed to create Kafka admin client")
	defer admin.Close()

	// Create topic
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     3,
		ReplicationFactor: 1,
	}
	err = admin.CreateTopic(testTopic, topicDetail, false)
	if err != nil && err != sarama.ErrTopicAlreadyExists {
		require.NoError(t, err, "Failed to create test topic")
	}

	logger.Info("Topic created or already exists", "topic", testTopic)
}

// generateTestMessages creates a slice of test messages
func generateTestMessages(count int) []models.Message {
	messages := make([]models.Message, count)
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("test-msg-%d", i)
		messages[i] = models.Message{
			ID:      id,
			Content: fmt.Sprintf("This is test message %d", i),
			Metadata: models.Metadata{
				Source: "e2e-test",
				Type:   "test",
				Tags:   []string{"test", "e2e"},
				Attributes: map[string]string{
					"testId": fmt.Sprintf("%d", i),
				},
			},
			Timestamp: time.Now(),
		}
	}
	return messages
}

// produceTestMessages sends test messages to Kafka
func produceTestMessages(t *testing.T, ctx context.Context, bootstrapServer string, messages []models.Message, logger *logging.Logger) {
	// Create producer config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	// Create producer
	producer, err := sarama.NewSyncProducer([]string{bootstrapServer}, config)
	require.NoError(t, err, "Failed to create Kafka producer")
	defer producer.Close()

	logger.Info("Sending test messages to Kafka", "count", len(messages), "topic", testTopic)

	// Send each message
	for _, msg := range messages {
		// Marshal message to JSON
		data, err := json.Marshal(msg)
		require.NoError(t, err, "Failed to marshal test message")

		// Create producer message
		producerMsg := &sarama.ProducerMessage{
			Topic: testTopic,
			Key:   sarama.StringEncoder(msg.ID),
			Value: sarama.ByteEncoder(data),
		}

		// Send message
		_, _, err = producer.SendMessage(producerMsg)
		require.NoError(t, err, "Failed to send test message")
	}

	logger.Info("Successfully produced test messages", "count", len(messages), "topic", testTopic)
}

// cleanTestStorage removes test storage directory if it exists and creates a fresh one
func cleanTestStorage(t *testing.T) {
	// First remove existing directory if it exists
	if _, err := os.Stat(storageBasePath); err == nil {
		err := os.RemoveAll(storageBasePath)
		require.NoError(t, err, "Failed to clean test storage directory")
	}

	// Create fresh directory for test data
	err := os.MkdirAll(storageBasePath, 0755)
	require.NoError(t, err, "Failed to create test storage directory")
}

// startShovelApplication creates and starts an instance of the Shovel application
func startShovelApplication(t *testing.T, logger *logging.Logger, bootstrapServer string) func() {
	// Create test configuration
	cfg := config.Config{
		Kafka: config.KafkaConfig{
			Brokers:       []string{bootstrapServer},
			Topic:         testTopic,
			ConsumerGroup: testGroup,
		},
		Storage: config.StorageConfig{
			Type:     "file",
			FilePath: storageBasePath,
		},
	}

	// Create storage
	store, err := storage.NewStorage(cfg.Storage)
	require.NoError(t, err, "Failed to create storage")

	// Create service
	svc := service.NewService(store, logger)

	// Create consumer
	consumer, err := kafka.NewConsumer(cfg.Kafka, svc.ProcessMessage, logger)
	require.NoError(t, err, "Failed to create Kafka consumer")

	// Setup context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	logger.Info("Starting Shovel application for test",
		"bootstrapServer", bootstrapServer,
		"topic", testTopic,
		"consumerGroup", testGroup)

	// Start consuming messages in the background
	go func() {
		err := consumer.Start(ctx)
		if err != nil && ctx.Err() == nil {
			logger.Error("Error starting consumer", "error", err)
		}
	}()

	// Return function to stop the application
	return func() {
		logger.Info("Stopping Shovel application")
		cancel()
		consumer.Close()
		store.Close()
	}
}

// waitForMessagesProcessed waits until all messages are processed and stored
func waitForMessagesProcessed(t *testing.T, messages []models.Message, logger *logging.Logger) {
	logger.Info("Waiting for messages to be processed", "count", len(messages))

	// Function to check if all files exist
	checkAllFilesExist := func() bool {
		for _, msg := range messages {
			filePath := filepath.Join(storageBasePath, fmt.Sprintf("%s.json", msg.ID))
			if _, err := os.Stat(filePath); os.IsNotExist(err) {
				return false
			}
		}
		return true
	}

	// Wait with timeout for all files to be created
	timeout := time.After(waitTimeout)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if checkAllFilesExist() {
				logger.Info("All messages have been processed")
				return
			}
		case <-timeout:
			logger.Error("Timed out waiting for messages to be processed")
			t.Fatalf("Timed out waiting for messages to be processed")
			return
		}
	}
}

// verifyMessageStorage checks that all messages were stored correctly
func verifyMessageStorage(t *testing.T, messages []models.Message, logger *logging.Logger) {
	logger.Info("Verifying message storage", "count", len(messages))

	for _, expected := range messages {
		// Check file exists
		filePath := filepath.Join(storageBasePath, fmt.Sprintf("%s.json", expected.ID))
		assert.FileExists(t, filePath)

		// Read and parse file content
		data, err := os.ReadFile(filePath)
		require.NoError(t, err, "Failed to read stored message file")

		var actual models.Message
		err = json.Unmarshal(data, &actual)
		require.NoError(t, err, "Failed to unmarshal stored message")

		// Compare important fields (ignoring timestamp as it might be updated)
		assert.Equal(t, expected.ID, actual.ID)
		assert.Equal(t, expected.Content, actual.Content)
		assert.Equal(t, expected.Metadata.Source, actual.Metadata.Source)
		assert.Equal(t, expected.Metadata.Tags, actual.Metadata.Tags)

		logger.Debug("Verified message storage", "messageId", expected.ID)
	}

	logger.Info("All messages verified successfully")
}
