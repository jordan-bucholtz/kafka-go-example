package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
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

	// Create a logger for testing
	logger := log.New(os.Stdout, "[e2e-test] ", log.LstdFlags)

	// Clean test storage directory before and after test
	cleanTestStorage(t)
	defer cleanTestStorage(t)

	// Start Kafka container with testcontainers Kafka module
	kafkaContainer, kafkaBootstrapServer := setupKafkaTestcontainer(t, ctx)
	defer func() {
		if err := kafkaContainer.Terminate(ctx); err != nil {
			t.Logf("failed to terminate kafka container: %s", err)
		}
	}()

	// Create the test topic
	createTestTopic(t, ctx, kafkaBootstrapServer)

	// Create test messages
	testMessages := generateTestMessages(messageCount)

	// Produce messages to Kafka
	produceTestMessages(t, ctx, kafkaBootstrapServer, testMessages)

	// Create and start the Shovel application
	stopShovel := startShovelApplication(t, logger, kafkaBootstrapServer)
	defer stopShovel()

	// Wait for messages to be processed
	waitForMessagesProcessed(t, testMessages)

	// Verify all messages were correctly stored
	verifyMessageStorage(t, testMessages)

	// Log success message
	t.Log("✅ All messages were successfully consumed from Kafka and stored correctly")
}

// setupKafkaTestcontainer creates a Kafka container for testing using the testcontainers Kafka module
func setupKafkaTestcontainer(t *testing.T, ctx context.Context) (*tcKafka.KafkaContainer, string) {
	logger := log.New(os.Stdout, "[kafka-setup] ", log.LstdFlags)
	logger.Println("Setting up Kafka container using testcontainers Kafka module...")

	// Start Kafka container using the dedicated module
	kafkaContainer, err := tcKafka.Run(ctx, "confluentinc/confluent-local:7.5.0",
		tcKafka.WithClusterID("test-cluster"),
	)
	require.NoError(t, err, "Failed to start Kafka container")

	// Get bootstrap server address
	bootstrapServers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err, "Failed to get bootstrap servers")

	bootstrapServer := bootstrapServers[0] // Using the first broker as bootstrap server

	logger.Printf("Kafka container started using testcontainers module, bootstrap server: %s", bootstrapServer)

	return kafkaContainer, bootstrapServer
}

// createTestTopic creates the test topic in Kafka
func createTestTopic(t *testing.T, ctx context.Context, bootstrapServer string) {
	log.Printf("Creating test topic %s on %s", testTopic, bootstrapServer)

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

	log.Printf("Topic %s created or already exists", testTopic)
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
func produceTestMessages(t *testing.T, ctx context.Context, bootstrapServer string, messages []models.Message) {
	// Create producer config
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	// Create producer
	producer, err := sarama.NewSyncProducer([]string{bootstrapServer}, config)
	require.NoError(t, err, "Failed to create Kafka producer")
	defer producer.Close()

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

	log.Printf("Successfully produced %d test messages to topic %s", len(messages), testTopic)
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
func startShovelApplication(t *testing.T, logger *log.Logger, bootstrapServer string) func() {
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
	consumer, err := kafka.NewConsumer(cfg.Kafka, svc.ProcessMessage)
	require.NoError(t, err, "Failed to create Kafka consumer")

	// Setup context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// Start consuming messages in the background
	go func() {
		err := consumer.Start(ctx)
		if err != nil && ctx.Err() == nil {
			logger.Printf("Error starting consumer: %v", err)
		}
	}()

	// Return function to stop the application
	return func() {
		cancel()
		consumer.Close()
		store.Close()
	}
}

// waitForMessagesProcessed waits until all messages are processed and stored
func waitForMessagesProcessed(t *testing.T, messages []models.Message) {
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
				return
			}
		case <-timeout:
			t.Fatalf("Timed out waiting for messages to be processed")
			return
		}
	}
}

// verifyMessageStorage checks that all messages were stored correctly
func verifyMessageStorage(t *testing.T, messages []models.Message) {
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
	}
}
