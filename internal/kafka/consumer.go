package kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/user/golang-test-kafka/internal/config"
	"github.com/user/golang-test-kafka/internal/logging"
)

// MessageHandler is a function that processes Kafka messages
// Returns error if processing fails and offset should not be committed
type MessageHandler func([]byte) error

// Consumer handles consuming messages from Kafka
type Consumer struct {
	client        sarama.ConsumerGroup
	topics        []string
	handler       MessageHandler
	logger        *logging.Logger
	ready         chan bool
	consumerGroup string
}

// ConsumerGroupHandler implements sarama.ConsumerGroupHandler
type ConsumerGroupHandler struct {
	handler    MessageHandler
	logger     *logging.Logger
	ready      chan bool
	mu         sync.Mutex
	errorCount map[string]int // Track errors per partition-topic
	maxRetries int
}

// NewConsumer creates a new Kafka consumer
func NewConsumer(cfg config.KafkaConfig, handler MessageHandler, logger *logging.Logger) (*Consumer, error) {
	// Create Kafka consumer configuration
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	// Set initial offset to earliest so we don't miss messages
	config.Consumer.Offsets.Initial = sarama.OffsetOldest
	// Enable autocommit for more reliable offset management
	config.Consumer.Offsets.AutoCommit.Enable = true
	// Set a reasonable autocommit interval (1 second)
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second
	// Set appropriate Kafka version
	config.Version = sarama.V2_8_0_0

	// Create consumer group client
	client, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.ConsumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	// Create and return the consumer
	consumer := &Consumer{
		client:        client,
		topics:        []string{cfg.Topic},
		handler:       handler,
		logger:        logger,
		ready:         make(chan bool),
		consumerGroup: cfg.ConsumerGroup,
	}

	return consumer, nil
}

// Start begins consuming messages from Kafka
func (c *Consumer) Start(ctx context.Context) error {
	c.logger.Info("Starting Shovel Kafka consumer...",
		"topics", c.topics,
		"consumerGroup", c.consumerGroup)

	// Create a new handler with error tracking
	handler := &ConsumerGroupHandler{
		handler:    c.handler,
		logger:     c.logger,
		ready:      c.ready,
		errorCount: make(map[string]int),
		maxRetries: 3, // Maximum retries before skipping message
	}

	// Consume messages in a loop to handle reconnections
	for {
		// Check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			c.logger.Info("Context cancelled, stopping consumer")
			return nil
		}

		// Consume via consumer group
		err := c.client.Consume(ctx, c.topics, handler)
		if err != nil {
			if strings.Contains(err.Error(), "context canceled") {
				return nil
			}
			c.logger.Error("Error from consumer", "error", err)
			time.Sleep(time.Second) // Wait before retrying
		}

		// Check if consumer is ready
		select {
		case <-c.ready:
			c.logger.Info("Consumer is ready")
		default:
			c.logger.Info("Consumer is not ready, waiting...")
			select {
			case <-c.ready:
				c.logger.Info("Consumer is now ready")
			case <-ctx.Done():
				return nil
			case <-time.After(10 * time.Second):
				c.logger.Warn("Timeout waiting for consumer to be ready, retrying...")
			}
		}
	}
}

// Close closes the consumer connection
func (c *Consumer) Close() error {
	c.logger.Info("Closing Kafka consumer")
	return c.client.Close()
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	h.logger.Debug("Consumer group setup complete")
	// Reset error counts on rebalance
	h.mu.Lock()
	h.errorCount = make(map[string]int)
	h.mu.Unlock()

	close(h.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	h.logger.Debug("Consumer group cleanup complete")
	h.ready = make(chan bool)
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages()
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE: Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/IBM/sarama/blob/main/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		msgKey := fmt.Sprintf("%s-%d-%d", message.Topic, message.Partition, message.Offset)
		msgLogger := h.logger.WithFields(map[string]interface{}{
			"topic":     message.Topic,
			"partition": message.Partition,
			"offset":    message.Offset,
			"key":       string(message.Key),
		})

		msgLogger.Debug("Processing message")

		// Process message with the handler
		if err := h.handler(message.Value); err != nil {
			// Handle errors
			h.mu.Lock()
			h.errorCount[msgKey]++
			retryCount := h.errorCount[msgKey]
			h.mu.Unlock()

			// Determine if we should retry or skip based on error count
			if retryCount <= h.maxRetries {
				msgLogger.Warn("Error processing message",
					"error", err,
					"attempt", retryCount,
					"maxRetries", h.maxRetries)
				// Do not mark message - it will be redelivered when the session ends
				// This is a deliberate non-commit to retry
				continue
			}

			msgLogger.Error("Max retries reached for message, skipping",
				"msgKey", msgKey,
				"retries", retryCount,
				"error", err)
			// Mark message as processed after max retries to avoid endless loop
			session.MarkMessage(message, "")
			continue
		}

		// Message processed successfully
		msgLogger.Info("Successfully processed and committed message")

		// Mark message as processed
		session.MarkMessage(message, "")

		// Reset error count for this message on success
		h.mu.Lock()
		delete(h.errorCount, msgKey)
		h.mu.Unlock()
	}
	return nil
}
