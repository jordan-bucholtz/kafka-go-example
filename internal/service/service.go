package service

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/user/golang-test-kafka/internal/logging"
	"github.com/user/golang-test-kafka/internal/storage"
	"github.com/user/golang-test-kafka/pkg/models"
)

// Service handles business logic for the application
type Service struct {
	store  storage.Repository
	logger *logging.Logger
}

// NewService creates a new service instance
func NewService(store storage.Repository, logger *logging.Logger) *Service {
	return &Service{
		store:  store,
		logger: logger,
	}
}

// ProcessMessage processes a message from Kafka and stores it
// Returns error only if processing fails, so consumer knows not to commit offset
func (s *Service) ProcessMessage(data []byte) error {
	// Create a logger for this specific message
	msgLogger := s.logger.WithField("rawData", string(data))
	msgLogger.Debug("Event received")

	startTime := time.Now()

	// Parse message into our data model
	var message models.Message
	if err := json.Unmarshal(data, &message); err != nil {
		msgLogger.Error("Failed to parse message", "error", err)
		// Return error so consumer doesn't commit offset for invalid messages
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	// Create a logger with message context
	msgLogger = s.logger.WithFields(map[string]interface{}{
		"messageId": message.ID,
		"source":    message.Metadata.Source,
	})

	// Set timestamp if not present
	if message.Timestamp.IsZero() {
		message.Timestamp = time.Now()
		msgLogger.Debug("Set missing timestamp on message")
	}

	// Validate the message
	if err := s.validateMessage(&message); err != nil {
		msgLogger.Error("Message validation failed", "error", err)
		return fmt.Errorf("message validation failed: %w", err)
	}

	// Store the message
	if err := s.store.SaveMessage(&message); err != nil {
		msgLogger.Error("Failed to store message", "error", err)
		return fmt.Errorf("failed to store message: %w", err)
	}

	// Log successful processing
	processingTime := time.Since(startTime).Milliseconds()
	msgLogger.Info("Message processed successfully",
		"processingTimeMs", processingTime,
		"messageType", message.Metadata.Type)

	// Only return nil if processing was successful
	// This will signal to the Kafka consumer that it's safe to commit the offset
	return nil
}

// validateMessage performs validation on the message
func (s *Service) validateMessage(message *models.Message) error {
	if message.ID == "" {
		return fmt.Errorf("message ID is required")
	}

	// Additional validation rules can be added here

	return nil
}
