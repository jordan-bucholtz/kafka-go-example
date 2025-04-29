package service

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/user/golang-test-kafka/internal/storage"
	"github.com/user/golang-test-kafka/pkg/models"
)

// Service handles business logic for the application
type Service struct {
	store  storage.Repository
	logger *log.Logger
}

// NewService creates a new service instance
func NewService(store storage.Repository, logger *log.Logger) *Service {
	return &Service{
		store:  store,
		logger: logger,
	}
}

// ProcessMessage processes a message from Kafka and stores it
// Returns error only if processing fails, so consumer knows not to commit offset
func (s *Service) ProcessMessage(data []byte) error {
	// Log the raw message to stdout
	s.logger.Printf("[EVENT RECEIVED] %s", string(data))

	startTime := time.Now()

	// Parse message into our data model
	var message models.Message
	if err := json.Unmarshal(data, &message); err != nil {
		s.logger.Printf("[ERROR] Failed to parse message: %v", err)
		// Return error so consumer doesn't commit offset for invalid messages
		return fmt.Errorf("failed to unmarshal message: %w", err)
	}

	// Set timestamp if not present
	if message.Timestamp.IsZero() {
		message.Timestamp = time.Now()
	}

	// Validate the message
	if err := s.validateMessage(&message); err != nil {
		s.logger.Printf("[ERROR] Message validation failed: %v", err)
		return fmt.Errorf("message validation failed: %w", err)
	}

	// Store the message
	if err := s.store.SaveMessage(&message); err != nil {
		s.logger.Printf("[ERROR] Failed to store message: %v", err)
		return fmt.Errorf("failed to store message: %w", err)
	}

	// Log successful processing
	processingTime := time.Since(startTime).Milliseconds()
	s.logger.Printf("[SUCCESS] Processed message ID=%s in %dms", message.ID, processingTime)

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
