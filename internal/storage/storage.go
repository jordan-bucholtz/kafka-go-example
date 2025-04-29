package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/user/golang-test-kafka/internal/config"
	"github.com/user/golang-test-kafka/pkg/models"
)

// Repository defines the storage interface
type Repository interface {
	SaveMessage(message *models.Message) error
	GetMessage(id string) (*models.Message, error)
	Close() error
}

// Storage implements the Repository interface
type Storage struct {
	cfg config.StorageConfig
}

// NewStorage creates a new storage repository based on the configuration
func NewStorage(cfg config.StorageConfig) (Repository, error) {
	switch cfg.Type {
	case "file":
		return &FileStorage{
			basePath: cfg.FilePath,
		}, nil
	// Add other storage implementations here (postgres, mysql, etc.)
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.Type)
	}
}

// FileStorage implements file-based storage
type FileStorage struct {
	basePath string
}

// SaveMessage saves a message to a file
func (f *FileStorage) SaveMessage(message *models.Message) error {
	// Ensure directory exists
	if err := os.MkdirAll(f.basePath, 0755); err != nil {
		return fmt.Errorf("failed to create storage directory: %w", err)
	}

	// Set timestamp if not already set
	if message.Timestamp.IsZero() {
		message.Timestamp = time.Now()
	}

	// Marshal the message to JSON
	data, err := json.MarshalIndent(message, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// Write to file
	filePath := filepath.Join(f.basePath, fmt.Sprintf("%s.json", message.ID))
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write message to file: %w", err)
	}

	return nil
}

// GetMessage retrieves a message from a file
func (f *FileStorage) GetMessage(id string) (*models.Message, error) {
	filePath := filepath.Join(f.basePath, fmt.Sprintf("%s.json", id))

	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("message not found: %s", id)
		}
		return nil, fmt.Errorf("failed to read message file: %w", err)
	}

	var message models.Message
	if err := json.Unmarshal(data, &message); err != nil {
		return nil, fmt.Errorf("failed to unmarshal message: %w", err)
	}

	return &message, nil
}

// Close closes the storage connection
func (f *FileStorage) Close() error {
	// No resources to release for file storage
	return nil
}
