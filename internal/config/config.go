package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Config holds all configuration for the application
type Config struct {
	Kafka   KafkaConfig   `json:"kafka"`
	Storage StorageConfig `json:"storage"`
	Server  ServerConfig  `json:"server"`
	Logging LoggingConfig `json:"logging"`
}

// KafkaConfig holds Kafka-related configuration
type KafkaConfig struct {
	Brokers       []string `json:"brokers"`
	Topic         string   `json:"topic"`
	ConsumerGroup string   `json:"consumerGroup"`
}

// StorageConfig holds storage-related configuration
type StorageConfig struct {
	Type     string `json:"type"` // "postgres", "mysql", "file", etc.
	Host     string `json:"host,omitempty"`
	Port     int    `json:"port,omitempty"`
	Database string `json:"database,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	FilePath string `json:"filePath,omitempty"`
}

// ServerConfig holds HTTP server configuration
type ServerConfig struct {
	Port int `json:"port"`
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level      string `json:"level"`      // debug, info, warn, error
	OutputPath string `json:"outputPath"` // stdout, stderr, or file path
	Encoding   string `json:"encoding"`   // json or console
	DevMode    bool   `json:"devMode"`    // if true, use development logger
}

// Load reads configuration from a JSON file and environment variables
func Load() (*Config, error) {
	cfg := &Config{
		Kafka: KafkaConfig{
			Brokers:       []string{"localhost:9092"},
			Topic:         "test-topic",
			ConsumerGroup: "test-consumer-group",
		},
		Storage: StorageConfig{
			Type:     "file",
			FilePath: "data/output",
		},
		Server: ServerConfig{
			Port: 8080,
		},
		Logging: LoggingConfig{
			Level:      "info",
			OutputPath: "stdout",
			Encoding:   "json",
			DevMode:    false,
		},
	}

	// Try to load from config file if it exists
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "config.json"
	}

	if _, err := os.Stat(configPath); err == nil {
		file, err := os.Open(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open config file: %w", err)
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		if err := decoder.Decode(cfg); err != nil {
			return nil, fmt.Errorf("failed to decode config file: %w", err)
		}
	}

	// Override with environment variables
	if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
		cfg.Kafka.Brokers = strings.Split(brokers, ",")
	}
	if topic := os.Getenv("KAFKA_TOPIC"); topic != "" {
		cfg.Kafka.Topic = topic
	}
	if consumerGroup := os.Getenv("KAFKA_CONSUMER_GROUP"); consumerGroup != "" {
		cfg.Kafka.ConsumerGroup = consumerGroup
	}

	// Logging environment variables
	if level := os.Getenv("LOG_LEVEL"); level != "" {
		cfg.Logging.Level = level
	}
	if output := os.Getenv("LOG_OUTPUT"); output != "" {
		cfg.Logging.OutputPath = output
	}
	if encoding := os.Getenv("LOG_ENCODING"); encoding != "" {
		cfg.Logging.Encoding = encoding
	}
	if devMode := os.Getenv("LOG_DEV_MODE"); devMode == "true" {
		cfg.Logging.DevMode = true
	}

	// Ensure storage directory exists if using file storage
	if cfg.Storage.Type == "file" && cfg.Storage.FilePath != "" {
		err := os.MkdirAll(filepath.Dir(cfg.Storage.FilePath), 0755)
		if err != nil {
			return nil, fmt.Errorf("failed to create storage directory: %w", err)
		}
	}

	return cfg, nil
}
