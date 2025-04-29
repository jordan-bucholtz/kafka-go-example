package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/user/golang-test-kafka/internal/config"
	"github.com/user/golang-test-kafka/internal/kafka"
	"github.com/user/golang-test-kafka/internal/logging"
	"github.com/user/golang-test-kafka/internal/service"
	"github.com/user/golang-test-kafka/internal/storage"
)

func main() {
	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		// Use standard log for initialization errors before logger is available
		panic("Failed to load configuration: " + err.Error())
	}

	// Create logger
	logConfig := logging.Config{
		Level:      cfg.Logging.Level,
		OutputPath: cfg.Logging.OutputPath,
		Encoding:   cfg.Logging.Encoding,
		DevMode:    cfg.Logging.DevMode,
	}
	logger, err := logging.New(logConfig)
	if err != nil {
		panic("Failed to create logger: " + err.Error())
	}
	defer logger.Sync()

	logger.Info("Starting Shovel - Kafka data transfer service...")

	// Create storage client
	store, err := storage.NewStorage(cfg.Storage)
	if err != nil {
		logger.Fatal("Failed to initialize storage", "error", err)
	}
	defer store.Close()

	// Create service with logger
	svc := service.NewService(store, logger)

	// Create Kafka consumer
	consumer, err := kafka.NewConsumer(cfg.Kafka, svc.ProcessMessage, logger)
	if err != nil {
		logger.Fatal("Failed to create Kafka consumer", "error", err)
	}
	defer consumer.Close()

	// Setup context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consuming messages in the background
	go func() {
		if err := consumer.Start(ctx); err != nil && ctx.Err() == nil {
			logger.Fatal("Failed to start Kafka consumer", "error", err)
		}
	}()

	logger.Info("Shovel is running and consuming from topic", "topic", cfg.Kafka.Topic)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Wait for termination signal
	sig := <-sigChan
	logger.Info("Received shutdown signal, gracefully shutting down...", "signal", sig.String())

	// Trigger graceful shutdown
	cancel()
	logger.Info("Shovel service stopped")
}
