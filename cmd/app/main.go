package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/user/golang-test-kafka/internal/config"
	"github.com/user/golang-test-kafka/internal/kafka"
	"github.com/user/golang-test-kafka/internal/service"
	"github.com/user/golang-test-kafka/internal/storage"
)

func main() {
	// Setup logger
	logger := log.New(os.Stdout, "shovel: ", log.LstdFlags)
	logger.Println("Starting Shovel - Kafka data transfer service...")

	// Load configuration
	cfg, err := config.Load()
	if err != nil {
		logger.Fatalf("Failed to load configuration: %v", err)
	}

	// Create storage client
	store, err := storage.NewStorage(cfg.Storage)
	if err != nil {
		logger.Fatalf("Failed to initialize storage: %v", err)
	}
	defer store.Close()

	// Create service with stdout logging
	svc := service.NewService(store, logger)

	// Create Kafka consumer
	consumer, err := kafka.NewConsumer(cfg.Kafka, svc.ProcessMessage)
	if err != nil {
		logger.Fatalf("Failed to create Kafka consumer: %v", err)
	}
	defer consumer.Close()

	// Setup context with cancellation for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consuming messages in the background
	go func() {
		if err := consumer.Start(ctx); err != nil {
			logger.Fatalf("Failed to start Kafka consumer: %v", err)
		}
	}()

	logger.Printf("Shovel is running and consuming from topic: %s", cfg.Kafka.Topic)

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logger.Println("Received shutdown signal, gracefully shutting down...")
	cancel()
	logger.Println("Shovel service stopped")
}
