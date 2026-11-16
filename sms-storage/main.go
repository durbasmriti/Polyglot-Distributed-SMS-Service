package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/durbasmriti/Polyglot-Distributed-SMS-Service/sms-storage/config"
	"github.com/durbasmriti/Polyglot-Distributed-SMS-Service/sms-storage/consumer"
	"github.com/durbasmriti/Polyglot-Distributed-SMS-Service/sms-storage/handler"
	"github.com/durbasmriti/Polyglot-Distributed-SMS-Service/sms-storage/repository"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {

	logger := slog.New(
		slog.NewJSONHandler(
			os.Stdout,
			&slog.HandlerOptions{
				Level: slog.LevelInfo,
			},
		),
	)

	slog.SetDefault(logger)

	if err := run(); err != nil {
		slog.Error("Application stopped with error", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.Load()

	slog.Info("Application configuration loaded")

	client, err := mongo.Connect(
		options.Client().ApplyURI(cfg.MongoURI),
	)

	if err != nil {
		return fmt.Errorf("MongoDB connection failed: %w", err)
	}

	defer func() {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cancel()

		if err := client.Disconnect(ctx); err != nil {
			slog.Error("MongoDB disconnect failed", "error", err)
		} else {
			slog.Info("MongoDB disconnected")
		}
	}()

	if err := client.Ping(context.Background(), nil); err != nil {
		return fmt.Errorf("MongoDB ping failed: %w", err)
	}

	slog.Info("Connected to MongoDB")

	collection := client.
		Database("smsdb").
		Collection("sms_events")

	smsRepository := repository.NewSmsRepository(collection)

	// Listen for termination signals
	appCtx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	// Kafka consumer
	consumerCtx, cancelConsumer := context.WithCancel(appCtx)
	defer cancelConsumer()

	kafkaConsumer := consumer.NewKafkaConsumer(
		smsRepository,
		cfg.KafkaBrokers,
		cfg.KafkaTopic,
		cfg.KafkaGroupID,
	)

	consumerDone := make(chan struct{})

	go func() {
		defer close(consumerDone)
		kafkaConsumer.Start(consumerCtx)
	}()

	// HTTP API
	smsHandler := handler.NewSmsHandler(smsRepository)

	mux := http.NewServeMux()

	mux.HandleFunc(
		"GET /v1/sms/history/{userId}",
		smsHandler.GetHistory,
	)

	server := &http.Server{
		Addr:    ":" + cfg.HTTPPort,
		Handler: mux,
	}

	serverErr := make(chan error, 1)

	go func() {
		slog.Info(
			"SMS Storage Service started",
			"port", cfg.HTTPPort,
		)
		serverErr <- server.ListenAndServe()
	}()

	// Wait for either a termination signal or server failure
	select {

	case <-appCtx.Done():
		slog.Info("Shutdown signal received")

	case err := <-serverErr:

		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			cancelConsumer()
			<-consumerDone
			return fmt.Errorf("HTTP server failed: %w", err)
		}
	}

	// Gracefully stop HTTP server
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer cancel()

	slog.Info("Shutting down HTTP server")

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("HTTP graceful shutdown failed", "error", err)

		if closeErr := server.Close(); closeErr != nil {
			slog.Error("HTTP server close failed", "error", closeErr)
		}
	}

	// Stop Kafka consumer
	cancelConsumer()

	select {
	case <-consumerDone:
		slog.Info("Kafka consumer stopped")

	case <-time.After(5 * time.Second):
		return fmt.Errorf("timed out waiting for Kafka consumer to stop")
	}

	slog.Info("Application shutdown complete")

	return nil
}
