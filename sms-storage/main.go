package main

import (
	"log"
	"net/http"
	"context"

	"github.com/durbasmriti/Polyglot-Distributed-SMS-Service/sms-storage/consumer"
	"github.com/durbasmriti/Polyglot-Distributed-SMS-Service/sms-storage/handler"
	"github.com/durbasmriti/Polyglot-Distributed-SMS-Service/sms-storage/repository"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {

	// -----------------------------
	// MongoDB
	// -----------------------------

	mongoURI := "mongodb://localhost:27017"

	client, err := mongo.Connect(
		options.Client().ApplyURI(mongoURI),
	)

	if err != nil {
		log.Fatal(err)
	}

	// Verify MongoDB connection
	if err := client.Ping(
	context.Background(),
	nil,
	); err != nil {
	log.Fatal("MongoDB connection failed:", err)
	}

	log.Println("Connected to MongoDB")

	collection := client.
		Database("smsdb").
		Collection("sms_events")

	// -----------------------------
	// Repository
	// -----------------------------

	smsRepository := repository.NewSmsRepository(
		collection,
	)

	// -----------------------------
	// Kafka Consumer
	// -----------------------------

	kafkaConsumer := consumer.NewKafkaConsumer(
		smsRepository,
	)

	go kafkaConsumer.Start()

	// -----------------------------
	// HTTP API
	// -----------------------------

	smsHandler := handler.NewSmsHandler(
		smsRepository,
	)

	mux := http.NewServeMux()

	mux.HandleFunc(
		"GET /v1/sms/history/{userId}",
		smsHandler.GetHistory,
	)

	log.Println(
		"SMS Storage Service running on port 8081",
	)

	if err := http.ListenAndServe(
		":8081",
		mux,
	); err != nil {

		log.Fatal(err)
	}
}