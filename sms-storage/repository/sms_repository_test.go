package repository

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/durbasmriti/Polyglot-Distributed-SMS-Service/sms-storage/model"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func setupTestRepository(t *testing.T) *SmsRepository {

	t.Helper()

	mongoURI := "mongodb://localhost:27017"

	client, err := mongo.Connect(
		options.Client().ApplyURI(mongoURI),
	)

	if err != nil {
		t.Fatalf("Failed to connect to MongoDB: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(),
		5*time.Second,
	)
	defer cancel()

	if err := client.Ping(ctx, nil); err != nil {
		t.Fatalf("MongoDB ping failed: %v", err)
	}

	collectionName := fmt.Sprintf(
		"sms_events_test_%d",
		time.Now().UnixNano(),
	)

	collection := client.
		Database("smsdb_test").
		Collection(collectionName)

	t.Cleanup(func() {

		cleanupCtx, cleanupCancel := context.WithTimeout(
			context.Background(),
			5*time.Second,
		)
		defer cleanupCancel()

		if err := collection.Drop(cleanupCtx); err != nil {
			t.Errorf("Failed to drop test collection: %v", err)
		}

		if err := client.Disconnect(cleanupCtx); err != nil {
			t.Errorf("Failed to disconnect MongoDB: %v", err)
		}
	})

	return NewSmsRepository(collection)
}

func TestSaveAndFindByUserID(t *testing.T) {

	repo := setupTestRepository(t)

	event1 := model.SmsEvent{
		UserID:      "user_123",
		PhoneNumber: "9876543210",
		Message:     "Hello",
		Status:      "SUCCESS",
		CreatedAt:   time.Now(),
	}

	event2 := model.SmsEvent{
		UserID:      "user_456",
		PhoneNumber: "9123456789",
		Message:     "Testing",
		Status:      "FAIL",
		CreatedAt:   time.Now(),
	}

	// Save both events
	if err := repo.Save(event1); err != nil {
		t.Fatalf("Failed to save event1: %v", err)
	}

	if err := repo.Save(event2); err != nil {
		t.Fatalf("Failed to save event2: %v", err)
	}

	// Retrieve events for user_123
	events, err := repo.FindByUserID("user_123")

	if err != nil {
		t.Fatalf("Failed to retrieve events: %v", err)
	}

	// Only one event should belong to user_123
	if len(events) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(events))
	}

	// Verify retrieved data
	if events[0].UserID != event1.UserID {
		t.Errorf("Unexpected UserID: %s", events[0].UserID)
	}

	if events[0].PhoneNumber != event1.PhoneNumber {
		t.Errorf("Unexpected phone number: %s", events[0].PhoneNumber)
	}

	if events[0].Message != event1.Message {
		t.Errorf("Unexpected message: %s", events[0].Message)
	}

	if events[0].Status != event1.Status {
		t.Errorf("Unexpected status: %s", events[0].Status)
	}
}

func TestFindByUserIDEmptyResult(t *testing.T) {

	repo := setupTestRepository(t)

	events, err := repo.FindByUserID("unknown_user")

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(events) != 0 {
		t.Errorf("Expected 0 events, got %d", len(events))
	}
}
