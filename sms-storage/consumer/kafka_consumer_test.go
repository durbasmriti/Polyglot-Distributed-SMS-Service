package consumer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/durbasmriti/Polyglot-Distributed-SMS-Service/sms-storage/model"
	"github.com/segmentio/kafka-go"
)

type FakeMessageReader struct {
	messages chan kafka.Message
	closed   chan struct{}
}

func NewFakeMessageReader() *FakeMessageReader {
	return &FakeMessageReader{
		messages: make(chan kafka.Message, 10),
		closed:   make(chan struct{}),
	}
}

func (f *FakeMessageReader) ReadMessage(
	ctx context.Context,
) (kafka.Message, error) {

	select {
	case message := <-f.messages:
		return message, nil

	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	}
}

func (f *FakeMessageReader) Close() error {
	select {
	case <-f.closed:
	default:
		close(f.closed)
	}

	return nil
}

type FakeSmsRepository struct {
	saveFunc func(model.SmsEvent) error
}

func (f *FakeSmsRepository) Save(
	event model.SmsEvent,
) error {
	return f.saveFunc(event)
}

func TestConsumerValidMessage(t *testing.T) {

	reader := NewFakeMessageReader()

	savedEvents := make(chan model.SmsEvent, 1)

	repo := &FakeSmsRepository{
		saveFunc: func(event model.SmsEvent) error {
			savedEvents <- event
			return nil
		},
	}

	consumer := &KafkaConsumer{
		reader:     reader,
		repository: repo,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		consumer.Start(ctx)
	}()

	reader.messages <- kafka.Message{
		Value: []byte(`{
            "userId":"user_123",
            "phoneNumber":"9876543210",
            "message":"Hello",
            "status":"SUCCESS"
        }`),
	}

	select {
	case event := <-savedEvents:

		if event.UserID != "user_123" {
			t.Errorf("Unexpected UserID: %s", event.UserID)
		}

		if event.Status != "SUCCESS" {
			t.Errorf("Unexpected status: %s", event.Status)
		}

		if event.CreatedAt.IsZero() {
			t.Error("Expected CreatedAt to be set")
		}

	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for saved event")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Consumer did not stop")
	}

	select {
	case <-reader.closed:
	default:
		t.Error("Kafka reader was not closed")
	}
}

func TestConsumerMalformedMessage(t *testing.T) {

	reader := NewFakeMessageReader()

	savedEvents := make(chan model.SmsEvent, 1)

	repo := &FakeSmsRepository{
		saveFunc: func(event model.SmsEvent) error {
			savedEvents <- event
			return nil
		},
	}

	consumer := &KafkaConsumer{
		reader:     reader,
		repository: repo,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		consumer.Start(ctx)
	}()

	// First message contains invalid JSON.
	reader.messages <- kafka.Message{
		Value: []byte(`invalid json`),
	}

	// Second message is valid.
	reader.messages <- kafka.Message{
		Value: []byte(`{
            "userId":"user_456",
            "phoneNumber":"9123456789",
            "message":"Testing",
            "status":"SUCCESS"
        }`),
	}

	select {
	case event := <-savedEvents:

		if event.UserID != "user_456" {
			t.Errorf("Unexpected UserID: %s", event.UserID)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("Consumer did not process the valid message")
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Consumer did not stop")
	}
}

func TestConsumerRepositoryError(t *testing.T) {

	reader := NewFakeMessageReader()

	savedEvents := make(chan model.SmsEvent, 1)

	saveCalls := 0

	repo := &FakeSmsRepository{
		saveFunc: func(event model.SmsEvent) error {

			saveCalls++

			if saveCalls == 1 {
				return errors.New("MongoDB unavailable")
			}

			savedEvents <- event
			return nil
		},
	}

	consumer := &KafkaConsumer{
		reader:     reader,
		repository: repo,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		defer close(done)
		consumer.Start(ctx)
	}()

	// First event will fail to save.
	reader.messages <- kafka.Message{
		Value: []byte(`{
            "userId":"user_111",
            "phoneNumber":"9876543210",
            "message":"First message",
            "status":"SUCCESS"
        }`),
	}

	// Second event should still be processed.
	reader.messages <- kafka.Message{
		Value: []byte(`{
            "userId":"user_222",
            "phoneNumber":"9123456789",
            "message":"Second message",
            "status":"SUCCESS"
        }`),
	}

	select {
	case event := <-savedEvents:

		if event.UserID != "user_222" {
			t.Errorf("Unexpected UserID: %s", event.UserID)
		}

	case <-time.After(2 * time.Second):
		t.Fatal("Consumer did not process the second message")
	}

	if saveCalls != 2 {
		t.Errorf("Expected 2 save attempts, got %d", saveCalls)
	}

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Consumer did not stop")
	}
}
