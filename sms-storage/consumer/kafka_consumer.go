package consumer

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/durbasmriti/Polyglot-Distributed-SMS-Service/sms-storage/model"
	"github.com/segmentio/kafka-go"
)

type MessageReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

type SmsRepository interface {
	Save(event model.SmsEvent) error
}

type KafkaConsumer struct {
	reader     MessageReader
	repository SmsRepository
}

func NewKafkaConsumer(
	repo SmsRepository,
	brokers []string,
	topic string,
	groupID string,
) *KafkaConsumer {

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,
	})

	return &KafkaConsumer{
		reader:     reader,
		repository: repo,
	}
}

func (c *KafkaConsumer) Start(ctx context.Context) {

	defer c.reader.Close()

	slog.Info("Kafka consumer started")
	slog.Info("Waiting for SMS events")

	for {

		message, err := c.reader.ReadMessage(ctx)

		if err != nil {

			// Check whether the application is shutting down
			if ctx.Err() != nil {
				slog.Info("Kafka consumer stopping")
				return
			}

			slog.Error(
				"Failed to read Kafka message",
				"error", err,
			)

			continue
		}

		var event model.SmsEvent

		if err := json.Unmarshal(
			message.Value,
			&event,
		); err != nil {

			slog.Error(
				"Failed to parse SMS event",
				"error", err,
				"partition", message.Partition,
				"offset", message.Offset,
			)

			continue
		}

		event.CreatedAt = time.Now()

		if err := c.repository.Save(event); err != nil {

			slog.Error(
				"Failed to save SMS event",
				"error", err,
				"userId", event.UserID,
			)

			continue
		}

		slog.Info(
			"SMS event saved",
			"userId", event.UserID,
			"status", event.Status,
		)
	}
}
