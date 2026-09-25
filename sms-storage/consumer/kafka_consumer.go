package consumer

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/durbasmriti/Polyglot-Distributed-SMS-Service/sms-storage/model"
	"github.com/durbasmriti/Polyglot-Distributed-SMS-Service/sms-storage/repository"
	"github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	reader     *kafka.Reader
	repository *repository.SmsRepository
}

func NewKafkaConsumer(
	repo *repository.SmsRepository,
) *KafkaConsumer {

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "sms-events",
		GroupID: "sms-storage-group",
	})

	return &KafkaConsumer{
		reader:     reader,
		repository: repo,
	}
}

func (c *KafkaConsumer) Start() {

	defer c.reader.Close()

	log.Println("Kafka consumer started")
	log.Println("Waiting for SMS events...")

	for {

		message, err := c.reader.ReadMessage(
			context.Background(),
		)

		if err != nil {
			log.Printf(
				"Error reading Kafka message: %v",
				err,
			)
			continue
		}

		var event model.SmsEvent

		if err := json.Unmarshal(
			message.Value,
			&event,
		); err != nil {

			log.Printf(
				"Error parsing SMS event: %v",
				err,
			)

			continue
		}

		event.CreatedAt = time.Now()

		if err := c.repository.Save(event); err != nil {

			log.Printf(
				"Error saving SMS event: %v",
				err,
			)

			continue
		}

		log.Printf(
			"SMS event saved for user: %s",
			event.UserID,
		)
	}
}