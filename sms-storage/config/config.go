package config

import (
	"os"
	"strings"
)

type Config struct {
	MongoURI     string
	KafkaBrokers []string
	KafkaTopic   string
	KafkaGroupID string
	HTTPPort     string
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)

	if value == "" {
		return fallback
	}

	return value
}

func Load() *Config {

	brokers := getEnv(
		"KAFKA_BROKERS",
		"localhost:9092",
	)

	brokerList := strings.Split(brokers, ",")

	for i, broker := range brokerList {
		brokerList[i] = strings.TrimSpace(broker)
	}

	return &Config{
		MongoURI: getEnv(
			"MONGO_URI",
			"mongodb://localhost:27017",
		),

		KafkaBrokers: brokerList,

		KafkaTopic: getEnv(
			"KAFKA_TOPIC",
			"sms-events",
		),

		KafkaGroupID: getEnv(
			"KAFKA_GROUP_ID",
			"sms-storage-group",
		),

		HTTPPort: getEnv(
			"HTTP_PORT",
			"8081",
		),
	}
}
