package config

import (
	"reflect"
	"testing"
)

func TestLoadDefaultConfig(t *testing.T) {

	t.Setenv("MONGO_URI", "")
	t.Setenv("KAFKA_BROKERS", "")
	t.Setenv("KAFKA_TOPIC", "")
	t.Setenv("KAFKA_GROUP_ID", "")
	t.Setenv("HTTP_PORT", "")

	cfg := Load()

	if cfg.MongoURI != "mongodb://localhost:27017" {
		t.Errorf("Unexpected MongoURI: %s", cfg.MongoURI)
	}

	if !reflect.DeepEqual(
		cfg.KafkaBrokers,
		[]string{"localhost:9092"},
	) {
		t.Errorf("Unexpected KafkaBrokers: %v", cfg.KafkaBrokers)
	}

	if cfg.KafkaTopic != "sms-events" {
		t.Errorf("Unexpected KafkaTopic: %s", cfg.KafkaTopic)
	}

	if cfg.KafkaGroupID != "sms-storage-group" {
		t.Errorf("Unexpected KafkaGroupID: %s", cfg.KafkaGroupID)
	}

	if cfg.HTTPPort != "8081" {
		t.Errorf("Unexpected HTTPPort: %s", cfg.HTTPPort)
	}
}

func TestLoadCustomConfig(t *testing.T) {

	t.Setenv("MONGO_URI", "mongodb://test:27017")
	t.Setenv("KAFKA_BROKERS", "localhost:9092,localhost:9093")
	t.Setenv("KAFKA_TOPIC", "test-events")
	t.Setenv("KAFKA_GROUP_ID", "test-group")
	t.Setenv("HTTP_PORT", "8082")

	cfg := Load()

	if cfg.MongoURI != "mongodb://test:27017" {
		t.Errorf("Unexpected MongoURI: %s", cfg.MongoURI)
	}

	expectedBrokers := []string{
		"localhost:9092",
		"localhost:9093",
	}

	if !reflect.DeepEqual(cfg.KafkaBrokers, expectedBrokers) {
		t.Errorf("Unexpected KafkaBrokers: %v", cfg.KafkaBrokers)
	}

	if cfg.KafkaTopic != "test-events" {
		t.Errorf("Unexpected KafkaTopic: %s", cfg.KafkaTopic)
	}

	if cfg.KafkaGroupID != "test-group" {
		t.Errorf("Unexpected KafkaGroupID: %s", cfg.KafkaGroupID)
	}

	if cfg.HTTPPort != "8082" {
		t.Errorf("Unexpected HTTPPort: %s", cfg.HTTPPort)
	}
}
