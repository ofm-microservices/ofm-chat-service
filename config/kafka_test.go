package config

import "testing"

func TestKafkaConfigReadsPrefixedEnvironment(t *testing.T) {
	t.Setenv("KAFKA_BROKERS", "broker-a:9092,broker-b:9092")
	t.Setenv("KAFKA_CHAT_GROUP_ID", "chat-test")
	t.Setenv("KAFKA_CHAT_CREATE_TOPIC", "chat.created.test")
	t.Setenv("CURSOR_SECRET", "test-cursor-secret")
	t.Setenv("CRYPTO_SECRET", "test-crypto-secret")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if got := len(cfg.Kafka.Brokers); got != 2 || cfg.Kafka.Brokers[0] != "broker-a:9092" {
		t.Fatalf("unexpected brokers: %#v", cfg.Kafka.Brokers)
	}
	if cfg.Kafka.GroupID != "chat-test" || cfg.Kafka.CreateTopic != "chat.created.test" {
		t.Fatalf("unexpected kafka config: %+v", cfg.Kafka)
	}
}
