package config

// KafkaConfig defines the Kafka broker and consumer group used by chat-service.
type KafkaConfig struct {
	Brokers           []string `env:"BROKERS" envSeparator:"," envDefault:"127.0.0.1:9092"`
	GroupID           string   `env:"CHAT_GROUP_ID" envDefault:"chat-service"`
	CreateTopic       string   `env:"CHAT_CREATE_TOPIC" envDefault:"chat.create"`
	CloseTopic        string   `env:"CHAT_CLOSE_TOPIC" envDefault:"chat.close"`
	RealtimeTopic     string   `env:"CHAT_REALTIME_TOPIC" envDefault:"realtime"`
	RecoveryTopic     string   `env:"CHAT_RECOVERY_TOPIC" envDefault:"migration.recovery.commands.chat"`
	RecoveryGroup     string   `env:"CHAT_RECOVERY_GROUP" envDefault:"chat-service-recovery"`
	RecoveryCompleted string   `env:"CHAT_RECOVERY_COMPLETED_TOPIC" envDefault:"migration.recovery.completed"`
}
