package config

// NATSConfig defines the NATS subjects owned or consumed by chat-service.
type NATSConfig struct {
	URL      string `env:"URL,required"`
	User     string `env:"USER"`
	Password string `env:"PASSWORD"`

	ChatLifecycleStream string `env:"STREAM_CHAT_LIFECYCLE" envDefault:"CHAT_LIFECYCLE"`
	RealtimeStream      string `env:"STREAM_REALTIME" envDefault:"REALTIME"`

	ChatCreateSubject string `env:"SUBJECT_CHAT_CREATE" envDefault:"chat.create"`
	ChatCloseSubject  string `env:"SUBJECT_CHAT_CLOSE" envDefault:"chat.close"`
	RealtimeSubject   string `env:"SUBJECT_REALTIME" envDefault:"realtime"`
}
