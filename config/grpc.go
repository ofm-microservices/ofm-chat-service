package config

// GRPCConfig defines the chat-service gRPC listener settings.
type GRPCConfig struct {
	Host string `env:"HOST" envDefault:"0.0.0.0"`
	Port int    `env:"PORT" envDefault:"9512"`
}
