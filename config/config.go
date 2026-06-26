package config

import "github.com/caarlos0/env/v11"

// Config groups the environment-backed settings owned by chat-service.
type Config struct {
	App     AppConfig         `envPrefix:"APP_"`
	GRPC    GRPCConfig        `envPrefix:"GRPC_"`
	Metrics MetricsConfig     `envPrefix:"METRICS_"`
	Scylla  ScyllaConfig      `envPrefix:"SCYLLA_"`
	Cursor  CursorConfig      `envPrefix:"CURSOR_"`
	Crypto  CryptoConfig      `envPrefix:"CRYPTO_"`
	File    FileServiceConfig `envPrefix:"FILE_SERVICE_"`
	NATS    NATSConfig        `envPrefix:"NATS_"`
}

// Load parses the service config from environment variables.
func Load() (*Config, error) {
	var cfg Config
	if err := env.Parse(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}
