package config

// FileServiceConfig defines the outbound gRPC target for file-service.
type FileServiceConfig struct {
	Address string `env:"ADDRESS" envDefault:"127.0.0.1:9504"`
}
