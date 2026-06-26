package config

// MetricsConfig defines the Prometheus scrape listener for chat-service.
type MetricsConfig struct {
	Enabled bool   `env:"ENABLED" envDefault:"true"`
	Host    string `env:"HOST" envDefault:"0.0.0.0"`
	Port    int    `env:"PORT" envDefault:"9613"`
	Path    string `env:"PATH" envDefault:"/metrics"`
}
