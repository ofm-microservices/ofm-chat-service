package config

// AppConfig defines runtime metadata for the service process.
type AppConfig struct {
	Name     string `env:"NAME" envDefault:"chat-service"`
	Env      string `env:"ENV" envDefault:"local"`
	LogLevel string `env:"LOG_LEVEL" envDefault:"info"`
}
