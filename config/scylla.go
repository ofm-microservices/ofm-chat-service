package config

import "time"

// ScyllaConfig defines the chat-service Scylla connection settings.
type ScyllaConfig struct {
	Hosts                  []string      `env:"HOSTS" envSeparator:"," envDefault:"127.0.0.1"`
	Port                   int           `env:"PORT" envDefault:"9042"`
	Keyspace               string        `env:"KEYSPACE" envDefault:"chat_service"`
	MigrationsPath         string        `env:"MIGRATIONS_PATH" envDefault:"file://migration/scylla"`
	MigrationsTable        string        `env:"MIGRATIONS_TABLE" envDefault:"schema_migrations_chat_service"`
	Username               string        `env:"USERNAME"`
	Password               string        `env:"PASSWORD"`
	Consistency            string        `env:"CONSISTENCY" envDefault:"quorum"`
	ConnectTimeout         time.Duration `env:"CONNECT_TIMEOUT" envDefault:"5s"`
	MaxWaitSchemaAgreement time.Duration `env:"MAX_WAIT_SCHEMA_AGREEMENT" envDefault:"30s"`
	RetryAttempts          int           `env:"RETRY_ATTEMPTS" envDefault:"5"`
	RetryBackoff           time.Duration `env:"RETRY_BACKOFF" envDefault:"1s"`
}
