package scylla

import (
	"errors"
	"fmt"
)

// ErrNilLogger is returned when schema bootstrap logging is requested without a logger.
var ErrNilLogger = errors.New("logger is nil")

// WrapCreateClusterSessionError annotates Scylla cluster session creation failures.
func WrapCreateClusterSessionError(err error) error {
	return fmt.Errorf("create cluster session: %w", err)
}

// WrapEnsureSchemaError annotates Scylla schema creation failures.
func WrapEnsureSchemaError(err error) error { return fmt.Errorf("ensure schema: %w", err) }

// WrapResolveMigrationsPathError annotates migration path resolution failures.
func WrapResolveMigrationsPathError(err error) error {
	return fmt.Errorf("resolve migrations path: %w", err)
}

// WrapRunMigrationsError annotates CQL migration execution failures.
func WrapRunMigrationsError(err error) error { return fmt.Errorf("run migrations: %w", err) }
