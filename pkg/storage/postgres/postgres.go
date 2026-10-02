package postgres

import (
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"chat-service/config"

	"github.com/XSAM/otelsql"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"go.opentelemetry.io/otel/attribute"
)

// Open connects to the PostgreSQL database owned by chat-service.
func Open(cfg config.DBConfig) (*sqlx.DB, error) {
	driver, err := otelsql.Register("pgx", otelsql.WithAttributes(attribute.String("db.system", "postgresql"), attribute.String("db.namespace", cfg.Name)))
	if err != nil {
		return nil, err
	}
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", url.QueryEscape(cfg.User), url.QueryEscape(cfg.Password), cfg.Host, cfg.Port, url.QueryEscape(cfg.Name), url.QueryEscape(cfg.SSLMode))
	db, err := sqlx.Connect(driver, dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(cfg.MaxOpenConns)
	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	db.SetConnMaxIdleTime(2 * time.Minute)
	return db, nil
}

// RunMigrations applies the chat-service PostgreSQL migrations.
func RunMigrations(cfg config.DBConfig) error {
	path := cfg.MigrationsPath
	if p, ok := strings.CutPrefix(path, "file://"); ok && !filepath.IsAbs(p) {
		abs, err := filepath.Abs(p)
		if err != nil {
			return err
		}
		path = "file://" + abs
	}
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s&x-migrations-table=%s", url.QueryEscape(cfg.User), url.QueryEscape(cfg.Password), cfg.Host, cfg.Port, url.QueryEscape(cfg.Name), url.QueryEscape(cfg.SSLMode), url.QueryEscape(cfg.MigrationsTable))
	m, err := migrate.New(path, dsn)
	if err != nil {
		return err
	}
	defer m.Close()
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return err
	}
	return nil
}
