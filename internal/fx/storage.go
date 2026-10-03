package appfx

import (
	"chat-service/config"
	pgstore "chat-service/pkg/storage/postgres"
	"context"
	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
)

// StorageModule provides the PostgreSQL database used by chat-service.
var StorageModule = fx.Options(fx.Invoke(InvokeRunMigrations), fx.Provide(ProvidePostgresDB))

// InvokeRunMigrations applies the chat PostgreSQL schema before repositories start.
func InvokeRunMigrations(cfg *config.Config, lg logging.Logger) error {
	if err := pgstore.RunMigrations(cfg.DB); err != nil {
		lg.Error("run PostgreSQL migrations failed", logging.Err(err))
		return err
	}
	lg.Info("PostgreSQL migrations applied")
	return nil
}

// ProvidePostgresDB opens the service-owned PostgreSQL connection pool.
func ProvidePostgresDB(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (*sqlx.DB, error) {
	db, err := pgstore.Open(cfg.DB)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { return db.Close() }})
	lg.Info("PostgreSQL connected")
	return db, nil
}
