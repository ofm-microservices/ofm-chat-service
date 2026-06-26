package appfx

import (
	"context"

	"github.com/gocql/gocql"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"chat-service/config"
	scyllastore "chat-service/pkg/storage/scylla"

	"go.uber.org/fx"
)

// StorageModule provides the Scylla session used by the chat.
var StorageModule = fx.Options(
	fx.Invoke(InvokeRunMigrations),
	fx.Provide(ProvideScyllaSession),
)

// InvokeRunMigrations applies the saga's CQL migrations before the session is opened.
func InvokeRunMigrations(cfg *config.Config, lg logging.Logger) error {
	if err := scyllastore.RunMigrations(cfg.Scylla, lg); err != nil {
		lg.Error("run migrations failed", logging.Err(err))
		return err
	}
	lg.Info("migrations applied")
	return nil
}

// ProvideScyllaSession connects to Scylla after migrations have been applied.
func ProvideScyllaSession(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (*gocql.Session, error) {
	session, err := scyllastore.ConnectAndEnsureSchema(cfg.Scylla, lg)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error { session.Close(); return nil }})
	return session, nil
}
