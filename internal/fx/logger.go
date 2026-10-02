package appfx

import (
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"chat-service/config"

	"go.uber.org/fx"
)

// LoggerModule provides the shared zap logger.
var LoggerModule = fx.Provide(ProvideLogger)

// ProvideLogger constructs the structured logger for the service.
func ProvideLogger(cfg *config.Config) (logging.Logger, error) {
	return logging.NewWithMode(cfg.App.Name, cfg.App.Env, cfg.App.ObservabilityMode, cfg.App.LogLevel)
}
