package appfx

import (
	"chat-service/config"

	"go.uber.org/fx"
)

// ConfigModule loads the environment-backed configuration.
var ConfigModule = fx.Provide(config.Load)
