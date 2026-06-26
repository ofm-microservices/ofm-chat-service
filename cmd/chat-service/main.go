package main

import (
	appfx "chat-service/internal/fx"

	"go.uber.org/fx"
)

var newApp = fx.New
var runApp = (*fx.App).Run

func main() {
	runApp(newApp(
		appfx.ConfigModule,
		appfx.LoggerModule,
		appfx.MetricsModule,
		appfx.AppModule,
		appfx.StorageModule,
		appfx.RepoModule,
		appfx.MessagingModule,
		appfx.OutboundModule,
		appfx.ServiceModule,
		appfx.PresentationModule,
	))
}
