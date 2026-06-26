package appfx

import "go.uber.org/fx"

// Module keeps the compatibility bundle for the service.
var Module = fx.Options(
	ConfigModule,
	LoggerModule,
	MetricsModule,
	AppModule,
	StorageModule,
	RepoModule,
	MessagingModule,
	OutboundModule,
	ServiceModule,
	PresentationModule,
)
