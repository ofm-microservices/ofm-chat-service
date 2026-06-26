package appfx

import (
	"context"

	"chat-service/config"
	app "chat-service/internal/application"
	filegrpc "chat-service/internal/infra/file/grpc"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
)

// OutboundModule wires outbound gRPC clients used by chat-service.
var OutboundModule = fx.Options(
	fx.Provide(
		ProvideFileClient,
	),
)

// ProvideFileClient constructs the file-service client used for attachment flows.
func ProvideFileClient(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (app.FileClient, error) {
	client, err := filegrpc.New(cfg.File, lg)
	if err != nil {
		lg.Error("open file service grpc client failed", logging.Err(err))
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(context.Context) error {
			return client.Close()
		},
	})
	return client, nil
}
