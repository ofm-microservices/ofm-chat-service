package appfx

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"chat-service/config"
	app "chat-service/internal/application"
	events "chat-service/internal/presentation/event_broker/nats"
	grpcsrv "chat-service/internal/presentation/grpc"

	"go.uber.org/fx"
)

// PresentationModule wires transport adapters and lifecycle subscribers.
var PresentationModule = fx.Options(
	fx.Provide(
		ProvideChatLifecycleSubscriber,
		ProvideGRPCServer,
	),
	fx.Invoke(
		InvokeSubscribeChatLifecycle,
		InvokeRunGRPCServer,
	),
)

// ProvideChatLifecycleSubscriber constructs the NATS subscriber that consumes chat lifecycle commands.
func ProvideChatLifecycleSubscriber(broker app.EventBroker, svc app.Service, cfg *config.Config, lg logging.Logger) (events.ChatLifecycleSubscriber, error) {
	return events.NewChatLifecycleSubscriber(broker, svc, cfg.NATS, lg)
}

// InvokeSubscribeChatLifecycle starts the chat lifecycle subscriptions.
func InvokeSubscribeChatLifecycle(lc fx.Lifecycle, sub events.ChatLifecycleSubscriber, lg logging.Logger) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			if err := sub.Subscribe(context.Background()); err != nil {
				lg.Error("subscribe chat lifecycle failed", logging.Err(err))
				return err
			}
			return nil
		},
	})
}

func ProvideGRPCServer(svc app.Service, cfg *config.Config, lg logging.Logger) (grpcsrv.Server, error) {
	return grpcsrv.NewServer(svc, cfg.GRPC, lg)
}

func InvokeRunGRPCServer(lc fx.Lifecycle, srv grpcsrv.Server) {
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error { go func() { _ = srv.Start() }(); return nil },
		OnStop:  func(ctx context.Context) error { return srv.Shutdown(ctx) },
	})
}
