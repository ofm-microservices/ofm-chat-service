package appfx

import (
	"context"
	"time"

	"chat-service/config"
	app "chat-service/internal/application"
	events "chat-service/internal/presentation/event_broker/kafka"
	grpcsrv "chat-service/internal/presentation/grpc"
	"github.com/ofm-microservices/ofm-common/pkg/logging"

	"go.uber.org/fx"
)

// PresentationModule wires transport adapters and lifecycle subscribers.
var PresentationModule = fx.Options(
	fx.Provide(
		ProvideChatLifecycleSubscriber,
		ProvideRecoverySubscriber,
		ProvideGRPCServer,
	),
	fx.Invoke(
		InvokeSubscribeChatLifecycle,
		InvokeSubscribeRecovery,
		InvokeRunGRPCServer,
	),
)

// ProvideRecoverySubscriber constructs the chat-owned migration consumer.
func ProvideRecoverySubscriber(broker app.EventBroker, svc app.Service, cfg *config.Config, lg logging.Logger) (events.RecoverySubscriber, error) {
	return events.NewRecoverySubscriber(broker, svc, cfg.Kafka, lg)
}

// InvokeSubscribeRecovery starts chat recovery consumption during startup.
func InvokeSubscribeRecovery(lc fx.Lifecycle, sub events.RecoverySubscriber, lg logging.Logger) {
	var cancel context.CancelFunc
	lc.Append(fx.Hook{OnStart: func(context.Context) error {
		ctx, stop := context.WithCancel(context.Background())
		cancel = stop
		go func() {
			for ctx.Err() == nil {
				if err := sub.Subscribe(ctx); err != nil && ctx.Err() == nil {
					lg.Error("chat recovery consumer stopped; retrying", logging.Err(err))
					timer := time.NewTimer(time.Second)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
					}
				}
			}
		}()
		return nil
	}, OnStop: func(context.Context) error {
		if cancel != nil {
			cancel()
		}
		return nil
	}})
}

// ProvideChatLifecycleSubscriber constructs the Kafka subscriber that consumes chat lifecycle commands.
func ProvideChatLifecycleSubscriber(broker app.EventBroker, svc app.Service, cfg *config.Config, lg logging.Logger) (events.ChatLifecycleSubscriber, error) {
	return events.NewChatLifecycleSubscriber(broker, svc, cfg.Kafka, lg)
}

// InvokeSubscribeChatLifecycle starts the chat lifecycle subscriptions.
func InvokeSubscribeChatLifecycle(lc fx.Lifecycle, sub events.ChatLifecycleSubscriber, lg logging.Logger) {
	var cancel context.CancelFunc
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			runCtx, runCancel := context.WithCancel(context.Background())
			cancel = runCancel
			go func() {
				if err := sub.Subscribe(runCtx); err != nil && runCtx.Err() == nil {
					lg.Error("subscribe chat lifecycle failed", logging.Err(err))
				}
			}()
			return nil
		},
		OnStop: func(context.Context) error {
			if cancel != nil {
				cancel()
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
