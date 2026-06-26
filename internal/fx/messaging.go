package appfx

import (
	"context"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"chat-service/config"
	app "chat-service/internal/application"
	eb "chat-service/internal/presentation/event_broker"
	broker "chat-service/internal/presentation/event_broker/nats"

	"go.uber.org/fx"
)

// MessagingModule wires the NATS broker adapter.
var MessagingModule = fx.Options(
	fx.Provide(ProvideEventBroker),
)

// ProvideEventBroker constructs the broker runtime.
func ProvideEventBroker(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (app.EventBroker, error) {
	eventBroker, err := broker.NewBroker(cfg.NATS, lg)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error {
		eventBroker.Close()
		return nil
	}})
	return &eventBrokerAdapter{broker: eventBroker}, nil
}

type eventBrokerAdapter struct {
	broker eb.EventBroker
}

func (a *eventBrokerAdapter) Publish(ctx context.Context, subject string, payload []byte) error {
	return a.broker.Publish(ctx, subject, payload)
}

func (a *eventBrokerAdapter) Subscribe(ctx context.Context, subject string, handler app.MessageHandler) error {
	return a.broker.Subscribe(ctx, subject, func(ctx context.Context, subject string, payload []byte) error {
		return handler(ctx, subject, payload)
	})
}

func (a *eventBrokerAdapter) Close() {
	a.broker.Close()
}
