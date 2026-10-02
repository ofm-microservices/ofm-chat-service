package appfx

import (
	"context"

	"chat-service/config"
	app "chat-service/internal/application"
	eb "chat-service/internal/presentation/event_broker"
	broker "chat-service/internal/presentation/event_broker/kafka"
	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/idempotency"
	"github.com/ofm-microservices/ofm-common/pkg/logging"

	"go.uber.org/fx"
)

// MessagingModule wires the Kafka broker adapter.
var MessagingModule = fx.Options(
	fx.Provide(ProvideEventBrokerWithStore),
)

// ProvideEventBroker constructs the broker runtime.
func ProvideEventBroker(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger) (app.EventBroker, error) {
	return provideEventBroker(lc, cfg, lg, nil)
}

// ProvideEventBrokerWithStore wires Kafka with durable PostgreSQL event claims.
func ProvideEventBrokerWithStore(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger, db *sqlx.DB) (app.EventBroker, error) {
	return provideEventBroker(lc, cfg, lg, &sqlEventStore{db: db})
}

type sqlEventStore struct{ db *sqlx.DB }

func (s *sqlEventStore) Claim(ctx context.Context, event idempotency.Event) (bool, error) {
	return idempotency.ClaimDB(ctx, s.db, event)
}

func (s *sqlEventStore) Release(ctx context.Context, eventID string) error {
	return idempotency.Release(ctx, s.db, eventID)
}

func provideEventBroker(lc fx.Lifecycle, cfg *config.Config, lg logging.Logger, store idempotency.Store) (app.EventBroker, error) {
	eventBroker, err := broker.NewBroker(cfg.Kafka)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{OnStop: func(context.Context) error {
		eventBroker.Close()
		return nil
	}})
	return &eventBrokerAdapter{broker: eventBroker, store: store}, nil
}

type eventBrokerAdapter struct {
	broker eb.EventBroker
	store  idempotency.Store
}

func (a *eventBrokerAdapter) Publish(ctx context.Context, subject string, payload []byte) error {
	return a.broker.Publish(ctx, subject, payload)
}

func (a *eventBrokerAdapter) Subscribe(ctx context.Context, subject string, handler app.MessageHandler) error {
	return a.broker.Subscribe(ctx, subject, func(ctx context.Context, subject string, payload []byte) error {
		if a.store != nil {
			event := idempotency.DecodeOrFingerprint(subject, payload)
			claimed, err := a.store.Claim(ctx, event)
			if err != nil {
				return err
			}
			if !claimed {
				return nil
			}
			if err := handler(ctx, subject, payload); err != nil {
				_ = a.store.Release(ctx, event.EventID)
				return err
			}
			return nil
		}
		return handler(ctx, subject, payload)
	})
}

func (a *eventBrokerAdapter) Close() {
	a.broker.Close()
}
