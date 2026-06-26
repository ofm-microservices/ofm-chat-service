package nats

import (
	"context"
	"encoding/json"

	"chat-service/config"
	app "chat-service/internal/application"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

// ChatLifecycleSubscriber consumes chat create/close commands from NATS.
type ChatLifecycleSubscriber interface {
	Subscribe(ctx context.Context) error
}

type chatLifecycleSubscriber struct {
	broker app.EventBroker
	svc    app.Service
	cfg    config.NATSConfig
	log    logging.Logger
}

// NewChatLifecycleSubscriber constructs the chat lifecycle subscriber.
func NewChatLifecycleSubscriber(broker app.EventBroker, svc app.Service, cfg config.NATSConfig, log logging.Logger) (ChatLifecycleSubscriber, error) {
	return &chatLifecycleSubscriber{
		broker: broker,
		svc:    svc,
		cfg:    cfg,
		log:    log.With(logging.String("module", "chat-lifecycle-subscriber")),
	}, nil
}

// Subscribe registers chat create and close subscriptions.
func (s *chatLifecycleSubscriber) Subscribe(ctx context.Context) error {
	if err := s.broker.Subscribe(ctx, s.cfg.ChatCreateSubject, s.handleCreate); err != nil {
		return err
	}
	return s.broker.Subscribe(ctx, s.cfg.ChatCloseSubject, s.handleClose)
}

func (s *chatLifecycleSubscriber) handleCreate(ctx context.Context, _ string, payload []byte) error {
	var cmd app.CreateChatCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return err
	}
	_, err := s.svc.CreateChat(ctx, cmd)
	return err
}

func (s *chatLifecycleSubscriber) handleClose(ctx context.Context, _ string, payload []byte) error {
	var cmd app.CloseChatCommand
	if err := json.Unmarshal(payload, &cmd); err != nil {
		return err
	}
	_, err := s.svc.CloseChat(ctx, cmd)
	return err
}
