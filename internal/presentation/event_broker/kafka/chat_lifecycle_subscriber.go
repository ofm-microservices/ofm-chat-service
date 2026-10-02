package kafka

import (
	"context"
	"encoding/json"
	"errors"

	"chat-service/config"
	app "chat-service/internal/application"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

// ChatLifecycleSubscriber consumes chat lifecycle commands from Kafka.
type ChatLifecycleSubscriber interface{ Subscribe(context.Context) error }

type chatLifecycleSubscriber struct {
	broker app.EventBroker
	svc    app.Service
	cfg    config.KafkaConfig
	log    logging.Logger
}

type chatCommandEvent struct {
	Payload json.RawMessage `json:"payload"`
}

// NewChatLifecycleSubscriber constructs the Kafka chat lifecycle subscriber.
func NewChatLifecycleSubscriber(broker app.EventBroker, svc app.Service, cfg config.KafkaConfig, log logging.Logger) (ChatLifecycleSubscriber, error) {
	if broker == nil || svc == nil || log == nil {
		return nil, errors.New("invalid chat lifecycle subscriber dependency")
	}
	return &chatLifecycleSubscriber{broker: broker, svc: svc, cfg: cfg, log: log.With(logging.String("module", "chat-lifecycle-subscriber"))}, nil
}

func (s *chatLifecycleSubscriber) Subscribe(ctx context.Context) error {
	for _, subscription := range []struct {
		topic   string
		handler app.MessageHandler
	}{{s.cfg.CreateTopic, s.handleCreate}, {s.cfg.CloseTopic, s.handleClose}} {
		go func(subscription struct {
			topic   string
			handler app.MessageHandler
		}) {
			if err := s.broker.Subscribe(ctx, subscription.topic, subscription.handler); err != nil && ctx.Err() == nil {
				s.log.Error("chat lifecycle Kafka consumer stopped", logging.String("topic", subscription.topic), logging.Err(err))
			}
		}(subscription)
	}
	return nil
}
func (s *chatLifecycleSubscriber) handleCreate(ctx context.Context, _ string, p []byte) error {
	var c app.CreateChatCommand
	if err := json.Unmarshal(p, &c); err != nil {
		return err
	}
	if c.OrderID == "" || c.BuyerID == "" || c.SellerID == "" {
		var event chatCommandEvent
		if err := json.Unmarshal(p, &event); err != nil {
			return err
		}
		if len(event.Payload) > 0 && string(event.Payload) != "null" {
			if err := json.Unmarshal(event.Payload, &c); err != nil {
				return err
			}
		}
	}
	_, err := s.svc.CreateChat(ctx, c)
	return err
}
func (s *chatLifecycleSubscriber) handleClose(ctx context.Context, _ string, p []byte) error {
	var c app.CloseChatCommand
	if err := json.Unmarshal(p, &c); err != nil {
		return err
	}
	if c.OrderID == "" || c.CloseReason == "" {
		var event chatCommandEvent
		if err := json.Unmarshal(p, &event); err != nil {
			return err
		}
		if len(event.Payload) > 0 && string(event.Payload) != "null" {
			if err := json.Unmarshal(event.Payload, &c); err != nil {
				return err
			}
		}
	}
	_, err := s.svc.CloseChat(ctx, c)
	return err
}
