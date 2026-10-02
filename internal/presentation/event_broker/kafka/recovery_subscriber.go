package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"chat-service/config"
	app "chat-service/internal/application"
	"chat-service/internal/domain"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/migration/events"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
)

// RecoverySubscriber applies chat fallback commands through chat application use-cases.
type RecoverySubscriber interface{ Subscribe(context.Context) error }
type recoverySubscriber struct {
	broker app.EventBroker
	svc    app.Service
	cfg    config.KafkaConfig
	log    logging.Logger
}

// NewRecoverySubscriber constructs the chat recovery Kafka adapter.
func NewRecoverySubscriber(b app.EventBroker, svc app.Service, cfg config.KafkaConfig, log logging.Logger) (RecoverySubscriber, error) {
	if b == nil || svc == nil || log == nil {
		return nil, errors.New("invalid chat recovery subscriber dependency")
	}
	return &recoverySubscriber{broker: b, svc: svc, cfg: cfg, log: log.With(logging.String("module", "kafka-chat-recovery-subscriber"))}, nil
}
func (s *recoverySubscriber) Subscribe(ctx context.Context) error {
	return s.broker.Subscribe(ctx, s.cfg.RecoveryTopic, s.handle)
}
func (s *recoverySubscriber) handle(ctx context.Context, _ string, raw []byte) error {
	var cmd events.Envelope
	if err := json.Unmarshal(raw, &cmd); err != nil {
		return fmt.Errorf("decode chat recovery command: %w", err)
	}
	if !strings.EqualFold(cmd.AggregateType, "chat") {
		return fmt.Errorf("unsupported chat recovery aggregate_type=%q", cmd.AggregateType)
	}
	operation := strings.ToLower(cmd.Operation)
	// The monolith uses HTTP POST for both chat creation and message creation.
	// The route is the authoritative discriminator when the recovery envelope
	// has no finer-grained operation value.
	if operation == "post" && strings.Contains(cmd.CommandPath, "/chat/messages") {
		operation = "message"
	}
	var result any
	var err error
	switch operation {
	case "post", "create":
		var c app.CreateChatCommand
		err = json.Unmarshal(cmd.Payload, &c)
		if err == nil {
			for field, value := range map[string]string{"order_id": c.OrderID, "buyer_id": c.BuyerID, "seller_id": c.SellerID} {
				if strings.TrimSpace(value) == "" {
					err = resilience.Permanent(fmt.Errorf("chat recovery command %s is missing %s", cmd.CommandID, field))
					break
				}
			}
		}
		if err == nil {
			result, err = s.svc.CreateChat(ctx, c)
		}
	case "delete", "close":
		var c app.CloseChatCommand
		err = json.Unmarshal(cmd.Payload, &c)
		if err == nil {
			result, err = s.svc.CloseChat(ctx, c)
		}
	case "message":
		var payload struct {
			OrderID       string   `json:"order_id"`
			MessageID     string   `json:"message_id"`
			UserID        string   `json:"user_id"`
			Text          string   `json:"text"`
			AttachmentIDs []string `json:"attachment_ids"`
			SellerID      string   `json:"seller_id"`
		}
		var c app.CreateMessageCommand
		err = json.Unmarshal(cmd.Payload, &payload)
		if err == nil {
			// Monolith fallback stores the authenticated principal in the
			// envelope metadata rather than duplicating it in the small message
			// payload. Preserve the same application command while normalizing
			// that transport shape at the recovery boundary.
			if c.UserID == "" {
				c.UserID = cmd.RecoveryPrincipalID
			}
			orderID := payload.OrderID
			if orderID == "" {
				// The monolith route carries the order identity in the command
				// path for message recovery, while its JSON body contains only
				// message content.
				const marker = "/orders/"
				if start := strings.Index(cmd.CommandPath, marker); start >= 0 {
					orderID = strings.SplitN(cmd.CommandPath[start+len(marker):], "/", 2)[0]
				}
			}
			c = app.CreateMessageCommand{OrderID: orderID, MessageID: payload.MessageID, UserID: payload.UserID, Text: payload.Text, AttachmentIDs: payload.AttachmentIDs}
			if strings.TrimSpace(c.MessageID) == "" {
				err = resilience.Permanent(fmt.Errorf("chat recovery command %s is missing message_id", cmd.CommandID))
			}
			if c.UserID == "" {
				c.UserID = cmd.RecoveryPrincipalID
			}
			if _, lookupErr := s.svc.GetOrderChat(ctx, app.GetOrderChatCommand{OrderID: c.OrderID, UserID: c.UserID, Limit: 1}); errors.Is(lookupErr, domain.ErrChatNotFound) && payload.SellerID != "" && payload.SellerID != c.UserID {
				_, err = s.svc.CreateChat(ctx, app.CreateChatCommand{OrderID: c.OrderID, BuyerID: c.UserID, SellerID: payload.SellerID})
			}
		}
		if err == nil {
			result, err = s.svc.CreateMessage(ctx, c)
		}
	default:
		return fmt.Errorf("unsupported chat recovery operation=%s", cmd.Operation)
	}
	if err != nil {
		s.log.Error("chat recovery application command failed", logging.String("command_id", cmd.CommandID), logging.String("operation", cmd.Operation), logging.Err(err))
		return err
	}
	completed := events.Envelope{EventID: cmd.EventID + ".completed", CommandID: cmd.CommandID, CorrelationID: cmd.CorrelationID, CausationID: cmd.EventID, IdempotencyKey: cmd.IdempotencyKey, TestRunID: cmd.TestRunID, EventType: "migration.recovery.completed", Operation: cmd.Operation, SchemaVersion: 1, AggregateType: "chat", AggregateID: cmd.AggregateID, SourceService: "chat-service-recovery", OccurredAt: time.Now().UTC(), Payload: marshal(result)}
	body, e := json.Marshal(completed)
	if e != nil {
		return e
	}
	if err := s.broker.Publish(context.WithoutCancel(ctx), s.cfg.RecoveryCompleted, body); err != nil {
		s.log.Error("chat recovery completion publish failed", logging.String("command_id", cmd.CommandID), logging.String("topic", s.cfg.RecoveryCompleted), logging.Err(err))
		return fmt.Errorf("publish chat recovery completion: %w", err)
	}
	s.log.Info("chat recovery command completed", logging.String("command_id", cmd.CommandID), logging.String("topic", s.cfg.RecoveryCompleted))
	return nil
}
func marshal(v any) []byte { b, _ := json.Marshal(v); return b }
