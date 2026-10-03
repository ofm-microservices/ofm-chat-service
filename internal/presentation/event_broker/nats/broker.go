package nats

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"chat-service/config"
	eb "chat-service/internal/presentation/event_broker"
	"github.com/nats-io/nats.go"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
)

type broker struct {
	conn *nats.Conn
	log  logging.Logger
}

// NewBroker connects to NATS and returns the runtime broker adapter.
func NewBroker(cfg config.NATSConfig, log logging.Logger) (eb.EventBroker, error) {
	if log == nil {
		return nil, fmt.Errorf("logger is nil")
	}
	opts := []nats.Option{nats.Timeout(5 * time.Second)}
	if cfg.User != "" || cfg.Password != "" {
		opts = append(opts, nats.UserInfo(cfg.User, cfg.Password))
	}
	conn, err := nats.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, err
	}
	return &broker{conn: conn, log: log.With(logging.String("module", "nats"))}, nil
}

// Publish writes one command or event to NATS.
func (b *broker) Publish(_ context.Context, subject string, payload []byte) error {
	js, err := b.conn.JetStream()
	if err == nil {
		msgID := sha256.Sum256(append([]byte(subject+":"), payload...))
		_, err = js.Publish(subject, payload, nats.MsgId(hex.EncodeToString(msgID[:])))
		if err == nil {
			return nil
		}
	}
	if err := b.conn.Publish(subject, payload); err != nil {
		return err
	}
	return b.conn.Flush()
}

// Subscribe starts a simple synchronous subscription.
func (b *broker) Subscribe(_ context.Context, subject string, handler eb.MessageHandler) error {
	_, err := b.conn.Subscribe(subject, func(msg *nats.Msg) {
		handlerCtx := context.Background()
		err := resilience.Retry(handlerCtx, resilience.RetryPolicyFromEnv(), func(callCtx context.Context, _ int) error {
			return handler(callCtx, msg.Subject, msg.Data)
		})
		if err != nil {
			b.log.Error("message handler exhausted retries", logging.String("subject", msg.Subject), logging.Err(err))
			if publishErr := b.conn.Publish(subject+".dead-letter", msg.Data); publishErr != nil {
				b.log.Error("nats dead-letter publish failed", logging.String("subject", subject), logging.Err(publishErr))
			}
		}
	})
	if err != nil {
		return err
	}
	return b.conn.Flush()
}

// Close releases the NATS connection.
func (b *broker) Close() {
	if b.conn != nil {
		b.conn.Close()
	}
}
