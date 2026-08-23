package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"chat-service/config"
	eb "chat-service/internal/presentation/event_broker"
	kafkaprop "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
	requestmetadata "github.com/ofm-microservices/ofm-common/pkg/observability/metadata"
	sharedmetrics "github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	"github.com/ofm-microservices/ofm-common/pkg/resilience"
	"github.com/segmentio/kafka-go"
)

type broker struct {
	brokers           []string
	group, deadLetter string
	mu                sync.Mutex
	readers           []*kafka.Reader
}

// NewBroker constructs the Kafka broker used by chat-service.
func NewBroker(cfg config.KafkaConfig) (eb.EventBroker, error) {
	if len(cfg.Brokers) == 0 {
		return nil, errors.New("kafka brokers are empty")
	}
	return &broker{brokers: cfg.Brokers, group: cfg.GroupID, deadLetter: cfg.GroupID + ".dead-letter"}, nil
}

func (b *broker) Publish(ctx context.Context, subject string, payload []byte) error {
	w := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: subject}
	defer w.Close()
	headers := make([]kafka.Header, 0, 5)
	for key, value := range requestmetadata.OutgoingHeaders(ctx) {
		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	return w.WriteMessages(ctx, kafka.Message{Value: payload, Headers: headers})
}

func (b *broker) Subscribe(ctx context.Context, subject string, handler eb.MessageHandler) error {
	group := strings.TrimSpace(b.group) + "-" + strings.TrimSpace(subject)
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: b.brokers, Topic: subject, GroupID: group, MinBytes: 1, MaxBytes: 10e6})
	b.mu.Lock()
	b.readers = append(b.readers, r)
	b.mu.Unlock()
	defer r.Close()
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			return err
		}
		attempts := 0
		err = resilience.Retry(ctx, resilience.RetryPolicyFromEnv(), func(attemptCtx context.Context, attempt int) error {
			attempts = attempt
			return handler(kafkaprop.Context(attemptCtx, msg.Headers), subject, msg.Value)
		})
		if err != nil {
			payload, marshalErr := resilience.MarshalDLQ(resilience.DLQRecord{OriginalKey: msg.Key, OriginalValue: msg.Value, OriginalTopic: subject, OriginalPartition: msg.Partition, OriginalOffset: msg.Offset, Attempts: attempts, ErrorClass: fmt.Sprintf("%T", err), Error: err.Error(), FailedAt: time.Now().UTC()})
			if marshalErr != nil {
				return marshalErr
			}
			writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: b.deadLetter, WriteTimeout: 5 * time.Second}
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			dlqErr := writer.WriteMessages(writeCtx, kafka.Message{Key: msg.Key, Value: payload, Headers: msg.Headers})
			cancel()
			_ = writer.Close()
			if dlqErr != nil {
				return dlqErr
			}
			sharedmetrics.IncKafkaDLQ(b.deadLetter)
		}
		if err := r.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

func (b *broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.readers {
		_ = r.Close()
	}
}
