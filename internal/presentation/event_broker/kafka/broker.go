package kafka

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"chat-service/config"
	eb "chat-service/internal/presentation/event_broker"
	commonevents "github.com/ofm-microservices/ofm-common/pkg/events"
	kafkaprop "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
	transportkafka "github.com/ofm-microservices/ofm-common/pkg/observability/kafka"
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
	enveloped, _, err := commonevents.Wrap(subject, payload)
	if err != nil {
		return err
	}
	w := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: subject, BatchSize: 100, BatchTimeout: 50 * time.Millisecond}
	defer w.Close()
	headers := make([]kafka.Header, 0, 5)
	for key, value := range requestmetadata.OutgoingHeaders(ctx) {
		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	transportkafka.Published(subject, enveloped)
	return w.WriteMessages(ctx, kafka.Message{Value: enveloped, Headers: headers})
}

func (b *broker) Subscribe(ctx context.Context, subject string, handler eb.MessageHandler) error {
	group := strings.TrimSpace(b.group) + "-" + strings.TrimSpace(subject)
	if strings.HasPrefix(subject, "migration.recovery.commands.") {
		group = "chat-service-recovery"
	}
	go func() {
		if err := (resilience.KafkaRetryQueueConfig{Brokers: b.brokers, Group: group, MaxAttempts: resilience.DefaultRetryPolicy.MaxAttempts}).Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("chat Kafka retry worker stopped: %v", err)
		}
	}()
	r := kafka.NewReader(kafka.ReaderConfig{Brokers: b.brokers, Topic: subject, GroupID: group, MinBytes: 1, MaxBytes: 10e6, MaxWait: 50 * time.Millisecond})
	b.mu.Lock()
	b.readers = append(b.readers, r)
	b.mu.Unlock()
	defer r.Close()
	for {
		msg, err := r.FetchMessage(ctx)
		if err != nil {
			return err
		}
		attempts := retryAttempt(msg.Headers)
		payload, _, unwrapErr := commonevents.Unwrap(msg.Value)
		if unwrapErr != nil {
			return b.deadLetterMessage(ctx, subject, msg, attempts, unwrapErr)
		}
		transportkafka.Consumed(subject, msg.Partition, msg.Offset, attempts, payload)
		if strings.HasPrefix(subject, "migration.recovery.commands.") {
			payload = msg.Value
		}
		err = handler(kafkaprop.Context(ctx, msg.Headers), subject, payload)
		if err != nil {
			var permanent resilience.PermanentError
			if !errors.As(err, &permanent) && attempts < resilience.DefaultRetryPolicy.MaxAttempts {
				writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: resilience.RetryTopic(group), WriteTimeout: 5 * time.Second}
				queueErr := (resilience.KafkaRetryQueue{Writer: writer}).Enqueue(ctx, msg, subject, attempts+1, err)
				_ = writer.Close()
				if queueErr != nil {
					return queueErr
				}
				continue
			}
			return b.deadLetterMessage(ctx, subject, msg, attempts, err)
		}
		if err := r.CommitMessages(ctx, msg); err != nil {
			return err
		}
	}
}

func retryAttempt(headers []kafka.Header) int {
	for _, header := range headers {
		if header.Key == "x-ofm-retry-attempt" {
			if n, err := strconv.Atoi(string(header.Value)); err == nil && n > 0 {
				return n
			}
		}
	}
	return 1
}

func (b *broker) deadLetterMessage(ctx context.Context, subject string, msg kafka.Message, attempts int, cause error) error {
	deadLetterTopic := b.deadLetter
	if strings.HasPrefix(subject, "migration.recovery.commands.") {
		deadLetterTopic = "migration.recovery.commands.dlq"
	}
	payload, err := resilience.MarshalDLQ(resilience.DLQRecord{OriginalKey: msg.Key, OriginalValue: msg.Value, OriginalTopic: subject, OriginalPartition: msg.Partition, OriginalOffset: msg.Offset, Attempts: attempts, ErrorClass: fmt.Sprintf("%T", cause), Error: cause.Error(), FailedAt: time.Now().UTC()})
	if err != nil {
		return err
	}
	writer := &kafka.Writer{Addr: kafka.TCP(b.brokers...), Topic: deadLetterTopic, WriteTimeout: 5 * time.Second}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	err = writer.WriteMessages(writeCtx, kafka.Message{Key: msg.Key, Value: payload, Headers: msg.Headers})
	cancel()
	_ = writer.Close()
	if err != nil {
		return err
	}
	sharedmetrics.IncKafkaDLQ(deadLetterTopic)
	return nil
}

func (b *broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, r := range b.readers {
		_ = r.Close()
	}
}
