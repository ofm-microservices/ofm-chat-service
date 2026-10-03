package application

import (
	"context"
	"encoding/json"
	"time"

	"chat-service/internal/domain"

	"github.com/ofm-microservices/ofm-common/pkg/cursor"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

// Service owns chat lifecycle, messaging, and order-scoped access control.
type Service interface {
	CreateChat(ctx context.Context, cmd CreateChatCommand) (*domain.Chat, error)
	CloseChat(ctx context.Context, cmd CloseChatCommand) (*domain.Chat, error)
	GetOrderChat(ctx context.Context, cmd GetOrderChatCommand) (*GetOrderChatResult, error)
	CreateMessage(ctx context.Context, cmd CreateMessageCommand) (*MessageView, error)
	EditMessage(ctx context.Context, cmd EditMessageCommand) (*MessageView, error)
	DeleteMessage(ctx context.Context, cmd DeleteMessageCommand) (*MessageView, error)
	CreateAttachmentUploadURL(ctx context.Context, cmd CreateAttachmentUploadURLCommand) (*CreateAttachmentUploadURLResult, error)
	CompleteAttachmentUpload(ctx context.Context, cmd CompleteAttachmentUploadCommand) (*AttachmentView, error)
}

// EventBroker abstracts NATS publishing and subscriptions.
type EventBroker interface {
	Publish(ctx context.Context, subject string, payload []byte) error
	Subscribe(ctx context.Context, subject string, handler MessageHandler) error
	Close()
}

// MessageHandler processes one broker payload.
type MessageHandler func(ctx context.Context, subject string, payload []byte) error

// FileClient abstracts the generic presigned-upload flow owned by file-service.
type FileClient interface {
	CreateDirectUpload(ctx context.Context, cmd CreateDirectUploadCommand) (*DirectUploadReservation, error)
	CompleteDirectUpload(ctx context.Context, fileID string) (*StoredFile, error)
	GetFileURLs(ctx context.Context, fileIDs []string) (map[string]string, error)
	Close() error
}

// MessageCipher encrypts and decrypts chat message text at rest.
type MessageCipher interface {
	Encrypt(plain string) (string, error)
	Decrypt(ciphertext string) (string, error)
}

// CursorCodec aliases the shared opaque cursor contract.
type CursorCodec = cursor.Codec

// Logger aliases the shared structured logger used by the application layer.
type Logger = logging.Logger

// CreateChatCommand creates the order-scoped chat after order creation.
type CreateChatCommand struct {
	OrderID  string `json:"order_id"`
	BuyerID  string `json:"buyer_id"`
	SellerID string `json:"seller_id"`
}

// CloseChatCommand closes an order chat once the order reaches a terminal state.
type CloseChatCommand struct {
	OrderID     string `json:"order_id"`
	CloseReason string `json:"close_reason"`
}

// GetOrderChatCommand loads one order chat timeline for a participant.
type GetOrderChatCommand struct {
	OrderID string
	UserID  string
	Cursor  string
	Limit   int
}

// CreateMessageCommand stores one new message in an open chat.
type CreateMessageCommand struct {
	OrderID       string
	MessageID     string
	UserID        string
	Text          string
	AttachmentIDs []string
}

// EditMessageCommand updates one existing message text.
type EditMessageCommand struct {
	OrderID   string
	UserID    string
	MessageID string
	Text      string
}

// DeleteMessageCommand soft-deletes one existing message.
type DeleteMessageCommand struct {
	OrderID   string
	UserID    string
	MessageID string
}

// CreateAttachmentUploadURLCommand reserves one direct upload for a chat attachment.
type CreateAttachmentUploadURLCommand struct {
	OrderID     string
	UserID      string
	Filename    string
	ContentType string
	SizeBytes   int64
}

// CompleteAttachmentUploadCommand finalizes one attachment upload in file-service.
type CompleteAttachmentUploadCommand struct {
	OrderID string
	UserID  string
	FileID  string
}

// DirectUploadReservation is the file-service upload reservation returned to the gateway.
type DirectUploadReservation struct {
	FileID    string
	UploadURL string
	Method    string
	Headers   map[string]string
	ExpiresAt time.Time
}

// CreateDirectUploadCommand requests one generic file upload reservation.
type CreateDirectUploadCommand struct {
	OwnerID     string
	Filename    string
	ContentType string
	SizeBytes   int64
}

// StoredFile is the finalized file-service record returned after upload completion.
type StoredFile struct {
	FileID      string
	Filename    string
	ContentType string
	SizeBytes   int64
	URL         string
}

// AttachmentView is the transport-facing attachment payload returned by chat-service.
type AttachmentView struct {
	FileID      string            `json:"file_id"`
	Filename    string            `json:"filename"`
	ContentType string            `json:"content_type"`
	SizeBytes   int64             `json:"size_bytes"`
	URL         string            `json:"url"`
	Headers     map[string]string `json:"headers,omitempty"`
	UploadURL   string            `json:"upload_url,omitempty"`
	Method      string            `json:"method,omitempty"`
	ExpiresAt   string            `json:"expires_at,omitempty"`
}

// MessageView is the transport-facing chat message payload.
type MessageView struct {
	MessageID    string           `json:"message_id"`
	OrderID      string           `json:"order_id"`
	SenderUserID string           `json:"sender_user_id"`
	MessageType  string           `json:"message_type"`
	Text         string           `json:"text"`
	Deleted      bool             `json:"deleted"`
	Edited       bool             `json:"edited"`
	Attachments  []AttachmentView `json:"attachments"`
	CreatedAt    string           `json:"created_at"`
	UpdatedAt    string           `json:"updated_at"`
}

// GetOrderChatResult returns one order chat and a page of messages.
type GetOrderChatResult struct {
	OrderID     string        `json:"order_id"`
	BuyerID     string        `json:"buyer_id"`
	SellerID    string        `json:"seller_id"`
	Status      string        `json:"status"`
	CloseReason string        `json:"close_reason"`
	Messages    []MessageView `json:"messages"`
	NextCursor  string        `json:"next_cursor"`
}

// RealtimeEnvelope is the shared realtime payload published to the fanout subject.
type RealtimeEnvelope struct {
	EventID       string          `json:"event_id,omitempty"`
	OperationID   string          `json:"operation_id,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	AggregateType string          `json:"aggregate_type,omitempty"`
	AggregateID   string          `json:"aggregate_id,omitempty"`
	Status        string          `json:"status,omitempty"`
	OccurredAt    string          `json:"occurred_at,omitempty"`
	ConnectionID  string          `json:"connection_id,omitempty"`
	UserID        string          `json:"user_id,omitempty"`
	DeliveryScope string          `json:"delivery_scope,omitempty"`
	Type          string          `json:"type"`
	Payload       json.RawMessage `json:"payload"`
}
