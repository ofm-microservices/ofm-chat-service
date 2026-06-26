package domain

import (
	"context"
	"time"
)

const (
	// ChatStatusOpen indicates that the order chat still accepts new messages.
	ChatStatusOpen = "open"
	// ChatStatusClosed indicates that the order chat is terminal and read-only.
	ChatStatusClosed = "closed"
)

const (
	// MessageTypeText stores plain text with no attachments.
	MessageTypeText = "text"
	// MessageTypeFile stores attachment-only messages.
	MessageTypeFile = "file"
	// MessageTypeMixed stores text together with attachments.
	MessageTypeMixed = "mixed"
)

const (
	// ChatCloseReasonCompleted indicates that the order completed normally.
	ChatCloseReasonCompleted = "order_completed"
	// ChatCloseReasonDisputeResolved indicates that an admin resolved a dispute.
	ChatCloseReasonDisputeResolved = "dispute_resolved"
	// ChatCloseReasonOrderFailed indicates that the order ended in a failure state.
	ChatCloseReasonOrderFailed = "order_failed"
)

// Chat is the order-scoped conversation owned by chat-service.
type Chat struct {
	OrderID       string
	BuyerID       string
	SellerID      string
	Status        string
	CloseReason   string
	LastMessageAt time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// Attachment stores one file reference attached to a chat message.
type Attachment struct {
	FileID      string
	Filename    string
	ContentType string
	SizeBytes   int64
	SortOrder   int32
}

// Message stores one persisted chat message.
type Message struct {
	OrderID      string
	MessageID    string
	SenderUserID string
	MessageType  string
	Ciphertext   string
	HasText      bool
	Deleted      bool
	Edited       bool
	Attachments  []Attachment
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// CreateChatParams carries the input needed to create an order chat.
type CreateChatParams struct {
	OrderID  string
	BuyerID  string
	SellerID string
}

// CloseChatParams carries the input needed to terminally close a chat.
type CloseChatParams struct {
	OrderID     string
	CloseReason string
}

// CreateMessageParams carries the input needed to create a chat message.
type CreateMessageParams struct {
	OrderID      string
	MessageID    string
	SenderUserID string
	MessageType  string
	Ciphertext   string
	HasText      bool
	Attachments  []Attachment
}

// UpdateMessageParams carries the input needed to edit an existing message.
type UpdateMessageParams struct {
	OrderID    string
	MessageID  string
	Ciphertext string
	HasText    bool
}

// DeleteMessageParams carries the input needed to soft-delete a message.
type DeleteMessageParams struct {
	OrderID   string
	MessageID string
}

// ListMessagesParams carries the cursor window for one chat timeline query.
type ListMessagesParams struct {
	OrderID         string
	CreatedAtBefore *time.Time
	MessageIDBefore string
	Limit           int
}

// ChatRepository persists order chat metadata.
type ChatRepository interface {
	Create(ctx context.Context, params CreateChatParams) (*Chat, error)
	GetByOrderID(ctx context.Context, orderID string) (*Chat, error)
	Close(ctx context.Context, params CloseChatParams) (*Chat, error)
	TouchLastMessageAt(ctx context.Context, orderID string, lastMessageAt time.Time) error
}

// MessageRepository persists chat message timelines.
type MessageRepository interface {
	Create(ctx context.Context, params CreateMessageParams) (*Message, error)
	GetByID(ctx context.Context, orderID, messageID string) (*Message, error)
	Update(ctx context.Context, params UpdateMessageParams) (*Message, error)
	SoftDelete(ctx context.Context, params DeleteMessageParams) (*Message, error)
	ListByOrderID(ctx context.Context, params ListMessagesParams) ([]Message, error)
}
