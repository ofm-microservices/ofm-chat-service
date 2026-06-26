package scylla

import "time"

// ChatRow is the Scylla persistence model for order chats.
type ChatRow struct {
	OrderID       string
	BuyerID       string
	SellerID      string
	Status        string
	CloseReason   string
	LastMessageAt time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// MessageRow is the Scylla persistence model for chat messages.
type MessageRow struct {
	OrderID      string
	MessageID    string
	SenderUserID string
	MessageType  string
	Ciphertext   string
	HasText      bool
	Deleted      bool
	Edited       bool
	AttachmentsJSON string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}
