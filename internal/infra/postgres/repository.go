package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"chat-service/internal/domain"

	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

type chatRepository struct {
	db  *sqlx.DB
	log logging.Logger
}

type messageRepository struct {
	db  *sqlx.DB
	log logging.Logger
}

// NewChatRepository constructs the PostgreSQL-backed chat repository.
func NewChatRepository(db *sqlx.DB, log logging.Logger) (domain.ChatRepository, error) {
	if db == nil {
		return nil, errors.New("postgres database is nil")
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	return &chatRepository{db: db, log: log.With(logging.String("module", "postgres-chat-repository"))}, nil
}

// NewMessageRepository constructs the PostgreSQL-backed message repository.
func NewMessageRepository(db *sqlx.DB, log logging.Logger) (domain.MessageRepository, error) {
	if db == nil {
		return nil, errors.New("postgres database is nil")
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	return &messageRepository{db: db, log: log.With(logging.String("module", "postgres-message-repository"))}, nil
}

type chatRow struct {
	OrderID       string    `db:"order_id"`
	BuyerID       string    `db:"buyer_id"`
	SellerID      string    `db:"seller_id"`
	Status        string    `db:"status"`
	CloseReason   string    `db:"close_reason"`
	LastMessageAt time.Time `db:"last_message_at"`
	CreatedAt     time.Time `db:"created_at"`
	UpdatedAt     time.Time `db:"updated_at"`
}

type messageRow struct {
	OrderID      string          `db:"order_id"`
	MessageID    string          `db:"message_id"`
	SenderUserID string          `db:"sender_user_id"`
	MessageType  string          `db:"message_type"`
	Ciphertext   string          `db:"ciphertext"`
	HasText      bool            `db:"has_text"`
	Deleted      bool            `db:"deleted"`
	Edited       bool            `db:"edited"`
	Attachments  json.RawMessage `db:"attachments_json"`
	CreatedAt    time.Time       `db:"created_at"`
	UpdatedAt    time.Time       `db:"updated_at"`
}

func (r *chatRepository) Create(ctx context.Context, p domain.CreateChatParams) (*domain.Chat, error) {
	now := time.Now().UTC()
	_, err := r.db.ExecContext(ctx, `INSERT INTO chats_by_order
		(order_id,buyer_id,seller_id,status,close_reason,last_message_at,created_at,updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (order_id) DO NOTHING`,
		p.OrderID, p.BuyerID, p.SellerID, domain.ChatStatusOpen, "", now, now, now)
	if err != nil {
		return nil, err
	}
	return &domain.Chat{OrderID: p.OrderID, BuyerID: p.BuyerID, SellerID: p.SellerID, Status: domain.ChatStatusOpen, LastMessageAt: now, CreatedAt: now, UpdatedAt: now}, nil
}

func (r *chatRepository) GetByOrderID(ctx context.Context, orderID string) (*domain.Chat, error) {
	var row chatRow
	err := r.db.GetContext(ctx, &row, `SELECT order_id,buyer_id,seller_id,status,close_reason,last_message_at,created_at,updated_at FROM chats_by_order WHERE order_id=$1`, orderID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrChatNotFound
	}
	if err != nil {
		return nil, err
	}
	return &domain.Chat{OrderID: row.OrderID, BuyerID: row.BuyerID, SellerID: row.SellerID, Status: row.Status, CloseReason: row.CloseReason, LastMessageAt: row.LastMessageAt, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}, nil
}

func (r *chatRepository) Close(ctx context.Context, p domain.CloseChatParams) (*domain.Chat, error) {
	_, err := r.db.ExecContext(ctx, `UPDATE chats_by_order SET status=$1,close_reason=$2,updated_at=$3 WHERE order_id=$4`, domain.ChatStatusClosed, p.CloseReason, time.Now().UTC(), p.OrderID)
	if err != nil {
		return nil, err
	}
	return r.GetByOrderID(ctx, p.OrderID)
}

func (r *chatRepository) TouchLastMessageAt(ctx context.Context, orderID string, at time.Time) error {
	_, err := r.db.ExecContext(ctx, `UPDATE chats_by_order SET last_message_at=$1,updated_at=$2 WHERE order_id=$3`, at.UTC(), time.Now().UTC(), orderID)
	return err
}

func (r *messageRepository) Create(ctx context.Context, p domain.CreateMessageParams) (*domain.Message, error) {
	now := time.Now().UTC()
	attachments, err := json.Marshal(p.Attachments)
	if err != nil {
		return nil, err
	}
	_, err = r.db.ExecContext(ctx, `INSERT INTO chat_messages
		(order_id,message_id,sender_user_id,message_type,ciphertext,has_text,deleted,edited,attachments_json,created_at,updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,false,false,$7::jsonb,$8,$8)`, p.OrderID, p.MessageID, p.SenderUserID, p.MessageType, p.Ciphertext, p.HasText, string(attachments), now)
	if err != nil {
		return nil, err
	}
	return &domain.Message{OrderID: p.OrderID, MessageID: p.MessageID, SenderUserID: p.SenderUserID, MessageType: p.MessageType, Ciphertext: p.Ciphertext, HasText: p.HasText, Attachments: p.Attachments, CreatedAt: now, UpdatedAt: now}, nil
}

func (r *messageRepository) GetByID(ctx context.Context, orderID, messageID string) (*domain.Message, error) {
	var row messageRow
	err := r.db.GetContext(ctx, &row, `SELECT order_id,message_id,sender_user_id,message_type,ciphertext,has_text,deleted,edited,attachments_json,created_at,updated_at FROM chat_messages WHERE order_id=$1 AND message_id=$2`, orderID, messageID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, domain.ErrMessageNotFound
	}
	if err != nil {
		return nil, err
	}
	return mapMessage(row)
}

func (r *messageRepository) Update(ctx context.Context, p domain.UpdateMessageParams) (*domain.Message, error) {
	msg, err := r.GetByID(ctx, p.OrderID, p.MessageID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	_, err = r.db.ExecContext(ctx, `UPDATE chat_messages SET ciphertext=$1,has_text=$2,edited=true,updated_at=$3 WHERE order_id=$4 AND message_id=$5`, p.Ciphertext, p.HasText, now, p.OrderID, p.MessageID)
	if err != nil {
		return nil, err
	}
	msg.Ciphertext = p.Ciphertext
	msg.HasText = p.HasText
	msg.Edited = true
	msg.UpdatedAt = now
	return msg, nil
}

func (r *messageRepository) SoftDelete(ctx context.Context, p domain.DeleteMessageParams) (*domain.Message, error) {
	msg, err := r.GetByID(ctx, p.OrderID, p.MessageID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	_, err = r.db.ExecContext(ctx, `UPDATE chat_messages SET deleted=true,updated_at=$1 WHERE order_id=$2 AND message_id=$3`, now, p.OrderID, p.MessageID)
	if err != nil {
		return nil, err
	}
	msg.Deleted = true
	msg.UpdatedAt = now
	return msg, nil
}

func (r *messageRepository) ListByOrderID(ctx context.Context, p domain.ListMessagesParams) ([]domain.Message, error) {
	query := `SELECT order_id,message_id,sender_user_id,message_type,ciphertext,has_text,deleted,edited,attachments_json,created_at,updated_at FROM chat_messages WHERE order_id=$1`
	args := []any{p.OrderID}
	if p.CreatedAtBefore != nil && p.MessageIDBefore != "" {
		query += ` AND (created_at,message_id)<($2,$3)`
		args = append(args, p.CreatedAtBefore.UTC(), p.MessageIDBefore)
	}
	query += ` ORDER BY created_at DESC,message_id DESC LIMIT $` + fmt.Sprint(len(args)+1)
	args = append(args, p.Limit)
	rows, err := r.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]domain.Message, 0, p.Limit)
	for rows.Next() {
		var row messageRow
		if err := rows.StructScan(&row); err != nil {
			return nil, err
		}
		msg, err := mapMessage(row)
		if err != nil {
			return nil, err
		}
		result = append(result, *msg)
	}
	return result, rows.Err()
}

func mapMessage(row messageRow) (*domain.Message, error) {
	var attachments []domain.Attachment
	if len(row.Attachments) > 0 && string(row.Attachments) != "null" {
		if err := json.Unmarshal(row.Attachments, &attachments); err != nil {
			return nil, err
		}
	}
	return &domain.Message{OrderID: row.OrderID, MessageID: row.MessageID, SenderUserID: row.SenderUserID, MessageType: row.MessageType, Ciphertext: row.Ciphertext, HasText: row.HasText, Deleted: row.Deleted, Edited: row.Edited, Attachments: attachments, CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt}, nil
}
