package scylla

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"chat-service/internal/domain"

	"github.com/gocql/gocql"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

type chatRepository struct {
	db  *gocql.Session
	log logging.Logger
}

type messageRepository struct {
	db  *gocql.Session
	log logging.Logger
}

// NewChatRepository constructs the Scylla-backed chat repository.
func NewChatRepository(db *gocql.Session, log logging.Logger) (domain.ChatRepository, error) {
	if db == nil {
		return nil, errors.New("scylla session is nil")
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	return &chatRepository{db: db, log: log.With(logging.String("module", "scylla-chat-repository"))}, nil
}

// NewMessageRepository constructs the Scylla-backed message repository.
func NewMessageRepository(db *gocql.Session, log logging.Logger) (domain.MessageRepository, error) {
	if db == nil {
		return nil, errors.New("scylla session is nil")
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	return &messageRepository{db: db, log: log.With(logging.String("module", "scylla-message-repository"))}, nil
}

func (r *chatRepository) Create(ctx context.Context, params domain.CreateChatParams) (*domain.Chat, error) {
	now := time.Now().UTC()
	applied, err := r.db.Query(
		insertChatQuery,
		params.OrderID,
		params.BuyerID,
		params.SellerID,
		domain.ChatStatusOpen,
		"",
		now,
		now,
		now,
	).WithContext(ctx).ScanCAS()
	if err != nil {
		return nil, err
	}
	if !applied {
		return r.GetByOrderID(ctx, params.OrderID)
	}
	return &domain.Chat{
		OrderID:       params.OrderID,
		BuyerID:       params.BuyerID,
		SellerID:      params.SellerID,
		Status:        domain.ChatStatusOpen,
		CloseReason:   "",
		LastMessageAt: now,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil
}

func (r *chatRepository) GetByOrderID(ctx context.Context, orderID string) (*domain.Chat, error) {
	var row ChatRow
	if err := r.db.Query(getChatByOrderIDQuery, orderID).WithContext(ctx).Consistency(gocql.One).Scan(
		&row.OrderID,
		&row.BuyerID,
		&row.SellerID,
		&row.Status,
		&row.CloseReason,
		&row.LastMessageAt,
		&row.CreatedAt,
		&row.UpdatedAt,
	); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil, domain.ErrChatNotFound
		}
		return nil, err
	}
	return &domain.Chat{
		OrderID:       row.OrderID,
		BuyerID:       row.BuyerID,
		SellerID:      row.SellerID,
		Status:        row.Status,
		CloseReason:   row.CloseReason,
		LastMessageAt: row.LastMessageAt,
		CreatedAt:     row.CreatedAt,
		UpdatedAt:     row.UpdatedAt,
	}, nil
}

func (r *chatRepository) Close(ctx context.Context, params domain.CloseChatParams) (*domain.Chat, error) {
	now := time.Now().UTC()
	if err := r.db.Query(closeChatQuery, domain.ChatStatusClosed, params.CloseReason, now, params.OrderID).WithContext(ctx).Exec(); err != nil {
		return nil, err
	}
	return r.GetByOrderID(ctx, params.OrderID)
}

func (r *chatRepository) TouchLastMessageAt(ctx context.Context, orderID string, lastMessageAt time.Time) error {
	return r.db.Query(touchChatLastMessageAtQuery, lastMessageAt.UTC(), time.Now().UTC(), orderID).WithContext(ctx).Exec()
}

func (r *messageRepository) Create(ctx context.Context, params domain.CreateMessageParams) (*domain.Message, error) {
	now := time.Now().UTC()
	attachmentsJSON, err := json.Marshal(params.Attachments)
	if err != nil {
		return nil, err
	}
	if err := r.db.Query(
		insertMessageByOrderQuery,
		params.OrderID,
		now,
		params.MessageID,
		params.SenderUserID,
		params.MessageType,
		params.Ciphertext,
		params.HasText,
		false,
		false,
		string(attachmentsJSON),
		now,
	).WithContext(ctx).Exec(); err != nil {
		return nil, err
	}
	if err := r.db.Query(
		insertMessageByIDQuery,
		params.OrderID,
		params.MessageID,
		params.SenderUserID,
		params.MessageType,
		params.Ciphertext,
		params.HasText,
		false,
		false,
		string(attachmentsJSON),
		now,
		now,
	).WithContext(ctx).Exec(); err != nil {
		return nil, err
	}
	return &domain.Message{
		OrderID:      params.OrderID,
		MessageID:    params.MessageID,
		SenderUserID: params.SenderUserID,
		MessageType:  params.MessageType,
		Ciphertext:   params.Ciphertext,
		HasText:      params.HasText,
		Deleted:      false,
		Edited:       false,
		Attachments:  params.Attachments,
		CreatedAt:    now,
		UpdatedAt:    now,
	}, nil
}

func (r *messageRepository) GetByID(ctx context.Context, orderID, messageID string) (*domain.Message, error) {
	var row MessageRow
	if err := r.db.Query(getMessageByIDQuery, orderID, messageID).WithContext(ctx).Consistency(gocql.One).Scan(
		&row.OrderID,
		&row.MessageID,
		&row.SenderUserID,
		&row.MessageType,
		&row.Ciphertext,
		&row.HasText,
		&row.Deleted,
		&row.Edited,
		&row.AttachmentsJSON,
		&row.CreatedAt,
		&row.UpdatedAt,
	); err != nil {
		if errors.Is(err, gocql.ErrNotFound) {
			return nil, domain.ErrMessageNotFound
		}
		return nil, err
	}
	return mapMessageRow(row)
}

func (r *messageRepository) Update(ctx context.Context, params domain.UpdateMessageParams) (*domain.Message, error) {
	msg, err := r.GetByID(ctx, params.OrderID, params.MessageID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if err := r.db.Query(updateMessageByIDQuery, params.Ciphertext, params.HasText, true, now, params.OrderID, params.MessageID).WithContext(ctx).Exec(); err != nil {
		return nil, err
	}
	if err := r.db.Query(updateMessageByOrderQuery, params.Ciphertext, params.HasText, true, now, params.OrderID, msg.CreatedAt, params.MessageID).WithContext(ctx).Exec(); err != nil {
		return nil, err
	}
	msg.Ciphertext = params.Ciphertext
	msg.HasText = params.HasText
	msg.Edited = true
	msg.UpdatedAt = now
	return msg, nil
}

func (r *messageRepository) SoftDelete(ctx context.Context, params domain.DeleteMessageParams) (*domain.Message, error) {
	msg, err := r.GetByID(ctx, params.OrderID, params.MessageID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if err := r.db.Query(deleteMessageByIDQuery, true, now, params.OrderID, params.MessageID).WithContext(ctx).Exec(); err != nil {
		return nil, err
	}
	if err := r.db.Query(deleteMessageByOrderQuery, true, now, params.OrderID, msg.CreatedAt, params.MessageID).WithContext(ctx).Exec(); err != nil {
		return nil, err
	}
	msg.Deleted = true
	msg.UpdatedAt = now
	return msg, nil
}

func (r *messageRepository) ListByOrderID(ctx context.Context, params domain.ListMessagesParams) ([]domain.Message, error) {
	query := r.db.Query(listMessagesByOrderQuery, params.OrderID, params.Limit).WithContext(ctx).Consistency(gocql.One)
	if params.CreatedAtBefore != nil && params.MessageIDBefore != "" {
		query = r.db.Query(listMessagesByOrderBeforeQuery, params.OrderID, params.CreatedAtBefore.UTC(), params.MessageIDBefore, params.Limit).WithContext(ctx).Consistency(gocql.One)
	}
	iter := query.Iter()
	defer iter.Close()

	rows := make([]domain.Message, 0, params.Limit)
	var row MessageRow
	for iter.Scan(
		&row.OrderID,
		&row.MessageID,
		&row.SenderUserID,
		&row.MessageType,
		&row.Ciphertext,
		&row.HasText,
		&row.Deleted,
		&row.Edited,
		&row.AttachmentsJSON,
		&row.CreatedAt,
		&row.UpdatedAt,
	) {
		msg, err := mapMessageRow(row)
		if err != nil {
			return nil, err
		}
		rows = append(rows, *msg)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return rows, nil
}

func mapMessageRow(row MessageRow) (*domain.Message, error) {
	var attachments []domain.Attachment
	if row.AttachmentsJSON != "" {
		if err := json.Unmarshal([]byte(row.AttachmentsJSON), &attachments); err != nil {
			return nil, err
		}
	}
	return &domain.Message{
		OrderID:      row.OrderID,
		MessageID:    row.MessageID,
		SenderUserID: row.SenderUserID,
		MessageType:  row.MessageType,
		Ciphertext:   row.Ciphertext,
		HasText:      row.HasText,
		Deleted:      row.Deleted,
		Edited:       row.Edited,
		Attachments:  attachments,
		CreatedAt:    row.CreatedAt,
		UpdatedAt:    row.UpdatedAt,
	}, nil
}
