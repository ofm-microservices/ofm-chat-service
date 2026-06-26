package application

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"chat-service/config"
	"chat-service/internal/domain"

	"github.com/google/uuid"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
)

type service struct {
	chats  domain.ChatRepository
	msgs   domain.MessageRepository
	files  FileClient
	broker EventBroker
	cursor CursorCodec
	cipher MessageCipher
	cfg    config.NATSConfig
	log    Logger
}

type messageCursor struct {
	CreatedAt string `json:"created_at"`
	MessageID string `json:"message_id"`
}

type chatRealtimePayload struct {
	OrderID string      `json:"order_id"`
	Message MessageView `json:"message"`
}

// New constructs the chat application service.
func New(
	chats domain.ChatRepository,
	msgs domain.MessageRepository,
	files FileClient,
	broker EventBroker,
	cursor CursorCodec,
	cipher MessageCipher,
	cfg config.NATSConfig,
	log Logger,
) (Service, error) {
	if chats == nil {
		return nil, ErrNilChatRepository
	}
	if msgs == nil {
		return nil, ErrNilMessageRepository
	}
	if files == nil {
		return nil, ErrNilFileClient
	}
	if broker == nil {
		return nil, ErrNilEventBroker
	}
	if cursor == nil {
		return nil, ErrNilCursorCodec
	}
	if cipher == nil {
		return nil, ErrNilMessageCipher
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	return &service{
		chats:  chats,
		msgs:   msgs,
		files:  files,
		broker: broker,
		cursor: cursor,
		cipher: cipher,
		cfg:    cfg,
		log:    log.With(logging.String("module", "application")),
	}, nil
}

// CreateChat creates an order-scoped chat for the buyer and seller.
func (s *service) CreateChat(ctx context.Context, cmd CreateChatCommand) (*domain.Chat, error) {
	if strings.TrimSpace(cmd.OrderID) == "" || strings.TrimSpace(cmd.BuyerID) == "" || strings.TrimSpace(cmd.SellerID) == "" {
		return nil, domain.ErrInvalidParticipant
	}
	if strings.TrimSpace(cmd.BuyerID) == strings.TrimSpace(cmd.SellerID) {
		return nil, domain.ErrInvalidParticipant
	}
	return s.chats.Create(ctx, domain.CreateChatParams{
		OrderID:  cmd.OrderID,
		BuyerID:  cmd.BuyerID,
		SellerID: cmd.SellerID,
	})
}

// CloseChat marks an order chat terminal and read-only.
func (s *service) CloseChat(ctx context.Context, cmd CloseChatCommand) (*domain.Chat, error) {
	if strings.TrimSpace(cmd.OrderID) == "" || strings.TrimSpace(cmd.CloseReason) == "" {
		return nil, domain.ErrInvalidMessage
	}
	return s.chats.Close(ctx, domain.CloseChatParams{
		OrderID:     cmd.OrderID,
		CloseReason: cmd.CloseReason,
	})
}

// GetOrderChat returns one chat timeline page for a valid participant.
func (s *service) GetOrderChat(ctx context.Context, cmd GetOrderChatCommand) (*GetOrderChatResult, error) {
	chat, err := s.chats.GetByOrderID(ctx, cmd.OrderID)
	if err != nil {
		return nil, err
	}
	if err := ensureParticipant(chat, cmd.UserID); err != nil {
		return nil, err
	}

	limit := cmd.Limit
	if limit <= 0 || limit > 50 {
		limit = 20
	}

	params := domain.ListMessagesParams{OrderID: cmd.OrderID, Limit: limit + 1}
	if strings.TrimSpace(cmd.Cursor) != "" {
		var cur messageCursor
		if err := s.cursor.Decode(cmd.Cursor, &cur); err != nil {
			return nil, domain.ErrInvalidCursor
		}
		parsed, err := time.Parse(time.RFC3339Nano, cur.CreatedAt)
		if err != nil {
			return nil, domain.ErrInvalidCursor
		}
		params.CreatedAtBefore = &parsed
		params.MessageIDBefore = cur.MessageID
	}

	msgs, err := s.msgs.ListByOrderID(ctx, params)
	if err != nil {
		return nil, err
	}

	nextCursor := ""
	if len(msgs) > limit {
		last := msgs[limit-1]
		msgs = msgs[:limit]
		nextCursor, err = s.cursor.Encode(messageCursor{
			CreatedAt: last.CreatedAt.UTC().Format(time.RFC3339Nano),
			MessageID: last.MessageID,
		})
		if err != nil {
			return nil, err
		}
	}

	views, err := s.toMessageViews(ctx, msgs)
	if err != nil {
		return nil, err
	}

	return &GetOrderChatResult{
		OrderID:     cmd.OrderID,
		BuyerID:     chat.BuyerID,
		SellerID:    chat.SellerID,
		Status:      chat.Status,
		CloseReason: chat.CloseReason,
		Messages:    views,
		NextCursor:  nextCursor,
	}, nil
}

// CreateMessage stores one new text/file/mixed message and emits realtime fanout.
func (s *service) CreateMessage(ctx context.Context, cmd CreateMessageCommand) (*MessageView, error) {
	chat, err := s.chats.GetByOrderID(ctx, cmd.OrderID)
	if err != nil {
		return nil, err
	}
	if err := ensureWritableParticipant(chat, cmd.UserID); err != nil {
		return nil, err
	}

	text := strings.TrimSpace(cmd.Text)
	attachments, err := s.loadAttachments(ctx, cmd.AttachmentIDs)
	if err != nil {
		return nil, err
	}
	if text == "" && len(attachments) == 0 {
		return nil, domain.ErrInvalidMessage
	}

	ciphertext := ""
	if text != "" {
		ciphertext, err = s.cipher.Encrypt(text)
		if err != nil {
			return nil, err
		}
	}

	msgType := domain.MessageTypeText
	switch {
	case text != "" && len(attachments) > 0:
		msgType = domain.MessageTypeMixed
	case text == "" && len(attachments) > 0:
		msgType = domain.MessageTypeFile
	}

	msg, err := s.msgs.Create(ctx, domain.CreateMessageParams{
		OrderID:      cmd.OrderID,
		MessageID:    uuid.Must(uuid.NewV7()).String(),
		SenderUserID: cmd.UserID,
		MessageType:  msgType,
		Ciphertext:   ciphertext,
		HasText:      text != "",
		Attachments:  attachments,
	})
	if err != nil {
		return nil, err
	}
	if err := s.chats.TouchLastMessageAt(ctx, cmd.OrderID, msg.CreatedAt); err != nil {
		return nil, err
	}

	view, err := s.toMessageView(ctx, *msg)
	if err != nil {
		return nil, err
	}
	_ = s.publishRealtimeMessage(ctx, chat, "chat_message_created", *view)
	return view, nil
}

// EditMessage updates the message text for its author.
func (s *service) EditMessage(ctx context.Context, cmd EditMessageCommand) (*MessageView, error) {
	chat, err := s.chats.GetByOrderID(ctx, cmd.OrderID)
	if err != nil {
		return nil, err
	}
	if err := ensureWritableParticipant(chat, cmd.UserID); err != nil {
		return nil, err
	}

	msg, err := s.msgs.GetByID(ctx, cmd.OrderID, cmd.MessageID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(msg.SenderUserID) != strings.TrimSpace(cmd.UserID) {
		return nil, domain.ErrChatAccessDenied
	}
	text := strings.TrimSpace(cmd.Text)
	if text == "" {
		return nil, domain.ErrInvalidMessage
	}
	ciphertext, err := s.cipher.Encrypt(text)
	if err != nil {
		return nil, err
	}
	updated, err := s.msgs.Update(ctx, domain.UpdateMessageParams{
		OrderID:    cmd.OrderID,
		MessageID:  cmd.MessageID,
		Ciphertext: ciphertext,
		HasText:    true,
	})
	if err != nil {
		return nil, err
	}

	view, err := s.toMessageView(ctx, *updated)
	if err != nil {
		return nil, err
	}
	_ = s.publishRealtimeMessage(ctx, chat, "chat_message_edited", *view)
	return view, nil
}

// DeleteMessage soft-deletes one authored message.
func (s *service) DeleteMessage(ctx context.Context, cmd DeleteMessageCommand) (*MessageView, error) {
	chat, err := s.chats.GetByOrderID(ctx, cmd.OrderID)
	if err != nil {
		return nil, err
	}
	if err := ensureWritableParticipant(chat, cmd.UserID); err != nil {
		return nil, err
	}

	msg, err := s.msgs.GetByID(ctx, cmd.OrderID, cmd.MessageID)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(msg.SenderUserID) != strings.TrimSpace(cmd.UserID) {
		return nil, domain.ErrChatAccessDenied
	}
	deleted, err := s.msgs.SoftDelete(ctx, domain.DeleteMessageParams{
		OrderID:   cmd.OrderID,
		MessageID: cmd.MessageID,
	})
	if err != nil {
		return nil, err
	}

	view, err := s.toMessageView(ctx, *deleted)
	if err != nil {
		return nil, err
	}
	_ = s.publishRealtimeMessage(ctx, chat, "chat_message_deleted", *view)
	return view, nil
}

// CreateAttachmentUploadURL reserves one direct upload for a valid chat participant.
func (s *service) CreateAttachmentUploadURL(ctx context.Context, cmd CreateAttachmentUploadURLCommand) (*CreateAttachmentUploadURLResult, error) {
	chat, err := s.chats.GetByOrderID(ctx, cmd.OrderID)
	if err != nil {
		return nil, err
	}
	if err := ensureWritableParticipant(chat, cmd.UserID); err != nil {
		return nil, err
	}
	if strings.TrimSpace(cmd.Filename) == "" || strings.TrimSpace(cmd.ContentType) == "" || cmd.SizeBytes <= 0 {
		return nil, domain.ErrInvalidMessage
	}
	res, err := s.files.CreateDirectUpload(ctx, CreateDirectUploadCommand{
		OwnerID:     cmd.UserID,
		Filename:    cmd.Filename,
		ContentType: cmd.ContentType,
		SizeBytes:   cmd.SizeBytes,
	})
	if err != nil {
		return nil, err
	}
	return &CreateAttachmentUploadURLResult{
		FileID:    res.FileID,
		UploadURL: res.UploadURL,
		Method:    res.Method,
		Headers:   res.Headers,
		ExpiresAt: res.ExpiresAt.UTC().Format(time.RFC3339Nano),
	}, nil
}

// CompleteAttachmentUpload finalizes a direct upload and returns file metadata.
func (s *service) CompleteAttachmentUpload(ctx context.Context, cmd CompleteAttachmentUploadCommand) (*AttachmentView, error) {
	chat, err := s.chats.GetByOrderID(ctx, cmd.OrderID)
	if err != nil {
		return nil, err
	}
	if err := ensureWritableParticipant(chat, cmd.UserID); err != nil {
		return nil, err
	}
	file, err := s.files.CompleteDirectUpload(ctx, cmd.FileID)
	if err != nil {
		return nil, err
	}
	return &AttachmentView{
		FileID:      file.FileID,
		Filename:    file.Filename,
		ContentType: file.ContentType,
		SizeBytes:   file.SizeBytes,
		URL:         file.URL,
	}, nil
}

// CreateAttachmentUploadURLResult returns one direct upload reservation.
type CreateAttachmentUploadURLResult struct {
	FileID    string            `json:"file_id"`
	UploadURL string            `json:"upload_url"`
	Method    string            `json:"method"`
	Headers   map[string]string `json:"headers"`
	ExpiresAt string            `json:"expires_at"`
}

func ensureParticipant(chat *domain.Chat, userID string) error {
	if chat == nil {
		return domain.ErrChatNotFound
	}
	if strings.TrimSpace(userID) == "" {
		return domain.ErrChatAccessDenied
	}
	if strings.TrimSpace(chat.BuyerID) != strings.TrimSpace(userID) && strings.TrimSpace(chat.SellerID) != strings.TrimSpace(userID) {
		return domain.ErrChatAccessDenied
	}
	return nil
}

func ensureWritableParticipant(chat *domain.Chat, userID string) error {
	if err := ensureParticipant(chat, userID); err != nil {
		return err
	}
	if chat.Status == domain.ChatStatusClosed {
		return domain.ErrChatClosed
	}
	return nil
}

func (s *service) loadAttachments(ctx context.Context, fileIDs []string) ([]domain.Attachment, error) {
	if len(fileIDs) == 0 {
		return nil, nil
	}
	files, err := s.files.GetFileURLs(ctx, fileIDs)
	if err != nil {
		return nil, err
	}
	attachments := make([]domain.Attachment, 0, len(fileIDs))
	for idx, fileID := range fileIDs {
		if _, ok := files[fileID]; !ok {
			return nil, domain.ErrInvalidMessage
		}
		attachments = append(attachments, domain.Attachment{
			FileID:    fileID,
			SortOrder: int32(idx + 1),
		})
	}
	return attachments, nil
}

func (s *service) toMessageViews(ctx context.Context, msgs []domain.Message) ([]MessageView, error) {
	if len(msgs) == 0 {
		return []MessageView{}, nil
	}
	views := make([]MessageView, 0, len(msgs))
	for _, msg := range msgs {
		view, err := s.toMessageView(ctx, msg)
		if err != nil {
			return nil, err
		}
		views = append(views, *view)
	}
	return views, nil
}

func (s *service) toMessageView(ctx context.Context, msg domain.Message) (*MessageView, error) {
	text := ""
	if msg.HasText && !msg.Deleted && strings.TrimSpace(msg.Ciphertext) != "" {
		plain, err := s.cipher.Decrypt(msg.Ciphertext)
		if err != nil {
			return nil, err
		}
		text = plain
	}

	attachments := []AttachmentView{}
	if !msg.Deleted && len(msg.Attachments) > 0 {
		fileIDs := make([]string, 0, len(msg.Attachments))
		for _, a := range msg.Attachments {
			fileIDs = append(fileIDs, a.FileID)
		}
		urls, err := s.files.GetFileURLs(ctx, fileIDs)
		if err != nil {
			return nil, err
		}
		for _, a := range msg.Attachments {
			attachments = append(attachments, AttachmentView{
				FileID:      a.FileID,
				Filename:    a.Filename,
				ContentType: a.ContentType,
				SizeBytes:   a.SizeBytes,
				URL:         urls[a.FileID],
			})
		}
	}

	return &MessageView{
		MessageID:    msg.MessageID,
		OrderID:      msg.OrderID,
		SenderUserID: msg.SenderUserID,
		MessageType:  msg.MessageType,
		Text:         text,
		Deleted:      msg.Deleted,
		Edited:       msg.Edited,
		Attachments:  attachments,
		CreatedAt:    msg.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt:    msg.UpdatedAt.UTC().Format(time.RFC3339Nano),
	}, nil
}

func (s *service) publishRealtimeMessage(ctx context.Context, chat *domain.Chat, eventType string, view MessageView) error {
	if strings.TrimSpace(s.cfg.RealtimeSubject) == "" {
		return nil
	}
	payload, err := json.Marshal(chatRealtimePayload{
		OrderID: chat.OrderID,
		Message: view,
	})
	if err != nil {
		return err
	}
	for _, userID := range []string{chat.BuyerID, chat.SellerID} {
		env, err := json.Marshal(RealtimeEnvelope{
			UserID:        userID,
			DeliveryScope: "user",
			Type:          eventType,
			Payload:       payload,
		})
		if err != nil {
			return err
		}
		if err := s.broker.Publish(ctx, s.cfg.RealtimeSubject, env); err != nil {
			s.log.Error("publish chat realtime event failed", logging.String("order_id", chat.OrderID), logging.String("user_id", userID), logging.Err(err))
		}
	}
	return nil
}
