package application

import (
	"context"
	"encoding/json"
	"time"

	"chat-service/config"
	"chat-service/internal/domain"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type chatRepoFake struct {
	getByOrderIDFn       func(context.Context, string) (*domain.Chat, error)
	touchLastMessageAtFn func(context.Context, string, time.Time) error
}

func (f *chatRepoFake) Create(context.Context, domain.CreateChatParams) (*domain.Chat, error) {
	return nil, nil
}

func (f *chatRepoFake) GetByOrderID(ctx context.Context, orderID string) (*domain.Chat, error) {
	if f.getByOrderIDFn != nil {
		return f.getByOrderIDFn(ctx, orderID)
	}
	return nil, domain.ErrChatNotFound
}

func (f *chatRepoFake) Close(context.Context, domain.CloseChatParams) (*domain.Chat, error) {
	return nil, nil
}

func (f *chatRepoFake) TouchLastMessageAt(ctx context.Context, orderID string, lastMessageAt time.Time) error {
	if f.touchLastMessageAtFn != nil {
		return f.touchLastMessageAtFn(ctx, orderID, lastMessageAt)
	}
	return nil
}

type messageRepoFake struct {
	listByOrderIDFn func(context.Context, domain.ListMessagesParams) ([]domain.Message, error)
	createFn        func(context.Context, domain.CreateMessageParams) (*domain.Message, error)
	getByIDFn       func(context.Context, string, string) (*domain.Message, error)
	createParams    []domain.CreateMessageParams
	listParams      []domain.ListMessagesParams
}

func (f *messageRepoFake) Create(ctx context.Context, params domain.CreateMessageParams) (*domain.Message, error) {
	f.createParams = append(f.createParams, params)
	if f.createFn != nil {
		return f.createFn(ctx, params)
	}
	now := time.Now().UTC()
	return &domain.Message{
		OrderID:      params.OrderID,
		MessageID:    params.MessageID,
		SenderUserID: params.SenderUserID,
		MessageType:  params.MessageType,
		Ciphertext:   params.Ciphertext,
		HasText:      params.HasText,
		Attachments:  params.Attachments,
		CreatedAt:    now,
		UpdatedAt:    now,
	}, nil
}

func (f *messageRepoFake) GetByID(ctx context.Context, orderID, messageID string) (*domain.Message, error) {
	if f.getByIDFn != nil {
		return f.getByIDFn(ctx, orderID, messageID)
	}
	return nil, domain.ErrMessageNotFound
}

func (f *messageRepoFake) Update(context.Context, domain.UpdateMessageParams) (*domain.Message, error) {
	return nil, nil
}

func (f *messageRepoFake) SoftDelete(context.Context, domain.DeleteMessageParams) (*domain.Message, error) {
	return nil, nil
}

func (f *messageRepoFake) ListByOrderID(ctx context.Context, params domain.ListMessagesParams) ([]domain.Message, error) {
	f.listParams = append(f.listParams, params)
	if f.listByOrderIDFn != nil {
		return f.listByOrderIDFn(ctx, params)
	}
	return nil, nil
}

type fileClientFake struct {
	getFileURLsFn          func(context.Context, []string) (map[string]string, error)
	completeDirectUploadFn func(context.Context, string) (*StoredFile, error)
	getFileURLCalls        [][]string
	completeCalls          []string
}

func (f *fileClientFake) CreateDirectUpload(context.Context, CreateDirectUploadCommand) (*DirectUploadReservation, error) {
	return nil, nil
}

func (f *fileClientFake) CompleteDirectUpload(ctx context.Context, fileID string) (*StoredFile, error) {
	f.completeCalls = append(f.completeCalls, fileID)
	if f.completeDirectUploadFn != nil {
		return f.completeDirectUploadFn(ctx, fileID)
	}
	return &StoredFile{FileID: fileID}, nil
}

func (f *fileClientFake) GetFileURLs(ctx context.Context, fileIDs []string) (map[string]string, error) {
	f.getFileURLCalls = append(f.getFileURLCalls, append([]string(nil), fileIDs...))
	if f.getFileURLsFn != nil {
		return f.getFileURLsFn(ctx, fileIDs)
	}
	out := make(map[string]string, len(fileIDs))
	for _, fileID := range fileIDs {
		out[fileID] = "http://files.local/" + fileID
	}
	return out, nil
}

func (f *fileClientFake) Close() error { return nil }

type brokerFake struct {
	publishes []publishCall
}

type publishCall struct {
	subject string
	payload []byte
}

func (f *brokerFake) Publish(_ context.Context, subject string, payload []byte) error {
	f.publishes = append(f.publishes, publishCall{subject: subject, payload: append([]byte(nil), payload...)})
	return nil
}

func (f *brokerFake) Subscribe(context.Context, string, MessageHandler) error { return nil }
func (f *brokerFake) Close()                                                  {}

type cursorFake struct {
	encodeFn func(any) (string, error)
	decodeFn func(string, any) error
}

func (f *cursorFake) Encode(value any) (string, error) {
	if f.encodeFn != nil {
		return f.encodeFn(value)
	}
	return "cursor-token", nil
}

func (f *cursorFake) Decode(token string, value any) error {
	if f.decodeFn != nil {
		return f.decodeFn(token, value)
	}
	return nil
}

type cipherFake struct{}

func (cipherFake) Encrypt(plain string) (string, error) { return "enc:" + plain, nil }
func (cipherFake) Decrypt(ciphertext string) (string, error) {
	if len(ciphertext) >= 4 && ciphertext[:4] == "enc:" {
		return ciphertext[4:], nil
	}
	return ciphertext, nil
}

func testLogger() logging.Logger {
	lg, err := logging.New("chat-service-test", "test", "error")
	Expect(err).NotTo(HaveOccurred())
	return lg
}

func newTestService(chats domain.ChatRepository, msgs domain.MessageRepository, files FileClient, broker EventBroker, cursor CursorCodec) Service {
	svc, err := New(chats, msgs, files, broker, cursor, cipherFake{}, config.NATSConfig{RealtimeSubject: "realtime"}, testLogger())
	Expect(err).NotTo(HaveOccurred())
	return svc
}

var _ = Describe("Service", func() {
	It("denies chat reads for non-participants", func() {
		svc := newTestService(
			&chatRepoFake{
				getByOrderIDFn: func(ctx context.Context, orderID string) (*domain.Chat, error) {
					return &domain.Chat{OrderID: orderID, BuyerID: "buyer-1", SellerID: "seller-1", Status: domain.ChatStatusOpen}, nil
				},
			},
			&messageRepoFake{},
			&fileClientFake{},
			&brokerFake{},
			&cursorFake{},
		)

		_, err := svc.GetOrderChat(context.Background(), GetOrderChatCommand{
			OrderID: "order-1",
			UserID:  "intruder",
			Limit:   20,
		})
		Expect(err).To(MatchError(domain.ErrChatAccessDenied))
	})

	It("returns one page of decrypted messages and a next cursor", func() {
		msgs := &messageRepoFake{
			listByOrderIDFn: func(ctx context.Context, params domain.ListMessagesParams) ([]domain.Message, error) {
				Expect(params.OrderID).To(Equal("order-1"))
				Expect(params.Limit).To(Equal(2))
				now := time.Unix(100, 0).UTC()
				return []domain.Message{
					{
						OrderID:      "order-1",
						MessageID:    "msg-2",
						SenderUserID: "buyer-1",
						MessageType:  domain.MessageTypeText,
						Ciphertext:   "enc:hello",
						HasText:      true,
						CreatedAt:    now,
						UpdatedAt:    now,
					},
					{
						OrderID:      "order-1",
						MessageID:    "msg-1",
						SenderUserID: "seller-1",
						MessageType:  domain.MessageTypeText,
						Ciphertext:   "enc:older",
						HasText:      true,
						CreatedAt:    now.Add(-time.Minute),
						UpdatedAt:    now.Add(-time.Minute),
					},
				}, nil
			},
		}
		svc := newTestService(
			&chatRepoFake{
				getByOrderIDFn: func(ctx context.Context, orderID string) (*domain.Chat, error) {
					return &domain.Chat{OrderID: orderID, BuyerID: "buyer-1", SellerID: "seller-1", Status: domain.ChatStatusOpen}, nil
				},
			},
			msgs,
			&fileClientFake{},
			&brokerFake{},
			&cursorFake{},
		)

		res, err := svc.GetOrderChat(context.Background(), GetOrderChatCommand{
			OrderID: "order-1",
			UserID:  "buyer-1",
			Limit:   1,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.Messages).To(HaveLen(1))
		Expect(res.Messages[0].Text).To(Equal("hello"))
		Expect(res.NextCursor).To(Equal("cursor-token"))
	})

	It("stores mixed messages and fans them out to both chat participants", func() {
		broker := &brokerFake{}
		files := &fileClientFake{
			getFileURLsFn: func(ctx context.Context, fileIDs []string) (map[string]string, error) {
				return map[string]string{
					"file-1": "http://files.local/file-1",
					"file-2": "http://files.local/file-2",
				}, nil
			},
		}
		msgs := &messageRepoFake{}
		svc := newTestService(
			&chatRepoFake{
				getByOrderIDFn: func(ctx context.Context, orderID string) (*domain.Chat, error) {
					return &domain.Chat{OrderID: orderID, BuyerID: "buyer-1", SellerID: "seller-1", Status: domain.ChatStatusOpen}, nil
				},
			},
			msgs,
			files,
			broker,
			&cursorFake{},
		)

		res, err := svc.CreateMessage(context.Background(), CreateMessageCommand{
			OrderID:       "order-1",
			UserID:        "buyer-1",
			Text:          "hello",
			AttachmentIDs: []string{"file-1", "file-2"},
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.MessageType).To(Equal(domain.MessageTypeMixed))
		Expect(msgs.createParams).To(HaveLen(1))
		Expect(msgs.createParams[0].Ciphertext).To(Equal("enc:hello"))
		Expect(msgs.createParams[0].Attachments).To(HaveLen(2))
		Expect(msgs.createParams[0].Attachments[0].SortOrder).To(Equal(int32(1)))
		Expect(msgs.createParams[0].Attachments[1].SortOrder).To(Equal(int32(2)))
		Expect(broker.publishes).To(HaveLen(2))

		var first RealtimeEnvelope
		Expect(json.Unmarshal(broker.publishes[0].payload, &first)).To(Succeed())
		Expect(first.UserID).To(Equal("buyer-1"))
		Expect(first.DeliveryScope).To(Equal("user"))
		Expect(first.Type).To(Equal("chat_message_created"))
	})

	It("rejects edits from a different sender", func() {
		svc := newTestService(
			&chatRepoFake{
				getByOrderIDFn: func(ctx context.Context, orderID string) (*domain.Chat, error) {
					return &domain.Chat{OrderID: orderID, BuyerID: "buyer-1", SellerID: "seller-1", Status: domain.ChatStatusOpen}, nil
				},
			},
			&messageRepoFake{
				getByIDFn: func(ctx context.Context, orderID, messageID string) (*domain.Message, error) {
					return &domain.Message{OrderID: orderID, MessageID: messageID, SenderUserID: "seller-1"}, nil
				},
			},
			&fileClientFake{},
			&brokerFake{},
			&cursorFake{},
		)

		_, err := svc.EditMessage(context.Background(), EditMessageCommand{
			OrderID:   "order-1",
			UserID:    "buyer-1",
			MessageID: "msg-1",
			Text:      "updated",
		})
		Expect(err).To(MatchError(domain.ErrChatAccessDenied))
	})

	It("passes the authenticated owner when completing attachment uploads", func() {
		files := &fileClientFake{
			completeDirectUploadFn: func(ctx context.Context, fileID string) (*StoredFile, error) {
				Expect(fileID).To(Equal("file-1"))
				return &StoredFile{
					FileID:      fileID,
					Filename:    "proof.txt",
					ContentType: "text/plain",
					SizeBytes:   12,
					URL:         "http://files.local/file-1",
				}, nil
			},
		}
		svc := newTestService(
			&chatRepoFake{
				getByOrderIDFn: func(ctx context.Context, orderID string) (*domain.Chat, error) {
					return &domain.Chat{OrderID: orderID, BuyerID: "buyer-1", SellerID: "seller-1", Status: domain.ChatStatusOpen}, nil
				},
			},
			&messageRepoFake{},
			files,
			&brokerFake{},
			&cursorFake{},
		)

		res, err := svc.CompleteAttachmentUpload(context.Background(), CompleteAttachmentUploadCommand{
			OrderID: "order-1",
			UserID:  "buyer-1",
			FileID:  "file-1",
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(res.FileID).To(Equal("file-1"))
		Expect(res.URL).To(Equal("http://files.local/file-1"))
		Expect(files.completeCalls).To(Equal([]string{"file-1"}))
	})
})
