package grpc

import (
	app "chat-service/internal/application"

	chatv1 "github.com/ofm-microservices/ofm-common/proto/chat/v1"
)

type chatMapper struct{}

func newChatMapper() *chatMapper {
	return &chatMapper{}
}

func (m *chatMapper) toGetOrderChatCommand(req *chatv1.GetOrderChatRequest) app.GetOrderChatCommand {
	return app.GetOrderChatCommand{
		OrderID: req.GetOrderId(),
		UserID:  req.GetUserId(),
		Cursor:  req.GetCursor(),
		Limit:   int(req.GetLimit()),
	}
}

func (m *chatMapper) toCreateMessageCommand(req *chatv1.CreateMessageRequest) app.CreateMessageCommand {
	return app.CreateMessageCommand{
		OrderID:       req.GetOrderId(),
		UserID:        req.GetUserId(),
		Text:          req.GetText(),
		AttachmentIDs: req.GetAttachmentIds(),
	}
}

func (m *chatMapper) toEditMessageCommand(req *chatv1.EditMessageRequest) app.EditMessageCommand {
	return app.EditMessageCommand{
		OrderID:   req.GetOrderId(),
		UserID:    req.GetUserId(),
		MessageID: req.GetMessageId(),
		Text:      req.GetText(),
	}
}

func (m *chatMapper) toDeleteMessageCommand(req *chatv1.DeleteMessageRequest) app.DeleteMessageCommand {
	return app.DeleteMessageCommand{
		OrderID:   req.GetOrderId(),
		UserID:    req.GetUserId(),
		MessageID: req.GetMessageId(),
	}
}

func (m *chatMapper) toCreateAttachmentUploadURLCommand(req *chatv1.CreateAttachmentUploadURLRequest) app.CreateAttachmentUploadURLCommand {
	return app.CreateAttachmentUploadURLCommand{
		OrderID:     req.GetOrderId(),
		UserID:      req.GetUserId(),
		Filename:    req.GetFilename(),
		ContentType: req.GetContentType(),
		SizeBytes:   req.GetSizeBytes(),
	}
}

func (m *chatMapper) toCompleteAttachmentUploadCommand(req *chatv1.CompleteAttachmentUploadRequest) app.CompleteAttachmentUploadCommand {
	return app.CompleteAttachmentUploadCommand{
		OrderID: req.GetOrderId(),
		UserID:  req.GetUserId(),
		FileID:  req.GetFileId(),
	}
}

func (m *chatMapper) toMessageProto(view app.MessageView) *chatv1.ChatMessage {
	attachments := make([]*chatv1.ChatAttachment, 0, len(view.Attachments))
	for _, item := range view.Attachments {
		attachments = append(attachments, &chatv1.ChatAttachment{
			FileId:      item.FileID,
			Filename:    item.Filename,
			ContentType: item.ContentType,
			SizeBytes:   item.SizeBytes,
			Url:         item.URL,
		})
	}
	return &chatv1.ChatMessage{
		MessageId:    view.MessageID,
		OrderId:      view.OrderID,
		SenderUserId: view.SenderUserID,
		MessageType:  view.MessageType,
		Text:         view.Text,
		Deleted:      view.Deleted,
		Edited:       view.Edited,
		Attachments:  attachments,
		CreatedAt:    view.CreatedAt,
		UpdatedAt:    view.UpdatedAt,
	}
}

func (m *chatMapper) toGetOrderChatResponse(result *app.GetOrderChatResult) *chatv1.GetOrderChatResponse {
	if result == nil {
		return &chatv1.GetOrderChatResponse{}
	}
	messages := make([]*chatv1.ChatMessage, 0, len(result.Messages))
	for _, item := range result.Messages {
		messages = append(messages, m.toMessageProto(item))
	}
	return &chatv1.GetOrderChatResponse{
		OrderId:     result.OrderID,
		BuyerId:     result.BuyerID,
		SellerId:    result.SellerID,
		Status:      result.Status,
		CloseReason: result.CloseReason,
		Messages:    messages,
		NextCursor:  result.NextCursor,
	}
}

func (m *chatMapper) toCreateMessageResponse(view *app.MessageView) *chatv1.CreateMessageResponse {
	if view == nil {
		return &chatv1.CreateMessageResponse{}
	}
	return &chatv1.CreateMessageResponse{Message: m.toMessageProto(*view)}
}

func (m *chatMapper) toEditMessageResponse(view *app.MessageView) *chatv1.EditMessageResponse {
	if view == nil {
		return &chatv1.EditMessageResponse{}
	}
	return &chatv1.EditMessageResponse{Message: m.toMessageProto(*view)}
}

func (m *chatMapper) toDeleteMessageResponse(view *app.MessageView) *chatv1.DeleteMessageResponse {
	if view == nil {
		return &chatv1.DeleteMessageResponse{}
	}
	return &chatv1.DeleteMessageResponse{Message: m.toMessageProto(*view)}
}

func (m *chatMapper) toCreateAttachmentUploadURLResponse(result *app.CreateAttachmentUploadURLResult) *chatv1.CreateAttachmentUploadURLResponse {
	if result == nil {
		return &chatv1.CreateAttachmentUploadURLResponse{}
	}
	return &chatv1.CreateAttachmentUploadURLResponse{
		FileId:    result.FileID,
		UploadUrl: result.UploadURL,
		Method:    result.Method,
		Headers:   result.Headers,
		ExpiresAt: result.ExpiresAt,
	}
}

func (m *chatMapper) toCompleteAttachmentUploadResponse(view *app.AttachmentView) *chatv1.CompleteAttachmentUploadResponse {
	if view == nil {
		return &chatv1.CompleteAttachmentUploadResponse{}
	}
	return &chatv1.CompleteAttachmentUploadResponse{
		Attachment: &chatv1.ChatAttachment{
			FileId:      view.FileID,
			Filename:    view.Filename,
			ContentType: view.ContentType,
			SizeBytes:   view.SizeBytes,
			Url:         view.URL,
		},
	}
}
