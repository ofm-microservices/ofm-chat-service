package grpc

import (
	"context"

	app "chat-service/internal/application"
)

// Client defines the file-service operations consumed by chat-service.
type Client interface {
	CreateDirectUpload(ctx context.Context, cmd app.CreateDirectUploadCommand) (*app.DirectUploadReservation, error)
	CompleteDirectUpload(ctx context.Context, fileID string) (*app.StoredFile, error)
	GetFileURLs(ctx context.Context, fileIDs []string) (map[string]string, error)
	Close() error
}
