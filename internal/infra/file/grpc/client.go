package grpc

import (
	"context"
	"strings"
	"time"

	"chat-service/config"
	app "chat-service/internal/application"
	grpcinfra "chat-service/internal/infra"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	filev1 "github.com/ofm-microservices/ofm-common/proto/file/v1"
	grpcpkg "google.golang.org/grpc"
)

type client struct {
	conn *grpcpkg.ClientConn
	cl   filev1.FileServiceClient
	log  logging.Logger
}

// New constructs the file-service gRPC client used by chat-service.
func New(cfg config.FileServiceConfig, log logging.Logger) (Client, error) {
	if log == nil {
		return nil, ErrNilLogger
	}
	addr := strings.TrimSpace(cfg.Address)
	if addr == "" {
		return nil, ErrEmptyAddress
	}
	conn, lg, err := grpcinfra.NewConn(addr, log, "file-grpc-client")
	if err != nil {
		return nil, err
	}
	return &client{
		conn: conn,
		cl:   filev1.NewFileServiceClient(conn),
		log:  lg,
	}, nil
}

func (c *client) CreateDirectUpload(ctx context.Context, cmd app.CreateDirectUploadCommand) (*app.DirectUploadReservation, error) {
	res, err := c.cl.CreateDirectUpload(ctx, &filev1.CreateDirectUploadRequest{
		OwnerId:     cmd.OwnerID,
		Filename:    cmd.Filename,
		ContentType: cmd.ContentType,
		SizeBytes:   cmd.SizeBytes,
	})
	if err != nil {
		return nil, err
	}
	expiresAt, _ := time.Parse(time.RFC3339Nano, res.GetExpiresAt())
	return &app.DirectUploadReservation{
		FileID:    res.GetFileId(),
		UploadURL: res.GetUploadUrl(),
		Method:    res.GetMethod(),
		Headers:   res.GetHeaders(),
		ExpiresAt: expiresAt,
	}, nil
}

func (c *client) CompleteDirectUpload(ctx context.Context, fileID string) (*app.StoredFile, error) {
	res, err := c.cl.CompleteDirectUpload(ctx, &filev1.CompleteDirectUploadRequest{
		FileId: fileID,
	})
	if err != nil {
		return nil, err
	}
	file := res.GetFile()
	return &app.StoredFile{
		FileID:      file.GetFileId(),
		Filename:    file.GetFilename(),
		ContentType: file.GetContentType(),
		SizeBytes:   file.GetSizeBytes(),
		URL:         res.GetUrl(),
	}, nil
}

func (c *client) GetFileURLs(ctx context.Context, fileIDs []string) (map[string]string, error) {
	res, err := c.cl.GetFileURLs(ctx, &filev1.GetFileURLsRequest{FileIds: fileIDs})
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(res.GetFileUrls()))
	for _, item := range res.GetFileUrls() {
		out[item.GetFileId()] = item.GetUrl()
	}
	return out, nil
}

func (c *client) Close() error {
	return grpcinfra.CloseConn(c.conn)
}
