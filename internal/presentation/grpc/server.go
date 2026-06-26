package grpc

import (
	"context"
	"errors"
	"fmt"
	"net"

	"chat-service/config"
	"chat-service/internal/domain"

	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	chatv1 "github.com/ofm-microservices/ofm-common/proto/chat/v1"
	otelgrpc "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type server struct {
	chatv1.UnimplementedChatServiceServer
	svc      Service
	cfg      config.GRPCConfig
	log      Logger
	mapr     *chatMapper
	srv      *grpc.Server
	listener net.Listener
}

// NewServer constructs the chat-service gRPC server.
func NewServer(svc Service, cfg config.GRPCConfig, log Logger) (Server, error) {
	if svc == nil {
		return nil, ErrNilService
	}
	if log == nil {
		return nil, ErrNilLogger
	}
	grpcSrv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.UnaryInterceptor(metrics.UnaryServerInterceptor()),
	)
	s := &server{
		svc:  svc,
		cfg:  cfg,
		log:  log.With(logging.String("module", "grpc-server")),
		mapr: newChatMapper(),
		srv:  grpcSrv,
	}
	chatv1.RegisterChatServiceServer(grpcSrv, s)
	return s, nil
}

// Start begins serving gRPC traffic on the configured address.
func (s *server) Start() error {
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.listener = lis
	s.log.Info("starting grpc server", logging.String("addr", addr))
	return s.srv.Serve(lis)
}

// Shutdown gracefully stops the gRPC server.
func (s *server) Shutdown(context.Context) error {
	if s.srv != nil {
		s.srv.GracefulStop()
	}
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// GetOrderChat returns one paginated order chat timeline.
func (s *server) GetOrderChat(ctx context.Context, req *chatv1.GetOrderChatRequest) (*chatv1.GetOrderChatResponse, error) {
	result, err := s.svc.GetOrderChat(ctx, s.mapr.toGetOrderChatCommand(req))
	if err != nil {
		return nil, toStatus(err)
	}
	return s.mapr.toGetOrderChatResponse(result), nil
}

// CreateMessage stores one new order chat message.
func (s *server) CreateMessage(ctx context.Context, req *chatv1.CreateMessageRequest) (*chatv1.CreateMessageResponse, error) {
	view, err := s.svc.CreateMessage(ctx, s.mapr.toCreateMessageCommand(req))
	if err != nil {
		return nil, toStatus(err)
	}
	return s.mapr.toCreateMessageResponse(view), nil
}

// EditMessage updates one existing order chat message.
func (s *server) EditMessage(ctx context.Context, req *chatv1.EditMessageRequest) (*chatv1.EditMessageResponse, error) {
	view, err := s.svc.EditMessage(ctx, s.mapr.toEditMessageCommand(req))
	if err != nil {
		return nil, toStatus(err)
	}
	return s.mapr.toEditMessageResponse(view), nil
}

// DeleteMessage soft-deletes one order chat message.
func (s *server) DeleteMessage(ctx context.Context, req *chatv1.DeleteMessageRequest) (*chatv1.DeleteMessageResponse, error) {
	view, err := s.svc.DeleteMessage(ctx, s.mapr.toDeleteMessageCommand(req))
	if err != nil {
		return nil, toStatus(err)
	}
	return s.mapr.toDeleteMessageResponse(view), nil
}

// CreateAttachmentUploadURL reserves one direct upload for a chat attachment.
func (s *server) CreateAttachmentUploadURL(ctx context.Context, req *chatv1.CreateAttachmentUploadURLRequest) (*chatv1.CreateAttachmentUploadURLResponse, error) {
	result, err := s.svc.CreateAttachmentUploadURL(ctx, s.mapr.toCreateAttachmentUploadURLCommand(req))
	if err != nil {
		return nil, toStatus(err)
	}
	return s.mapr.toCreateAttachmentUploadURLResponse(result), nil
}

// CompleteAttachmentUpload finalizes one direct upload and returns attachment metadata.
func (s *server) CompleteAttachmentUpload(ctx context.Context, req *chatv1.CompleteAttachmentUploadRequest) (*chatv1.CompleteAttachmentUploadResponse, error) {
	view, err := s.svc.CompleteAttachmentUpload(ctx, s.mapr.toCompleteAttachmentUploadCommand(req))
	if err != nil {
		return nil, toStatus(err)
	}
	return s.mapr.toCompleteAttachmentUploadResponse(view), nil
}

func toStatus(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, domain.ErrChatNotFound), errors.Is(err, domain.ErrMessageNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrChatAccessDenied):
		return status.Error(codes.PermissionDenied, err.Error())
	case errors.Is(err, domain.ErrChatClosed):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrInvalidParticipant), errors.Is(err, domain.ErrInvalidMessage), errors.Is(err, domain.ErrInvalidCursor):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, "internal server error")
	}
}
