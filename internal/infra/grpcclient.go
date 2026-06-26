package infra

import (
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"github.com/ofm-microservices/ofm-common/pkg/observability/metrics"
	otelgrpc "go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	grpcpkg "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// NewConn opens a gRPC client connection with the service's shared telemetry settings.
func NewConn(address string, log logging.Logger, module string) (*grpcpkg.ClientConn, logging.Logger, error) {
	conn, err := grpcpkg.NewClient(address, grpcpkg.WithTransportCredentials(insecure.NewCredentials()), grpcpkg.WithStatsHandler(otelgrpc.NewClientHandler()), grpcpkg.WithUnaryInterceptor(metrics.UnaryClientInterceptor()))
	if err != nil {
		return nil, nil, err
	}
	return conn, log.With(logging.String("module", module), logging.String("address", address)), nil
}

// CloseConn closes a gRPC client connection safely.
func CloseConn(conn *grpcpkg.ClientConn) error {
	if conn == nil {
		return nil
	}
	return conn.Close()
}
