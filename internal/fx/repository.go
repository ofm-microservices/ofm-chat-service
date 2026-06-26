package appfx

import (
	"github.com/gocql/gocql"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"chat-service/internal/domain"
	scyllarepo "chat-service/internal/infra/scylla"

	"go.uber.org/fx"
)

// RepoModule provides the concrete saga repositories.
var RepoModule = fx.Options(
	fx.Provide(ProvideChatRepository),
	fx.Provide(ProvideMessageRepository),
)

// ProvideChatRepository constructs the Scylla-backed chat repository.
func ProvideChatRepository(db *gocql.Session, lg logging.Logger) (domain.ChatRepository, error) {
	return scyllarepo.NewChatRepository(db, lg)
}

// ProvideMessageRepository constructs the Scylla-backed message repository.
func ProvideMessageRepository(db *gocql.Session, lg logging.Logger) (domain.MessageRepository, error) {
	return scyllarepo.NewMessageRepository(db, lg)
}
