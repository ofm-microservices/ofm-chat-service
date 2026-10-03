package appfx

import (
	"chat-service/internal/domain"
	pgrepo "chat-service/internal/infra/postgres"
	"github.com/jmoiron/sqlx"
	"github.com/ofm-microservices/ofm-common/pkg/logging"
	"go.uber.org/fx"
)

// RepoModule provides the PostgreSQL-backed chat repositories.
var RepoModule = fx.Options(fx.Provide(ProvideChatRepository), fx.Provide(ProvideMessageRepository))

// ProvideChatRepository constructs the PostgreSQL chat repository.
func ProvideChatRepository(db *sqlx.DB, lg logging.Logger) (domain.ChatRepository, error) {
	return pgrepo.NewChatRepository(db, lg)
}

// ProvideMessageRepository constructs the PostgreSQL message repository.
func ProvideMessageRepository(db *sqlx.DB, lg logging.Logger) (domain.MessageRepository, error) {
	return pgrepo.NewMessageRepository(db, lg)
}
