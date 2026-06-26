package appfx

import (
	"chat-service/config"
	app "chat-service/internal/application"
	"chat-service/internal/domain"
	"github.com/ofm-microservices/ofm-common/pkg/cursor"
	"github.com/ofm-microservices/ofm-common/pkg/logging"

	"go.uber.org/fx"
)

// ServiceModule provides the application service.
var ServiceModule = fx.Options(
	fx.Provide(
		ProvideCursorCodec,
		ProvideMessageCipher,
		ProvideService,
	),
)

// ProvideCursorCodec constructs the opaque cursor codec used by message pagination.
func ProvideCursorCodec(cfg *config.Config) (app.CursorCodec, error) {
	return cursor.NewCodec(cursor.Config{Secret: cfg.Cursor.Secret})
}

// ProvideMessageCipher constructs the at-rest text cipher used by chat-service.
func ProvideMessageCipher(cfg *config.Config) (app.MessageCipher, error) {
	return app.NewMessageCipher(cfg.Crypto.Secret)
}

// ProvideService constructs the chat application service.
func ProvideService(chats domain.ChatRepository, msgs domain.MessageRepository, files app.FileClient, broker app.EventBroker, cur app.CursorCodec, cipher app.MessageCipher, cfg *config.Config, lg logging.Logger) (app.Service, error) {
	return app.New(chats, msgs, files, broker, cur, cipher, cfg.NATS, lg)
}
