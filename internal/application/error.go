package application

import "errors"

var (
	// ErrNilChatRepository reports a missing chat repository dependency.
	ErrNilChatRepository = errors.New("chat repository is nil")
	// ErrNilMessageRepository reports a missing message repository dependency.
	ErrNilMessageRepository = errors.New("message repository is nil")
	// ErrNilEventBroker reports a missing broker dependency.
	ErrNilEventBroker = errors.New("event broker is nil")
	// ErrNilFileClient reports a missing file-service client dependency.
	ErrNilFileClient = errors.New("file client is nil")
	// ErrNilCursorCodec reports a missing cursor codec dependency.
	ErrNilCursorCodec = errors.New("cursor codec is nil")
	// ErrNilMessageCipher reports a missing message cipher dependency.
	ErrNilMessageCipher = errors.New("message cipher is nil")
	// ErrNilLogger reports a missing logger dependency.
	ErrNilLogger = errors.New("logger is nil")
)
