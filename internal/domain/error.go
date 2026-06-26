package domain

import "errors"

var (
	// ErrChatNotFound indicates that the requested order chat does not exist.
	ErrChatNotFound = errors.New("chat not found")
	// ErrMessageNotFound indicates that the requested chat message does not exist.
	ErrMessageNotFound = errors.New("message not found")
	// ErrChatClosed indicates that the caller attempted to mutate a closed chat.
	ErrChatClosed = errors.New("chat is closed")
	// ErrChatAccessDenied indicates that the caller does not belong to the chat.
	ErrChatAccessDenied = errors.New("chat access denied")
	// ErrInvalidParticipant indicates that the chat participants are invalid.
	ErrInvalidParticipant = errors.New("invalid chat participant")
	// ErrInvalidMessage indicates that the supplied message payload is invalid.
	ErrInvalidMessage = errors.New("invalid chat message")
	// ErrInvalidCursor indicates that the supplied cursor cannot be decoded.
	ErrInvalidCursor = errors.New("invalid chat cursor")
)
