package grpc

import "errors"

var (
	// ErrNilLogger reports a missing logger dependency.
	ErrNilLogger = errors.New("logger is nil")
	// ErrEmptyAddress reports a missing file-service address.
	ErrEmptyAddress = errors.New("file service address is empty")
)
