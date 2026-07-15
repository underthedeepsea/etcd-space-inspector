// Package apperr defines stable errors safe to expose through CLI and HTTP.
package apperr

// Error carries a stable code, a safe message, and an internal cause.
type Error struct {
	Code    string
	Message string
	Cause   error
}

// Error returns only the safe user-facing message.
func (e *Error) Error() string { return e.Message }

// Unwrap exposes the internal cause to errors.Is and errors.As.
func (e *Error) Unwrap() error { return e.Cause }

// E constructs a coded application error.
func E(code, message string, cause error) error {
	return &Error{Code: code, Message: message, Cause: cause}
}
