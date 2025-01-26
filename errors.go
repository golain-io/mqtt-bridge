package bridge

import "fmt"

// BridgeError represents a base error type for bridge-related errors
type BridgeError struct {
	Op      string // Operation that failed
	Message string // Human-readable error message
	Err     error  // Underlying error if any
}

func (e *BridgeError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %s: %v", e.Op, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: %s", e.Op, e.Message)
}

func (e *BridgeError) Unwrap() error {
	return e.Err
}

// SessionError represents session-specific errors
type SessionError struct {
	BridgeError
	SessionID string
}

func (e *SessionError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: session %s: %s: %v", e.Op, e.SessionID, e.Message, e.Err)
	}
	return fmt.Sprintf("%s: session %s: %s", e.Op, e.SessionID, e.Message)
}

// Specific error types for different scenarios
var (
	ErrSessionActive    = &SessionError{BridgeError: BridgeError{Message: "session is already active"}}
	ErrSessionNotFound  = &SessionError{BridgeError: BridgeError{Message: "session not found"}}
	ErrInvalidSession   = &SessionError{BridgeError: BridgeError{Message: "invalid session"}}
	ErrSessionSuspended = &SessionError{BridgeError: BridgeError{Message: "session is suspended"}}
	ErrUnauthorized     = &SessionError{BridgeError: BridgeError{Message: "unauthorized operation"}}
	ErrConnectionFailed = &SessionError{BridgeError: BridgeError{Message: "failed to create connection"}}

	// Additional error types
	ErrGeneric        = &SessionError{BridgeError: BridgeError{Message: "error"}}
	ErrSessionClosed  = &SessionError{BridgeError: BridgeError{Message: "session is closed"}}
	ErrInvalidState   = &SessionError{BridgeError: BridgeError{Message: "invalid session state"}}
	ErrSessionExpired = &SessionError{BridgeError: BridgeError{Message: "session has expired"}}
	ErrMaxSessions    = &SessionError{BridgeError: BridgeError{Message: "maximum number of sessions reached"}}
)

// Error constructors for common operations
func NewSessionActiveError(sessionID string) error {
	return &SessionError{
		BridgeError: BridgeError{
			Op:      "dial",
			Message: ErrSessionActive.Message,
		},
		SessionID: sessionID,
	}
}

func NewSessionNotFoundError(sessionID string) error {
	return &SessionError{
		BridgeError: BridgeError{
			Op:      "dial",
			Message: ErrSessionNotFound.Message,
		},
		SessionID: sessionID,
	}
}

func NewInvalidSessionError(sessionID string) error {
	return &SessionError{
		BridgeError: BridgeError{
			Op:      "dial",
			Message: ErrInvalidSession.Message,
		},
		SessionID: sessionID,
	}
}

func NewSessionSuspendedError(sessionID string) error {
	return &SessionError{
		BridgeError: BridgeError{
			Op:      "dial",
			Message: ErrSessionSuspended.Message,
		},
		SessionID: sessionID,
	}
}

func NewUnauthorizedError(sessionID string) error {
	return &SessionError{
		BridgeError: BridgeError{
			Op:      "dial",
			Message: ErrUnauthorized.Message,
		},
		SessionID: sessionID,
	}
}

func NewConnectionFailedError(sessionID string, err error) error {
	return &SessionError{
		BridgeError: BridgeError{
			Op:      "dial",
			Message: ErrConnectionFailed.Message,
			Err:     err,
		},
		SessionID: sessionID,
	}
}

// Helper function to create a generic bridge error
func NewBridgeError(op, message string, err error) error {
	return &BridgeError{
		Op:      op,
		Message: message,
		Err:     err,
	}
}
