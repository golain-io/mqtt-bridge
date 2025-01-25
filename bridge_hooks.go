package bridge

import (
	"fmt"

	"go.uber.org/zap"
)

const (
	OnMessageReceived byte = iota
	OnSessionCreated
	OnSessionResumed
	OnSessionSuspended
	OnSessionDisconnected
)

// BridgeHook defines the interface for bridge hooks
type BridgeHook interface {
	// ID returns the unique identifier for this hook
	ID() string

	// Init initializes the hook with the provided configuration
	Init(config any) error

	// Stop gracefully stops the hook
	Stop() error

	// Provides indicates whether this hook provides the specified functionality
	Provides(b byte) bool

	// OnMessageReceived processes incoming messages
	OnMessageReceived(msg []byte) []byte

	// OnSessionCreated is called when a new session is created
	OnSessionCreated(session *SessionInfo) error

	// OnSessionResumed is called when a session is resumed
	OnSessionResumed(session *SessionInfo) error

	// OnSessionSuspended is called when a session is suspended
	OnSessionSuspended(session *SessionInfo) error

	// OnSessionDisconnected is called when a session is disconnected
	OnSessionDisconnected(session *SessionInfo) error
}

// BridgeHooks manages a collection of hooks
type BridgeHooks struct {
	logger *zap.Logger
	Hooks  []BridgeHook
}

// Add adds a new hook to the collection
func (h *BridgeHooks) Add(hook BridgeHook, config any) error {
	if err := hook.Init(config); err != nil {
		return fmt.Errorf("failed to initialize hook: %v", err)
	}
	h.Hooks = append(h.Hooks, hook)
	return nil
}

// OnMessageReceived processes a message through all hooks
func (h *BridgeHooks) OnMessageReceived(msg []byte) []byte {
	if h == nil {
		return msg
	}
	result := msg
	for _, hook := range h.Hooks {
		if hook.Provides(OnMessageReceived) {
			result = hook.OnMessageReceived(result)
		}
	}
	return result
}

// Stop stops all hooks
func (h *BridgeHooks) Stop() {
	if h == nil {
		return
	}
	for _, hook := range h.Hooks {
		if err := hook.Stop(); err != nil {
			h.logger.Error("Failed to stop hook",
				zap.String("hook", hook.ID()),
				zap.Error(err))
		}
	}
}

// Len returns the number of hooks added.
func (h *BridgeHooks) Len() int64 {
	return int64(len(h.Hooks))
}

// Provides returns true if any one hook provides any of the requested hook methods.
func (h *BridgeHooks) Provides(b ...byte) bool {
	for _, hook := range h.Hooks {
		for _, hb := range b {
			if hook.Provides(hb) {
				return true
			}
		}
	}
	return false
}

// GetAll returns a slice of all the hooks.
func (h *BridgeHooks) GetAll() []BridgeHook {
	return h.Hooks
}

// OnSessionCreated calls the OnSessionCreated hook for all hooks that provide it
func (h *BridgeHooks) OnSessionCreated(session *SessionInfo) error {
	if h == nil {
		return nil
	}
	for _, hook := range h.Hooks {
		if hook.Provides(OnSessionCreated) {
			if err := hook.OnSessionCreated(session); err != nil {
				h.logger.Error("Failed to execute OnSessionCreated hook",
					zap.String("hook", hook.ID()),
					zap.Error(err))
				return err
			}
		}
	}
	return nil
}

// OnSessionResumed calls the OnSessionResumed hook for all hooks that provide it
func (h *BridgeHooks) OnSessionResumed(session *SessionInfo) error {
	if h == nil {
		return nil
	}
	for _, hook := range h.Hooks {
		if hook.Provides(OnSessionResumed) {
			if err := hook.OnSessionResumed(session); err != nil {
				h.logger.Error("Failed to execute OnSessionResumed hook",
					zap.String("hook", hook.ID()),
					zap.Error(err))
				return err
			}
		}
	}
	return nil
}

// OnSessionSuspended calls the OnSessionSuspended hook for all hooks that provide it
func (h *BridgeHooks) OnSessionSuspended(session *SessionInfo) error {
	if h == nil {
		return nil
	}
	for _, hook := range h.Hooks {
		if hook.Provides(OnSessionSuspended) {
			if err := hook.OnSessionSuspended(session); err != nil {
				h.logger.Error("Failed to execute OnSessionSuspended hook",
					zap.String("hook", hook.ID()),
					zap.Error(err))
				return err
			}
		}
	}
	return nil
}

// OnSessionDisconnected calls the OnSessionDisconnected hook for all hooks that provide it
func (h *BridgeHooks) OnSessionDisconnected(session *SessionInfo) error {
	if h == nil {
		return nil
	}
	for _, hook := range h.Hooks {
		if hook.Provides(OnSessionDisconnected) {
			if err := hook.OnSessionDisconnected(session); err != nil {
				h.logger.Error("Failed to execute OnSessionDisconnected hook",
					zap.String("hook", hook.ID()),
					zap.Error(err))
				return err
			}
		}
	}
	return nil
}
