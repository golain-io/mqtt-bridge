package bridge

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

const (
	SetOptions byte = iota
	OnMessageReceived
	OnSessionCreated
	OnSessionResumed
	OnSessionSuspended
	OnSessionDisconnected
	StoredSessions
)

var (
	// ErrInvalidConfigType indicates a different Type of config value was expected to what was received.
	ErrInvalidConfigType = errors.New("invalid config type provided")
)

// HookLoadConfig contains the hook and configuration as loaded from a configuration (usually file).
type HookLoadConfig struct {
	Hook   BridgeHook
	Config any
}

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

	// SetOpts is called by the server to propagate internal values
	SetOpts(l *zap.Logger, o *HookOptions)

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

	// StoredSessions returns stored sessions
	StoredSessions() ([]SessionInfo, error)
}

// HookOptions contains values which are inherited from the server on initialisation.
type HookOptions struct {
	// Add any server capabilities or options needed by hooks
}

// BridgeHooks manages a collection of hooks
type BridgeHooks struct {
	logger   *zap.Logger
	internal atomic.Value // []BridgeHook
	wg       sync.WaitGroup
	qty      int64
	sync.Mutex
}

// Add adds a new hook to the collection
func (h *BridgeHooks) Add(hook BridgeHook, config any) error {
	h.Lock()
	defer h.Unlock()

	if err := hook.Init(config); err != nil {
		return fmt.Errorf("failed to initialize hook: %v", err)
	}

	i, ok := h.internal.Load().([]BridgeHook)
	if !ok {
		i = []BridgeHook{}
	}

	i = append(i, hook)
	h.internal.Store(i)
	atomic.AddInt64(&h.qty, 1)
	h.wg.Add(1)

	return nil
}

// GetAll returns a slice of all the hooks.
func (h *BridgeHooks) GetAll() []BridgeHook {
	i, ok := h.internal.Load().([]BridgeHook)
	if !ok {
		return []BridgeHook{}
	}
	return i
}

// Stop stops all hooks
func (h *BridgeHooks) Stop() {
	go func() {
		for _, hook := range h.GetAll() {
			h.logger.Info("stopping hook", zap.String("hook", hook.ID()))
			if err := hook.Stop(); err != nil {
				h.logger.Error("Failed to stop hook",
					zap.String("hook", hook.ID()),
					zap.Error(err))
			}
			h.wg.Done()
		}
	}()
	h.wg.Wait()
}

// Len returns the number of hooks added.
func (h *BridgeHooks) Len() int64 {
	return atomic.LoadInt64(&h.qty)
}

// Provides returns true if any one hook provides any of the requested hook methods.
func (h *BridgeHooks) Provides(b ...byte) bool {
	for _, hook := range h.GetAll() {
		for _, hb := range b {
			if hook.Provides(hb) {
				return true
			}
		}
	}
	return false
}

// OnMessageReceived processes a message through all hooks
func (h *BridgeHooks) OnMessageReceived(msg []byte) []byte {
	if h == nil {
		return msg
	}
	result := msg
	for _, hook := range h.GetAll() {
		if hook.Provides(OnMessageReceived) {
			result = hook.OnMessageReceived(result)
		}
	}
	return result
}

// OnSessionCreated calls the OnSessionCreated hook for all hooks that provide it
func (h *BridgeHooks) OnSessionCreated(session *SessionInfo) error {
	if h == nil {
		return nil
	}
	for _, hook := range h.GetAll() {
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
	for _, hook := range h.GetAll() {
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
	for _, hook := range h.GetAll() {
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
	for _, hook := range h.GetAll() {
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

// StoredSessions calls the StoredSessions hook for all hooks that provide it
func (h *BridgeHooks) StoredSessions() ([]SessionInfo, error) {
	if h == nil {
		return nil, nil
	}
	for _, hook := range h.GetAll() {
		if hook.Provides(StoredSessions) {
			sessions, err := hook.StoredSessions()
			if err != nil {
				h.logger.Error("Failed to get stored sessions",
					zap.String("hook", hook.ID()),
					zap.Error(err))
				return nil, err
			}
			if len(sessions) > 0 {
				return sessions, nil
			}
		}
	}
	return nil, nil
}

// BridgeHookBase provides a set of default methods for each hook
type BridgeHookBase struct {
	BridgeHook
	Log  *zap.Logger
	Opts *HookOptions
}

// ID returns the ID of the hook
func (h *BridgeHookBase) ID() string {
	return "base"
}

// Provides indicates which methods a hook provides
func (h *BridgeHookBase) Provides(b byte) bool {
	return false
}

// Init initializes the hook
func (h *BridgeHookBase) Init(config any) error {
	return nil
}

// SetOpts sets the options for the hook
func (h *BridgeHookBase) SetOpts(l *zap.Logger, opts *HookOptions) {
	h.Log = l
	h.Opts = opts
}

// Stop stops the hook
func (h *BridgeHookBase) Stop() error {
	return nil
}

// OnMessageReceived processes incoming messages
func (h *BridgeHookBase) OnMessageReceived(msg []byte) []byte {
	return msg
}

// OnSessionCreated is called when a new session is created
func (h *BridgeHookBase) OnSessionCreated(session *SessionInfo) error {
	return nil
}

// OnSessionResumed is called when a session is resumed
func (h *BridgeHookBase) OnSessionResumed(session *SessionInfo) error {
	return nil
}

// OnSessionSuspended is called when a session is suspended
func (h *BridgeHookBase) OnSessionSuspended(session *SessionInfo) error {
	return nil
}

// OnSessionDisconnected is called when a session is disconnected
func (h *BridgeHookBase) OnSessionDisconnected(session *SessionInfo) error {
	return nil
}

// StoredSessions returns stored sessions
func (h *BridgeHookBase) StoredSessions() ([]SessionInfo, error) {
	return nil, nil
}
