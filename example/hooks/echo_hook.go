package hooks

import (
	"fmt"
	"sync/atomic"
	"go.uber.org/zap"

	bridge "github.com/golain-io/mqtt-bridge"
)

// EchoHook implements BridgeHook interface to provide message logging functionality
type EchoHook struct {
	logger    *zap.Logger
	isRunning atomic.Bool
	id        string
}

// NewEchoHook creates a new LoggingHook instance
func NewEchoHook(logger *zap.Logger) *EchoHook {
	return &EchoHook{
		logger: logger,
		id:     "net_bridge_hook",
	}
}

// OnMessageReceived logs the received message
func (h *EchoHook) OnMessageReceived(msg []byte) error {
	if !h.isRunning.Load() {
		return fmt.Errorf("hook %s is not running", h.id)
	}

	h.logger.Info("message received echo",
		zap.ByteString("message", msg),
		zap.String("hook_id", h.id))
	return nil
}

// Provides indicates whether this hook provides the specified functionality
func (h *EchoHook) Provides(b byte) bool {
	return b == bridge.OnMessageReceived
}

// Init initializes the hook with the provided configuration
func (h *EchoHook) Init(config any) error {
	if config != nil {
		// You could add configuration handling here
		// For example, if config contains log level or other settings
	}
	h.isRunning.Store(true)
	return nil
}

// Stop gracefully stops the hook
func (h *EchoHook) Stop() error {
	h.isRunning.Store(false)
	return nil
}

// ID returns the unique identifier for this hook
func (h *EchoHook) ID() string {
	return h.id
}
