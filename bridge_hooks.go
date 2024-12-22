package bridge

import (
	"fmt"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

const (
	OnMessageReceived byte = iota
)

// BridgeHook provides an interface for handling bridge-related events.
type BridgeHook interface {
	OnMessageReceived(msg []byte) []byte
	Provides(b byte) bool
	Init(config any) error
	Stop() error
	ID() string
}

// BridgeHooks manages a collection of bridge hooks.
type BridgeHooks struct {
	internal atomic.Value
	qty      int64
	wg       sync.WaitGroup
	sync.RWMutex
	logger *zap.Logger
}

// OnMessageReceived is called when a message is received from a bridge connection.
func (h *BridgeHooks) OnMessageReceived(msg []byte) []byte {
	h.RLock()
	defer h.RUnlock()

	for _, hook := range h.GetAll() {
		if hook.Provides(OnMessageReceived) {
			msg = hook.OnMessageReceived(msg)
		}
	}
	return msg
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

// Add adds and initializes a new hook.
func (h *BridgeHooks) Add(hook BridgeHook, config any) error {
	h.Lock()
	defer h.Unlock()

	err := hook.Init(config)
	if err != nil {
		return fmt.Errorf("failed initialising %s hook: %w", hook.ID(), err)
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

// Stop indicates all attached hooks to gracefully end.
func (h *BridgeHooks) Stop() {
	go func() {
		for _, hook := range h.GetAll() {
			if err := hook.Stop(); err != nil {
				h.logger.Error("problem stopping hook",
					zap.Error(err),
					zap.String("hook", hook.ID()))
			}
			h.wg.Done()
		}
	}()
	h.wg.Wait()
}
