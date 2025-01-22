package bridge

import (
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
)

// BridgeOption configures bridge behavior
type BridgeOption func(*BridgeConfig)

// BridgeConfig holds bridge-specific configuration
type BridgeConfig struct {
	rootTopic  string
	qos        byte
	logger     *zap.Logger
	mqttClient mqtt.Client
	rateLimit  float64
	rateBurst  int
	maxConns   int
}

const (
	defaultRootTopic = "golain"
	defaultQoS       = byte(1)
	defaultRateLimit = 100  // Default operations per second
	defaultRateBurst = 200  // Default burst size
	defaultConnLimit = 1000 // Default maximum concurrent connections
)

// WithMQTTClient sets the MQTT client for the bridge
func WithMQTTClient(client mqtt.Client) BridgeOption {
	return func(cfg *BridgeConfig) {
		cfg.mqttClient = client
	}
}

// WithLogger sets the logger for the bridge
func WithLogger(logger *zap.Logger) BridgeOption {
	return func(cfg *BridgeConfig) {
		cfg.logger = logger
	}
}

// WithRootTopic sets the root topic for the bridge
func WithRootTopic(topic string) BridgeOption {
	return func(cfg *BridgeConfig) {
		cfg.rootTopic = topic
	}
}

// WithQoS sets the MQTT QoS level for the bridge
func WithQoS(qos byte) BridgeOption {
	return func(cfg *BridgeConfig) {
		cfg.qos = qos
	}
}

// WithRateLimit sets the rate limit for operations
func WithRateLimit(ops float64) BridgeOption {
	return func(cfg *BridgeConfig) {
		cfg.rateLimit = ops
	}
}

// WithRateBurst sets the burst size for rate limiting
func WithRateBurst(burst int) BridgeOption {
	return func(cfg *BridgeConfig) {
		cfg.rateBurst = burst
	}
}

// WithMaxConnections sets the maximum number of concurrent connections
func WithMaxConnections(max int) BridgeOption {
	return func(cfg *BridgeConfig) {
		cfg.maxConns = max
	}
}

// BridgeSessionState represents the current state of a bridge session
type BridgeSessionState int

const (
	BridgeSessionStateActive BridgeSessionState = iota
	BridgeSessionStateSuspended
	BridgeSessionStateClosed
)

// String returns the string representation of BridgeSessionState
func (s BridgeSessionState) String() string {
	switch s {
	case BridgeSessionStateActive:
		return "active"
	case BridgeSessionStateSuspended:
		return "suspended"
	case BridgeSessionStateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// SessionOption configures session behavior
type SessionOption func(*SessionConfig)

// SessionConfig holds session-specific configuration
type SessionConfig struct {
	SessionID string
	State     BridgeSessionState
	Timeout   time.Duration
}

// WithSessionID sets a specific session ID for connection
func WithSessionID(sessionID string) SessionOption {
	return func(cfg *SessionConfig) {
		cfg.SessionID = sessionID
	}
}

// WithSessionState sets the initial session state
func WithSessionState(state BridgeSessionState) SessionOption {
	return func(cfg *SessionConfig) {
		cfg.State = state
	}
}

func WithSessionTimeout(timeout time.Duration) SessionOption {
	return func(cfg *SessionConfig) {
		cfg.Timeout = timeout
	}
}
