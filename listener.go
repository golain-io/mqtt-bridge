package bridge

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/resolver"
)

// MQTTNetBridge implements net.Listener over MQTT
type MQTTNetBridge struct {
	mqttClient      mqtt.Client
	logger          *zap.Logger
	bridgeID        string // Our "listening address"
	clientID        string // The client ID of the bridge
	rootTopic       string
	rootTopicParts  []string
	qos             byte
	cleanUpInterval time.Duration
	// Session management
	sessionManager *SessionManager

	// Channel for new connections waiting to be accepted
	acceptCh chan *MQTTNetBridgeConn

	// Shutdown management
	ctx    context.Context
	cancel context.CancelFunc

	hooks *BridgeHooks

	proxyAddr net.Addr

	// Add mutex and closed flag for safe shutdown
	closeMu      sync.Mutex
	closed       bool
	connCleanup  sync.WaitGroup
}

// MQTTAddr implements net.Addr for MQTT connections
type MQTTAddr struct {
	network string
	address string
}

func (a *MQTTAddr) Network() string { return a.network }
func (a *MQTTAddr) String() string  { return a.address }

const (
	// Handshake topics
	handshakeRequestTopic  = "%s/bridge/%s/handshake/request/%s"  // serverID, clientID
	handshakeResponseTopic = "%s/bridge/%s/handshake/response/%s" // serverID, clientID

	// Session topics
	sessionUpTopic   = "%s/bridge/%s/session/%s/up"   // serverID, sessionID
	sessionDownTopic = "%s/bridge/%s/session/%s/down" // serverID, sessionID
)

func (b *MQTTNetBridge) Scheme() string {
	return "mqtt"
}

func (b *MQTTNetBridge) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	cc.UpdateState(resolver.State{
		Endpoints: []resolver.Endpoint{
			{
				Addresses: []resolver.Address{
					{Addr: target.URL.Host},
				},
			},
		},
	})
	return &mqttResolver{cc: cc}, nil
}

// NewMQTTNetBridge creates a new bridge that listens on a specific bridgeID
func NewMQTTNetBridge(mqttClient mqtt.Client, bridgeID string, opts ...BridgeOption) *MQTTNetBridge {
	// Apply options
	cfg := &BridgeConfig{
		rootTopic:       defaultRootTopic,
		qos:             defaultQoS,
		logger:          zap.NewNop(),
		mqttClient:      mqttClient,
		cleanUpInterval: defaultCleanUpInterval,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	ctx, cancel := context.WithCancel(context.Background())

	bridge := &MQTTNetBridge{
		mqttClient:     cfg.mqttClient,
		logger:         cfg.logger,
		bridgeID:       bridgeID,
		clientID:       uuid.New().String(),
		rootTopic:      cfg.rootTopic,
		rootTopicParts: strings.Split(cfg.rootTopic, "/"),
		qos:            cfg.qos,
		acceptCh:       make(chan *MQTTNetBridgeConn, 100),
		ctx:            ctx,
		cancel:         cancel,
		hooks:          &BridgeHooks{logger: cfg.logger},
		proxyAddr:      cfg.proxyAddr,
	}

	// Initialize session manager
	bridge.sessionManager = NewSessionManager(bridge, cfg.logger, cfg.cleanUpInterval)

	// Subscribe to handshake requests
	handshakeTopic := fmt.Sprintf(handshakeRequestTopic, bridge.rootTopic, bridge.bridgeID, "+")
	token := bridge.mqttClient.Subscribe(handshakeTopic, bridge.qos, bridge.handleHandshake)
	if token.Wait() && token.Error() != nil {
		bridge.logger.Error("Failed to subscribe to handshake topic",
			zap.String("topic", handshakeTopic),
			zap.Error(token.Error()))
		return nil
	}

	return bridge
}

// Accept implements net.Listener.Accept
func (b *MQTTNetBridge) Accept() (net.Conn, error) {
	b.closeMu.Lock()
	if b.closed {
		b.closeMu.Unlock()
		return nil, fmt.Errorf("listener closed")
	}
	b.closeMu.Unlock()

	b.logger.Debug("Waiting to accept new connection")
	select {
	case conn, ok := <-b.acceptCh:
		if !ok {
			b.logger.Info("Listener closed, no longer accepting connections")
			return nil, fmt.Errorf("listener closed")
		}
		b.logger.Info("Accepted new connection",
			zap.String("sessionID", conn.sessionID),
			zap.String("remoteAddr", conn.remoteAddr.String()))

		if b.proxyAddr != nil {
			// Try to connect to the proxy target with retries
			var proxyConn net.Conn
			var err error
			for retries := 3; retries > 0; retries-- {
				// Check if socket file exists before attempting connection
				if _, err := os.Stat(b.proxyAddr.String()); err != nil {
					b.logger.Debug("Socket file not found, retrying",
						zap.String("address", b.proxyAddr.String()),
						zap.Int("retries_left", retries-1),
						zap.Error(err))
					time.Sleep(100 * time.Millisecond)
					continue
				}

				proxyConn, err = net.Dial(b.proxyAddr.Network(), b.proxyAddr.String())
				if err == nil {
					break
				}
				b.logger.Debug("Failed to connect to proxy target, retrying",
					zap.String("address", b.proxyAddr.String()),
					zap.Int("retries_left", retries-1),
					zap.Error(err))
				time.Sleep(100 * time.Millisecond)
			}
			if err != nil {
				conn.Close()
				return nil, fmt.Errorf("failed to connect to proxy target after retries: %v", err)
			}

			b.logger.Debug("Connected to proxy target",
				zap.String("network", b.proxyAddr.Network()),
				zap.String("address", b.proxyAddr.String()))

			go b.proxyConn(proxyConn, conn)

			return conn, nil
		}

		return conn, nil
	case <-b.ctx.Done():
		return nil, b.ctx.Err()
	}
}

// Close implements net.Listener.Close
func (b *MQTTNetBridge) Close() error {
	b.closeMu.Lock()
	if b.closed {
		b.closeMu.Unlock()
		return nil
	}
	b.closed = true
	b.closeMu.Unlock()

	b.logger.Info("Closing MQTT bridge", zap.String("bridgeID", b.bridgeID))
	b.cancel() // Cancel the context

	// Wait for in-flight conn.Close cleanup before suspending sessions.
	b.connCleanup.Wait()

	for id, clientID := range b.sessionManager.ActiveSessionClientIDs() {
		if err := b.sessionManager.SuspendSession(id, clientID); err != nil {
			b.logger.Error("Failed to suspend session during shutdown",
				zap.String("sessionID", id),
				zap.Error(err))
		}
	}

	// Unsubscribe from handshake topic
	handshakeTopic := fmt.Sprintf(handshakeRequestTopic, b.rootTopic, b.bridgeID, "+")
	token := b.mqttClient.Unsubscribe(handshakeTopic)
	token.Wait()

	close(b.acceptCh)
	return nil
}

// Addr implements net.Listener.Addr
func (b *MQTTNetBridge) Addr() net.Addr {
	return &MQTTAddr{
		network: "mqtt",
		address: b.bridgeID,
	}
}

// AddHook adds a new hook to the bridge
func (b *MQTTNetBridge) AddHook(hook BridgeHook, config any) error {
	if b.hooks == nil {
		b.hooks = &BridgeHooks{
			logger: b.logger,
		}
	}

	b.logger.Info("Adding hook to bridge",
		zap.String("hook", hook.ID()),
		zap.String("bridgeID", b.bridgeID))

	// First add the hook
	if err := b.hooks.Add(hook, config); err != nil {
		b.logger.Error("Failed while adding hook",
			zap.String("hook", hook.ID()),
			zap.Error(err))
		return err
	}

	// Then check if it implements SessionStore
	if store, ok := hook.(ISessionStore); ok {
		b.logger.Info("Hook implements SessionStore, updating session manager",
			zap.String("hook", hook.ID()))

		if err := b.sessionManager.UpdateStore(store); err != nil {
			b.logger.Error("Failed to update session store",
				zap.String("hook", hook.ID()),
				zap.Error(err))
			// Continue even if store update fails since the hook is already added
		}
	}

	return nil
}

// CleanupStaleSessions removes sessions that have been suspended longer than the timeout
func (b *MQTTNetBridge) CleanupStaleSessions() {
	b.sessionManager.CleanupStaleSessions()
}

// DisconnectSession disconnects an active session and cleans it up
func (b *MQTTNetBridge) DisconnectSession(sessionID string) error {
	session, exists := b.sessionManager.GetSession(sessionID)
	if !exists {
		return NewSessionNotFoundError("disconnect", sessionID)
	}

	if session.Connection != nil {
		// Send disconnect request
		requestTopic := fmt.Sprintf(handshakeRequestTopic, b.rootTopic, session.Connection.remoteAddr.String(), b.clientID)
		msg := fmt.Sprintf("%s:%s", disconnectMsg, sessionID)
		token := b.mqttClient.Publish(requestTopic, b.qos, false, []byte(msg))
		if token.Wait() && token.Error() != nil {
			return NewBridgeError("disconnect", "disconnect request failed", token.Error())
		}
	}

	return b.sessionManager.DisconnectSession(sessionID)
}
