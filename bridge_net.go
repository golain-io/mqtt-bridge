package bridge

import (
	"context"
	"fmt"
	"io"
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
	mqttClient     mqtt.Client
	logger         *zap.Logger
	bridgeID       string // Our "listening address"
	rootTopic      string
	rootTopicParts []string
	qos            byte

	// Session management
	sessions   map[string]*SessionInfo
	sessionsMu sync.RWMutex

	sessionSuspendedChanMap   map[string]chan struct{}
	sessionSuspendedChanMapMu sync.RWMutex

	sessionResumeChanMap   map[string]chan struct{}
	sessionResumeChanMapMu sync.RWMutex

	sessionErrorChanMap   map[string]chan struct{}
	sessionErrorChanMapMu sync.RWMutex

	// Channel for new connections waiting to be accepted
	acceptCh chan *MQTTNetBridgeConn

	// Shutdown management
	ctx    context.Context
	cancel context.CancelFunc

	hooks *BridgeHooks
}

// SessionInfo tracks session state and metadata
type SessionInfo struct {
	ID            string
	ClientID      string // Current/last client that owns this session
	State         BridgeSessionState
	LastActive    time.Time
	LastSuspended time.Time
	Connection    *MQTTNetBridgeConn
	Metadata      map[string]string
}

// MQTTAddr implements net.Addr for MQTT connections
type MQTTAddr struct {
	network string
	address string
}

func (a *MQTTAddr) Network() string { return a.network }
func (a *MQTTAddr) String() string  { return a.address }

// Update constants for topic patterns
const (
	// Handshake topics
	handshakeRequestTopic  = "%s/bridge/%s/handshake/request/%s"  // serverID, clientID
	handshakeResponseTopic = "%s/bridge/%s/handshake/response/%s" // serverID, clientID

	// Session topics
	sessionUpTopic   = "%s/bridge/%s/session/%s/up"   // serverID, sessionID
	sessionDownTopic = "%s/bridge/%s/session/%s/down" // serverID, sessionID

	// Message types
	connectMsg       = "connect"
	connectAckMsg    = "connect_ack"
	suspendMsg       = "suspend"
	suspendAckMsg    = "suspend_ack"
	resumeMsg        = "resume"
	resumeAckMsg     = "resume_ack"
	errorMsg         = "error"
	disconnectMsg    = "disconnect"
	disconnectAckMsg = "disconnect_ack"

	// Error types
	errSessionActive    = "session_active"
	errSessionNotFound  = "session_not_found"
	errInvalidSession   = "invalid_session"
	errSessionSuspended = "session_suspended"

	// Session management
	defaultSessionTimeout = 30 * time.Minute // Default timeout for suspended sessions

	// Add new constant for disconnect timeout
	defaultDisconnectTimeout = 2 * time.Second // Time to wait before cleaning up disconnected sessions

	// Add new constant for disconnect acknowledgment
)

type mqttResolver struct {
	cc resolver.ClientConn
}

func (r *mqttResolver) ResolveNow(resolver.ResolveNowOptions) {}
func (r *mqttResolver) Close()                                {}

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
		rootTopic:  defaultRootTopic,
		qos:        defaultQoS,
		logger:     zap.NewNop(),
		mqttClient: mqttClient,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	ctx, cancel := context.WithCancel(context.Background())

	bridge := &MQTTNetBridge{
		mqttClient:              cfg.mqttClient,
		logger:                  cfg.logger,
		bridgeID:                bridgeID,
		rootTopic:               cfg.rootTopic,
		rootTopicParts:          strings.Split(cfg.rootTopic, "/"),
		qos:                     cfg.qos,
		sessions:                make(map[string]*SessionInfo),
		acceptCh:                make(chan *MQTTNetBridgeConn, 100),
		ctx:                     ctx,
		cancel:                  cancel,
		hooks:                   &BridgeHooks{logger: cfg.logger},
		sessionSuspendedChanMap: make(map[string]chan struct{}),
		sessionResumeChanMap:    make(map[string]chan struct{}),
		sessionErrorChanMap:     make(map[string]chan struct{}),
	}

	// Subscribe to handshake requests
	handshakeTopic := fmt.Sprintf(handshakeRequestTopic, bridge.rootTopic, bridge.bridgeID, "+")
	token := bridge.mqttClient.Subscribe(handshakeTopic, bridge.qos, bridge.handleHandshake)
	if token.Wait() && token.Error() != nil {
		bridge.logger.Error("Failed to subscribe to handshake topic",
			zap.String("topic", handshakeTopic),
			zap.Error(token.Error()))
		return nil
	}

	bridge.startCleanupTask(5*time.Minute, defaultSessionTimeout)

	return bridge
}

// Accept implements net.Listener.Accept
func (b *MQTTNetBridge) Accept() (net.Conn, error) {
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
		return conn, nil
	case <-b.ctx.Done():
		return nil, b.ctx.Err()
	}
}

// Close implements net.Listener.Close
func (b *MQTTNetBridge) Close() error {
	b.logger.Info("Closing MQTT bridge", zap.String("bridgeID", b.bridgeID))
	b.cancel() // Cancel the context

	// Close all existing sessions
	for _, session := range b.sessions {
		if session.Connection != nil {
			session.Connection.Close()
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

// MQTTNetBridgeConn implements net.Conn over MQTT
type MQTTNetBridgeConn struct {
	ctx    context.Context
	cancel context.CancelFunc

	bridge     *MQTTNetBridge
	localAddr  net.Addr
	remoteAddr net.Addr
	sessionID  string

	// Read buffer management
	readBuf    chan []byte
	readBuffer []byte // Holds partially read data
	readMu     sync.Mutex
	readErr    error
	deadline   time.Time

	// Write management
	writeMu   sync.Mutex
	writeErr  error
	wDeadline time.Time

	// Connection state
	closed  bool
	closeMu sync.RWMutex

	// Add topics
	upTopic   string
	downTopic string
	connected bool
	connMu    sync.RWMutex
	role      string // "client" or "server"

	respChan chan struct {
		payload []byte
		topic   string
	}
}

func (c *MQTTNetBridgeConn) SessionID() string {
	return c.sessionID
}

// This is for client side.
func (c *MQTTNetBridgeConn) handleLifecycleHandshake() {
	for {
		select {
		case resp := <-c.respChan:
			payload := resp.payload
			topic := resp.topic
			topicParts := strings.Split(topic, "/")
			// ignore root topic (substract length of root topic)
			topicParts = topicParts[len(c.bridge.rootTopicParts):]
			if len(topicParts) != 5 {
				c.bridge.logger.Error("Invalid handshake response format",
					zap.String("topic", topic),
					zap.String("payload", string(payload)))
			}
			b := c.bridge
			msgParts := strings.Split(UnsafeString(payload), ":")
			msgType := msgParts[0]
			switch msgType {
			case suspendAckMsg:
				if len(msgParts) < 2 {
					b.logger.Error("Invalid handshake response format",
						zap.String("topic", topic),
						zap.String("payload", string(payload)))
				}
				sessionID := msgParts[1]
				b.sessionSuspendedChanMapMu.RLock()
				sessionSuspendedChan, exists := b.sessionSuspendedChanMap[sessionID]
				if !exists {
					b.sessionSuspendedChanMapMu.RUnlock()
					b.logger.Error("Session not found",
						zap.String("sessionID", sessionID))
					continue
				}
				b.sessionSuspendedChanMapMu.RUnlock()
				sessionSuspendedChan <- struct{}{}
			case resumeAckMsg:
				if len(msgParts) < 2 {
					b.logger.Error("Invalid handshake response format",
						zap.String("topic", topic),
						zap.String("payload", string(payload)))
				}
				sessionID := msgParts[1]
				b.sessionResumeChanMapMu.RLock()
				sessionResumeChan, exists := b.sessionResumeChanMap[sessionID]
				if !exists {
					b.sessionResumeChanMapMu.RUnlock()
					b.logger.Error("Session not found",
						zap.String("sessionID", sessionID))
					continue
				}
				b.sessionResumeChanMapMu.RUnlock()
				sessionResumeChan <- struct{}{}

			case disconnectAckMsg:

				if len(msgParts) < 2 {
					b.logger.Error("Invalid disconnect ack format",
						zap.String("topic", topic),
						zap.String("payload", string(payload)))
					continue
				}
				sessionID := msgParts[1]
				b.logger.Info("Received disconnect ack",
					zap.String("sessionID", sessionID))
				b.sessionErrorChanMapMu.RLock()
				sessionErrorChan, exists := b.sessionErrorChanMap[c.sessionID]
				if exists {
					sessionErrorChan <- struct{}{}
				}
				b.sessionErrorChanMapMu.RUnlock()

			case errorMsg:
				if len(msgParts) < 2 {
					b.logger.Error("Invalid error handshake response format",
						zap.String("topic", topic),
						zap.String("payload", string(payload)))
					continue
				}
				b.logger.Error("Received error message",
					zap.String("sessionID", c.sessionID),
					zap.String("error", string(payload)))
			}
		case <-c.ctx.Done():
			return
		}
	}
}

// Implement net.Conn interface stubs (we'll flesh these out next)
func (c *MQTTNetBridgeConn) Read(b []byte) (n int, err error) {
	c.connMu.RLock()
	if !c.connected {
		c.connMu.RUnlock()
		return 0, fmt.Errorf("connection not established")
	}
	c.connMu.RUnlock()

	c.readMu.Lock()
	deadline := c.deadline
	c.readMu.Unlock()

	var timer *time.Timer
	var timeout <-chan time.Time

	if !deadline.IsZero() {
		if time.Now().After(deadline) {
			return 0, os.ErrDeadlineExceeded
		}
		timer = time.NewTimer(time.Until(deadline))
		timeout = timer.C
		defer timer.Stop()
	}

	select {
	case data, ok := <-c.readBuf:
		if !ok {
			return 0, io.EOF
		}
		n = copy(b, data)
		return n, nil
	case <-timeout:
		return 0, os.ErrDeadlineExceeded
	case <-c.bridge.ctx.Done():
		return 0, c.bridge.ctx.Err()
	}
}

func (c *MQTTNetBridgeConn) Write(b []byte) (n int, err error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if c.closed {
		return 0, fmt.Errorf("connection closed")
	}

	// Determine which topic to use based on role
	topic := c.upTopic
	if c.role == "server" {
		topic = c.downTopic // Server writes to down topic
	}

	token := c.bridge.mqttClient.Publish(topic, c.bridge.qos, false, b)
	c.bridge.logger.Debug("Writing data",
		zap.String("sessionID", c.sessionID),
		zap.Int("bytes", len(b)),
		zap.String("topic", topic))

	if !token.WaitTimeout(5 * time.Second) {
		return 0, fmt.Errorf("write timeout")
	}
	if token.Error() != nil {
		return 0, token.Error()
	}

	return len(b), nil
}

func (c *MQTTNetBridgeConn) Close() error {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return nil
	}
	c.closed = true
	c.closeMu.Unlock()

	// Only call DisconnectSession if we're not already being closed by it
	select {
	case <-c.ctx.Done():
		// Context already cancelled, we're being closed by DisconnectSession
		return nil
	default:
		// We're initiating the close, call DisconnectSession
		if err := c.bridge.DisconnectSession(c.sessionID); err != nil {
			c.bridge.logger.Error("Failed to disconnect session during close",
				zap.String("sessionID", c.sessionID),
				zap.Error(err))
		}
	}

	c.cancel()
	return nil
}

func (c *MQTTNetBridgeConn) LocalAddr() net.Addr {
	return c.localAddr
}

func (c *MQTTNetBridgeConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *MQTTNetBridgeConn) SetDeadline(t time.Time) error {
	c.readMu.Lock()
	c.writeMu.Lock()
	defer c.readMu.Unlock()
	defer c.writeMu.Unlock()

	c.deadline = t
	c.wDeadline = t
	return nil
}

func (c *MQTTNetBridgeConn) SetReadDeadline(t time.Time) error {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	c.deadline = t
	return nil
}

func (c *MQTTNetBridgeConn) SetWriteDeadline(t time.Time) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.wDeadline = t
	return nil
}

// handleDisconnect processes a disconnect message for a session
func (b *MQTTNetBridge) handleDisconnect(sessionID string) {
	b.logger.Debug("Handling session disconnect",
		zap.String("sessionID", sessionID))

	b.sessionsMu.Lock()
	defer b.sessionsMu.Unlock()

	session, exists := b.sessions[sessionID]
	if !exists {
		b.logger.Debug("No session found for disconnect",
			zap.String("sessionID", sessionID))
		return
	}

	b.logger.Debug("Found session for disconnect",
		zap.String("sessionID", sessionID),
		zap.String("currentState", session.State.String()))

	// Mark as suspended and update timestamp
	session.State = BridgeSessionStateSuspended
	session.LastSuspended = time.Now()

	// Close connection if it exists
	if session.Connection != nil {
		session.Connection.closed = true
		close(session.Connection.readBuf)
		session.Connection = nil
	}

	b.logger.Debug("Marked session as suspended",
		zap.String("sessionID", sessionID))

	// Start cleanup timer
	go b.startSessionCleanupTimer(sessionID)
}

// startSessionCleanupTimer starts a timer to clean up a suspended session
func (b *MQTTNetBridge) startSessionCleanupTimer(sessionID string) {
	b.logger.Debug("Starting cleanup timer",
		zap.String("sessionID", sessionID),
		zap.Duration("timeout", defaultDisconnectTimeout))

	time.Sleep(defaultDisconnectTimeout)

	b.sessionsMu.Lock()
	defer b.sessionsMu.Unlock()

	session, exists := b.sessions[sessionID]
	if !exists {
		return
	}

	// Only clean up if still suspended and timeout has elapsed
	if session.State == BridgeSessionStateSuspended &&
		time.Since(session.LastSuspended) >= defaultDisconnectTimeout {
		delete(b.sessions, sessionID)
		b.logger.Debug("Cleaned up suspended session after timeout",
			zap.String("sessionID", sessionID))
	}
}

// handleIncomingData processes incoming MQTT messages
func (b *MQTTNetBridge) handleIncomingData(client mqtt.Client, msg mqtt.Message) {
	payload := b.hooks.OnMessageReceived(msg.Payload())

	b.logger.Debug("Received incoming data",
		zap.String("topic", msg.Topic()),
		zap.Int("bytes", len(payload)),
		zap.String("payload", string(payload)))

	parts := strings.Split(msg.Topic(), "/")
	if len(parts) < 6 {
		b.logger.Error("Invalid topic format", zap.String("topic", msg.Topic()))
		return
	}

	sessionID := parts[len(parts)-2]

	// Handle regular data messages
	b.sessionsMu.RLock()
	session, exists := b.sessions[sessionID]
	b.sessionsMu.RUnlock()

	if !exists || session.Connection == nil || session.Connection.closed {
		b.logger.Debug("No active session/connection",
			zap.String("sessionID", sessionID))
		return
	}

	select {
	case session.Connection.readBuf <- payload:
		b.logger.Debug("Forwarded data to connection",
			zap.String("sessionID", sessionID),
			zap.Int("bytes", len(payload)))
	default:
		b.logger.Warn("Read buffer full, dropping message",
			zap.String("session", sessionID))
	}
}

// createNewConnection creates a new server-side connection
func (b *MQTTNetBridge) createNewConnection(sessionID string) *MQTTNetBridgeConn {
	ctx, cancel := context.WithCancel(b.ctx)
	conn := &MQTTNetBridgeConn{
		ctx:        ctx,
		cancel:     cancel,
		bridge:     b,
		sessionID:  sessionID,
		readBuf:    make(chan []byte, 100),
		localAddr:  b.Addr(),
		remoteAddr: &MQTTAddr{network: "mqtt", address: sessionID},
		upTopic:    fmt.Sprintf(sessionUpTopic, b.rootTopic, b.bridgeID, sessionID),
		downTopic:  fmt.Sprintf(sessionDownTopic, b.rootTopic, b.bridgeID, sessionID),
		role:       "server",
	}

	// Add mutex initialization if not already present
	conn.connMu = sync.RWMutex{}

	// Subscribe to session up topic for server with QoS 1 to ensure delivery
	token := b.mqttClient.Subscribe(conn.upTopic, b.qos, func(client mqtt.Client, msg mqtt.Message) {
		conn.connMu.RLock()
		if conn.closed {
			conn.connMu.RUnlock()
			return
		}
		conn.connMu.RUnlock()

		payload := b.hooks.OnMessageReceived(msg.Payload())

		// Check for disconnect message on server side
		msgStr := string(payload)
		if strings.HasPrefix(msgStr, disconnectMsg+":") {
			msgParts := strings.Split(msgStr, ":")
			if len(msgParts) == 2 && msgParts[1] == sessionID {
				b.handleDisconnect(sessionID)
				return
			}
		}

		// Check if channel is still open before sending
		conn.connMu.RLock()
		if !conn.closed && conn.readBuf != nil {
			select {
			case conn.readBuf <- payload:
				b.logger.Debug("Server forwarded data to connection",
					zap.String("sessionID", sessionID),
					zap.Int("bytes", len(payload)))
			default:
				b.logger.Warn("Server read buffer full, dropping message",
					zap.String("session", sessionID))
			}
		}
		conn.connMu.RUnlock()
	})

	if token.Wait() && token.Error() != nil {
		b.logger.Error("Failed to subscribe to session topic",
			zap.String("topic", conn.upTopic),
			zap.Error(token.Error()))
		return nil
	}

	return conn
}

// Dial creates a new connection to a specific bridge
func (b *MQTTNetBridge) Dial(ctx context.Context, targetBridgeID string, opts ...SessionOption) (net.Conn, error) {
	// Parse session options
	cfg := &SessionConfig{
		State: BridgeSessionStateActive,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	sessionID := cfg.SessionID

	clientID := uuid.New().String()
	b.logger.Info("Initiating connection",
		zap.String("targetBridgeID", targetBridgeID),
		zap.String("clientID", clientID))

	// Subscribe to handshake response
	responseTopic := fmt.Sprintf(handshakeResponseTopic, b.rootTopic, targetBridgeID, clientID)
	respChan := make(chan struct {
		payload []byte
		topic   string
	}, 1)

	token := b.mqttClient.Subscribe(responseTopic, b.qos, func(_ mqtt.Client, msg mqtt.Message) {
		payload := b.hooks.OnMessageReceived(msg.Payload())
		select {
		case respChan <- struct {
			payload []byte
			topic   string
		}{payload: payload, topic: msg.Topic()}:
		default:
			b.logger.Warn("Response channel full")
		}
	})
	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("handshake subscribe failed: %v", token.Error())
	}

	// Send connect request
	requestTopic := fmt.Sprintf(handshakeRequestTopic, b.rootTopic, targetBridgeID, clientID)
	var msg string
	if sessionID != "" {
		// If we have a sessionID, we're resuming
		msg = fmt.Sprintf("resume:%s", sessionID)
	} else {
		// Otherwise, it's a new connection
		msg = connectMsg
	}

	token = b.mqttClient.Publish(requestTopic, b.qos, false, []byte(msg))
	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("handshake request failed: %v", token.Error())
	}

	// Wait for connect_ack
	select {
	case resp := <-respChan:
		payload := resp.payload
		topic := resp.topic
		topicParts := strings.Split(topic, "/")
		// ignore root topic (substract length of root topic)
		topicParts = topicParts[len(b.rootTopicParts):]
		if len(topicParts) != 5 {
			return nil, fmt.Errorf("invalid handshake response format")
		}

		msgParts := strings.Split(UnsafeString(payload), ":")
		msgType := msgParts[0]
		switch msgType {
		case connectAckMsg:
			sessionID = msgParts[1]
			upTopic := msgParts[2]
			downTopic := msgParts[3]

			b.logger.Debug("Received connection acknowledgment",
				zap.String("sessionID", sessionID),
				zap.String("upTopic", upTopic),
				zap.String("downTopic", downTopic))

			connCtx, cancel := context.WithCancel(b.ctx)
			// Create client connection
			conn := &MQTTNetBridgeConn{
				ctx:        connCtx,
				cancel:     cancel,
				bridge:     b,
				sessionID:  sessionID,
				readBuf:    make(chan []byte, 100),
				localAddr:  b.Addr(),
				remoteAddr: &MQTTAddr{network: "mqtt", address: targetBridgeID},
				upTopic:    upTopic,
				downTopic:  downTopic,
				role:       "client",
				respChan:   respChan,
			}

			// Store connection
			b.sessionsMu.Lock()
			b.sessions[sessionID] = &SessionInfo{
				ID:         sessionID,
				State:      BridgeSessionStateActive,
				LastActive: time.Now(),
				Connection: conn,
				Metadata:   make(map[string]string),
				ClientID:   clientID,
			}
			b.sessionsMu.Unlock()

			// Subscribe to session messages
			token = b.mqttClient.Subscribe(conn.downTopic, b.qos, b.handleIncomingData)
			if token.Wait() && token.Error() != nil {
				conn.Close()
				return nil, fmt.Errorf("session subscribe failed: %v", token.Error())
			}
			b.logger.Debug("Subscribed to session down topic",
				zap.String("topic", conn.downTopic))

			conn.connMu.Lock()
			conn.connected = true
			conn.connMu.Unlock()

			// After successful handshake, update session info
			session := b.sessions[sessionID]
			session.State = BridgeSessionStateActive
			session.LastActive = time.Now()
			session.Connection = conn

			go conn.handleLifecycleHandshake()

			return conn, nil
		default:
			return nil, fmt.Errorf("unexpected message type: %s", msgType)
		}

	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("handshake timeout")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// SuspendSession suspends an active session for later resumption
func (b *MQTTNetBridge) SuspendSession(sessionID string) error {
	// First, verify session exists and get the client ID
	b.sessionsMu.RLock()
	session, exists := b.sessions[sessionID]
	if !exists {
		b.sessionsMu.RUnlock()
		return fmt.Errorf("session %s not found", sessionID)
	}

	if session.State != BridgeSessionStateActive {
		b.sessionsMu.RUnlock()
		return fmt.Errorf("session %s is not active", sessionID)
	}

	clientID := session.ClientID
	remoteAddr := session.Connection.remoteAddr.String()
	b.sessionsMu.RUnlock()

	// Send suspend request to server
	b.logger.Info("Sending suspend request to server",
		zap.String("sessionID", sessionID),
		zap.String("clientID", clientID))

	sessionSuspendedChan := make(chan struct{})
	b.sessionSuspendedChanMapMu.Lock()
	b.sessionSuspendedChanMap[sessionID] = sessionSuspendedChan
	b.sessionSuspendedChanMapMu.Unlock()

	// Send suspend request
	requestTopic := fmt.Sprintf(handshakeRequestTopic, b.rootTopic, remoteAddr, clientID)
	msg := fmt.Sprintf("%s:%s", suspendMsg, sessionID)
	token := b.mqttClient.Publish(requestTopic, b.qos, false, []byte(msg))
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("suspend request failed: %v", token.Error())
	}

	// Wait for suspend_ack
	select {
	case <-sessionSuspendedChan:
		b.sessionSuspendedChanMapMu.Lock()
		delete(b.sessionSuspendedChanMap, sessionID)
		b.sessionSuspendedChanMapMu.Unlock()

		session.State = BridgeSessionStateSuspended
		session.LastSuspended = time.Now()

		session.Connection.Close()

		return nil

	case <-time.After(5 * time.Second):
		return fmt.Errorf("suspend timeout")
	}
}

// ResumeSession attempts to resume a suspended session that may still exist on the server bridge (not cleaned up yet)
func (b *MQTTNetBridge) ResumeSession(ctx context.Context, targetBridgeID, sessionID string) (net.Conn, error) {
	return b.Dial(ctx, targetBridgeID, WithSessionID(sessionID), WithSessionState(BridgeSessionStateActive))
}

// listens for connections on a unix socket and proxies them to the bridge
func (b *MQTTNetBridge) ListenOnUnixSocket(path string, addr string) error {
	// Remove existing socket file if it exists
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove existing socket: %v", err)
	}

	// listen for connections on the socket
	listener, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	defer listener.Close()

	// accept connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		// proxy the connection to the bridge
		bConn, err := b.Dial(b.ctx, addr)
		if err != nil {
			return err
		}
		b.proxyConn(conn, bConn)
	}
}

func (b *MQTTNetBridge) WriteOnUnixSocket(path string, addr string) (net.Conn, error) {
	// Remove existing socket file if it exists
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to remove existing socket: %v", err)
	}

	// Create the Unix socket
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("failed to create unix socket: %v", err)
	}
	defer listener.Close()

	bConn, err := b.Accept()
	if err != nil {
		return nil, err
	}

	// Wait for a client to connect to our Unix socket
	conn, err := listener.Accept()
	if err != nil {
		bConn.Close()
		return nil, fmt.Errorf("failed to accept unix connection: %v", err)
	}

	go b.proxyConn(conn, bConn)

	return bConn, nil
}

func (b *MQTTNetBridge) proxyConn(conn net.Conn, bConn net.Conn) {
	go func() {
		for {
			if bConn == nil || conn == nil {
				break
			}
			_, err := io.Copy(bConn, conn)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.logger.Error("Error copying data from client to bridge", zap.Error(err))
				bConn.Close()
				conn.Close()
				break
			}
		}
	}()
	go func() {
		for {
			if bConn == nil || conn == nil {
				break
			}
			_, err := io.Copy(conn, bConn)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.logger.Error("Error copying data from bridge to client", zap.Error(err))
				bConn.Close()
				conn.Close()
				break
			}
		}
	}()
}

// handleHandshake processes incoming handshake messages
func (b *MQTTNetBridge) handleHandshake(client mqtt.Client, msg mqtt.Message) {
	payload := msg.Payload()
	payload = b.hooks.OnMessageReceived(payload)

	b.logger.Debug("Received handshake message",
		zap.String("topic", msg.Topic()))

	parts := strings.Split(msg.Topic(), "/")
	if len(parts) < 6 {
		b.logger.Error("Invalid handshake topic format",
			zap.String("topic", msg.Topic()),
			zap.Int("parts", len(parts)))
		return
	}

	msgParts := strings.Split(UnsafeString(payload), ":")
	msgType := msgParts[0]
	clientID := parts[len(parts)-1]

	b.logger.Debug("Parsed handshake request",
		zap.String("msgType", msgType),
		zap.String("clientID", clientID))

	if parts[len(parts)-2] != "request" {
		b.logger.Warn("Unexpected handshake message",
			zap.String("msgType", msgType),
			zap.String("requestType", parts[len(parts)-2]))
		return
	}

	responseTopic := fmt.Sprintf(handshakeResponseTopic, b.rootTopic, b.bridgeID, clientID)

	switch msgType {
	case connectMsg:
		// Handle new connection
		sessionID := uuid.New().String()
		conn := b.createNewConnection(sessionID)
		if conn == nil {
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, "failed_to_create_connection")))
			return
		}

		b.handleNewConnection(conn, clientID, responseTopic)

	case resumeMsg:
		if len(msgParts) < 2 {
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errInvalidSession)))
			return
		}

		sessionID := msgParts[1]
		b.sessionsMu.Lock()
		session, exists := b.sessions[sessionID]
		if !exists {
			b.sessionsMu.Unlock()
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errSessionNotFound)))
			return
		}

		if session.State == BridgeSessionStateActive {
			// Add clientID info to error logging
			b.logger.Warn("Attempt to resume active session",
				zap.String("sessionID", sessionID),
				zap.String("currentClientID", session.ClientID),
				zap.String("requestingClientID", clientID))
			b.sessionsMu.Unlock()
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errSessionActive)))
			return
		}

		if session.State != BridgeSessionStateSuspended {
			b.sessionsMu.Unlock()
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errSessionSuspended)))
			return
		}

		// Update clientID for resumed session
		session.ClientID = clientID
		b.sessionsMu.Unlock()

		conn := b.createNewConnection(sessionID)
		if conn == nil {
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, "failed_to_create_connection")))
			return
		}

		b.handleNewConnection(conn, clientID, responseTopic)

	case suspendMsg:
		if len(msgParts) < 2 {
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errInvalidSession)))
			return
		}

		sessionID := msgParts[1]
		b.sessionsMu.Lock()
		session, exists := b.sessions[sessionID]
		if !exists {
			b.sessionsMu.Unlock()
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errSessionNotFound)))
			return
		}

		// Verify the client owns this session
		if session.ClientID != clientID {
			b.logger.Warn("Unauthorized suspend attempt",
				zap.String("sessionID", sessionID),
				zap.String("sessionClientID", session.ClientID),
				zap.String("requestingClientID", clientID))
			b.sessionsMu.Unlock()
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, "unauthorized")))
			return
		}

		if session.State != BridgeSessionStateActive {
			b.sessionsMu.Unlock()
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errSessionActive)))
			return
		}

		b.logger.Info("Suspending session",
			zap.String("sessionID", sessionID),
			zap.String("clientID", clientID))

		b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", suspendAckMsg, sessionID)))

		session.State = BridgeSessionStateSuspended
		session.LastSuspended = time.Now()

		// Close the connection but keep session info
		if session.Connection != nil {
			b.sessionsMu.Unlock()
			session.Connection.Close()

			b.sessionsMu.Lock()
			session.Connection = nil
		}
		b.sessionsMu.Unlock()

	case disconnectMsg:
		if len(msgParts) < 2 {
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errInvalidSession)))
			return
		}

		sessionID := msgParts[1]
		b.sessionsMu.Lock()
		session, exists := b.sessions[sessionID]
		if !exists {
			b.sessionsMu.Unlock()
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errSessionNotFound)))
			return
		}

		// Verify the client owns this session
		if session.ClientID != clientID {
			b.logger.Warn("Unauthorized disconnect attempt",
				zap.String("sessionID", sessionID),
				zap.String("sessionClientID", session.ClientID),
				zap.String("requestingClientID", clientID))
			b.sessionsMu.Unlock()
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, "unauthorized")))
			return
		}

		if session.State != BridgeSessionStateActive {
			b.sessionsMu.Unlock()
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errSessionActive)))
			return
		}

		b.logger.Info("Disconnecting session",
			zap.String("sessionID", sessionID),
			zap.String("clientID", clientID))

		// Send disconnect acknowledgment instead of error message
		b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", disconnectAckMsg, sessionID)))

		// Mark session as suspended instead of deleting it
		session.State = BridgeSessionStateSuspended
		b.sessionsMu.Unlock()

		b.handleDisconnect(sessionID)

	}
}

func (b *MQTTNetBridge) handleNewConnection(conn *MQTTNetBridgeConn, clientID, responseTopic string) {
	// Send connection acknowledgment with session details
	response := fmt.Sprintf("%s:%s:%s:%s", connectAckMsg, conn.sessionID, conn.upTopic, conn.downTopic)

	b.logger.Debug("Sending connect_ack",
		zap.String("topic", responseTopic),
		zap.String("response", response))

	token := b.mqttClient.Publish(responseTopic, b.qos, false, []byte(response))
	if token.Wait() && token.Error() != nil {
		b.logger.Error("Failed to send connect_ack",
			zap.String("clientID", clientID),
			zap.Error(token.Error()))
		conn.Close()
		return
	}

	// Create or update session info
	b.sessionsMu.Lock()
	session := &SessionInfo{
		ID:         conn.sessionID,
		ClientID:   clientID,
		State:      BridgeSessionStateActive,
		LastActive: time.Now(),
		Connection: conn,
		Metadata:   make(map[string]string),
	}
	b.sessions[conn.sessionID] = session
	b.sessionsMu.Unlock()

	// Queue for Accept after successful ack
	select {
	case b.acceptCh <- conn:
		conn.connMu.Lock()
		conn.connected = true
		conn.connMu.Unlock()
		b.logger.Info("Connection established",
			zap.String("sessionID", conn.sessionID),
			zap.String("clientID", clientID))
	default:
		b.logger.Warn("Accept channel full, dropping connection",
			zap.Int("total_connections", len(b.sessions)),
			zap.Int("accept_channel_size", len(b.acceptCh)),
			zap.String("sessionID", conn.sessionID))
		conn.Close()
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

	return b.hooks.Add(hook, config)
}

// CleanupStaleSessions removes sessions that have been suspended for longer than the timeout
func (b *MQTTNetBridge) CleanupStaleSessions(timeout time.Duration) {
	if timeout == 0 {
		timeout = defaultSessionTimeout
	}

	b.sessionsMu.Lock()
	defer b.sessionsMu.Unlock()

	now := time.Now()
	for id, session := range b.sessions {
		// Clean up sessions that are:
		// 1. In suspended state and have been suspended longer than the timeout
		// 2. In closed state (shouldn't exist, but clean them up if they do)
		if (session.State == BridgeSessionStateSuspended && now.Sub(session.LastSuspended) > timeout) ||
			session.State == BridgeSessionStateClosed {
			delete(b.sessions, id)
			b.logger.Debug("Cleaned up stale session",
				zap.String("sessionID", id),
				zap.String("state", session.State.String()),
				zap.Duration("age", now.Sub(session.LastSuspended)))
		}
	}
}

// StartCleanupTask starts a periodic cleanup of stale sessions
func (b *MQTTNetBridge) startCleanupTask(interval, timeout time.Duration) {
	if interval == 0 {
		interval = 5 * time.Minute // Default to running cleanup every minute
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				b.CleanupStaleSessions(timeout)
			case <-b.ctx.Done():
				return
			}
		}
	}()
}

// DisconnectSession disconnects an active session and cleans it up
func (b *MQTTNetBridge) DisconnectSession(sessionID string) error {
	// First, verify session exists and get the client ID
	b.sessionsMu.RLock()
	session, exists := b.sessions[sessionID]
	if !exists {
		b.sessionsMu.RUnlock()
		return fmt.Errorf("session %s not found", sessionID)
	}

	if session.State != BridgeSessionStateActive {
		b.sessionsMu.RUnlock()
		return fmt.Errorf("session %s is not active", sessionID)
	}

	// Get the info we need and release the read lock
	clientID := session.ClientID
	remoteAddr := session.Connection.remoteAddr.String()
	conn := session.Connection
	b.sessionsMu.RUnlock()

	// Send disconnect request to server
	b.logger.Info("Sending disconnect request to server",
		zap.String("sessionID", sessionID),
		zap.String("clientID", clientID))

	// Send disconnect request
	requestTopic := fmt.Sprintf(handshakeRequestTopic, b.rootTopic, remoteAddr, clientID)
	msg := fmt.Sprintf("%s:%s", disconnectMsg, sessionID)
	token := b.mqttClient.Publish(requestTopic, b.qos, false, []byte(msg))
	if token.Wait() && token.Error() != nil {
		return fmt.Errorf("disconnect request failed: %v", token.Error())
	}

	// Clean up the session
	b.sessionsMu.Lock()
	if session, exists := b.sessions[sessionID]; exists {
		session.State = BridgeSessionStateClosed
		delete(b.sessions, sessionID)
	}
	b.sessionsMu.Unlock()

	// Close the connection after releasing the mutex
	if conn != nil {
		conn.connMu.Lock()
		conn.closed = true // Mark as closed to prevent recursive DisconnectSession call
		if conn.readBuf != nil {
			close(conn.readBuf)
			conn.readBuf = nil
		}
		conn.connMu.Unlock()
		conn.cancel()
	}

	return nil
}
