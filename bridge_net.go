package bridge

import (
	"context"
	"errors"
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
	clientID       string // The client ID of the bridge
	rootTopic      string
	rootTopicParts []string
	qos            byte

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
	closeMu sync.Mutex
	closed  bool
}

// MQTTAddr implements net.Addr for MQTT connections
type MQTTAddr struct {
	network string
	address string
}

func (a *MQTTAddr) Network() string { return a.network }
func (a *MQTTAddr) String() string  { return a.address }

// ProxyAddr implements net.Addr for proxy connections
type ProxyAddr struct {
	network string
	address string
}

func (a *ProxyAddr) Network() string { return a.network }
func (a *ProxyAddr) String() string  { return a.address }

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
	disconnectMsg    = "disconnect"
	disconnectAckMsg = "disconnect_ack"

	// Error types
	errorMsg            = "error"
	errSessionActive    = "session_active"
	errSessionNotFound  = "session_not_found"
	errUnauthorized     = "unauthorized"
	errInvalidSession   = "invalid_session"
	errSessionSuspended = "session_suspended"
	errConnectionFailed = "connection_failed"
	errSessionClosed    = "session_closed"
	errInvalidState     = "invalid_state"
	errSessionExpired   = "session_expired"
	errMaxSessions      = "max_sessions"

	// Session management
	defaultSessionTimeout    = 30 * time.Minute // Default timeout for suspended sessions
	defaultDialTimeout       = 5 * time.Second  // Default timeout for dial operations
	defaultDisconnectTimeout = 1 * time.Minute  // Time to wait before cleaning up disconnected sessions
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

	// Get all sessions first
	sessions := b.sessionManager.GetAllSessions()

	// Suspend all active sessions
	for _, session := range sessions {
		// Mark as suspended in memory and trigger hooks
		if session.State == BridgeSessionStateActive {
			if err := b.sessionManager.SuspendSession(session.ID, session.ClientID); err != nil {
				b.logger.Error("Failed to suspend session during shutdown",
					zap.String("sessionID", session.ID),
					zap.Error(err))
			}
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

	// Lifecycle message handling
	respChan chan struct {
		payload []byte
		topic   string
	}

	reader io.Reader
	writer io.Writer
}

func (c *MQTTNetBridgeConn) SessionID() string {
	return c.sessionID
}

// handleLifecycleHandshake handles lifecycle messages for client connections
func (c *MQTTNetBridgeConn) handleLifecycleHandshake() {
	for {
		select {
		case resp := <-c.respChan:
			c.bridge.sessionManager.HandleLifecycleMessage(resp.payload, resp.topic)
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
	// and if the session is still active
	select {
	case <-c.ctx.Done():
		// Context already cancelled, we're being closed by DisconnectSession
		return nil
	default:
		c.bridge.mqttClient.Unsubscribe(c.downTopic)
		c.bridge.mqttClient.Unsubscribe(c.upTopic)

		if session, exists := c.bridge.sessionManager.GetSession(c.sessionID); exists && session.State == BridgeSessionStateActive {
			if err := c.bridge.DisconnectSession(c.sessionID); err != nil {
				c.bridge.logger.Error("Failed to disconnect session during close",
					zap.String("sessionID", c.sessionID),
					zap.Error(err))
			}
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
	// First set read deadline
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	// Then set write deadline
	return c.SetWriteDeadline(t)
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
	session, exists := b.sessionManager.GetSession(sessionID)
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
		connMu:     sync.RWMutex{},
	}

	// Subscribe to session up topic for server with QoS 1 to ensure delivery
	token := b.mqttClient.Subscribe(conn.upTopic, b.qos, b.handleIncomingData)
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
	startTime := time.Now()
	b.logger.Debug("Starting Dial operation",
		zap.String("targetBridgeID", targetBridgeID))

	// Parse session options
	cfg := &SessionConfig{
		State:       BridgeSessionStateActive,
		Timeout:     defaultSessionTimeout,
		DialTimeout: defaultDialTimeout,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	sessionID := cfg.SessionID
	clientID := b.clientID

	// Subscribe to handshake response
	responseTopic := fmt.Sprintf(handshakeResponseTopic, b.rootTopic, targetBridgeID, clientID)
	respChan := make(chan struct {
		payload []byte
		topic   string
	}, 1)

	// Create subscription handler
	handler := func(c mqtt.Client, m mqtt.Message) {
		b.logger.Debug("Received ack",
			zap.String("topic", string(m.Topic())),
			zap.String("payload", string(m.Payload())))
		select {
		case respChan <- struct {
			payload []byte
			topic   string
		}{
			payload: m.Payload(),
			topic:   m.Topic(),
		}:
		default:
			b.logger.Warn("Response channel full, dropping message",
				zap.String("topic", m.Topic()))
		}
	}

	// Subscribe to response topic
	token := b.mqttClient.Subscribe(responseTopic, b.qos, handler)
	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("handshake subscribe failed: %v", token.Error())
	}

	// Send handshake message
	handshakeTopic := fmt.Sprintf(handshakeRequestTopic, b.rootTopic, targetBridgeID, clientID)
	msg := fmt.Sprintf("%s:%s", connectMsg, cfg.Timeout.String())
	if sessionID != "" {
		msg = fmt.Sprintf("%s:%s", resumeMsg, sessionID)
	}

	token = b.mqttClient.Publish(handshakeTopic, b.qos, false, UnsafeBytes(msg))
	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("handshake publish failed: %v", token.Error())
	}

	// Wait for response
	select {
	case resp := <-respChan:
		payload := b.hooks.OnMessageReceived(resp.payload)

		msgParts := strings.Split(UnsafeString(payload), ":")
		msgType := msgParts[0]

		switch msgType {
		case connectAckMsg, resumeAckMsg:
			if len(msgParts) < 4 {
				return nil, NewBridgeError("dial", "invalid acknowledgment format", nil)
			}

			sessionID = msgParts[1]
			upTopic := msgParts[2]
			downTopic := msgParts[3]

			// Create client connection
			connCtx, cancel := context.WithCancel(b.ctx)
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
				connMu:     sync.RWMutex{},
			}

			conn.SetDeadline(time.Now().Add(cfg.Timeout))

			// Let SessionManager handle the session creation/resumption
			err := b.sessionManager.HandleConnectionEstablished(sessionID, conn, clientID, cfg.Timeout)
			if err != nil {
				conn.Close()
				return nil, err
			}

			// Subscribe to session messages
			token = b.mqttClient.Subscribe(conn.downTopic, b.qos, b.handleIncomingData)
			if token.Wait() && token.Error() != nil {
				conn.Close()
				return nil, fmt.Errorf("session subscribe failed: %v", token.Error())
			}

			conn.connMu.Lock()
			conn.connected = true
			conn.connMu.Unlock()

			go func() {
				for {
					select {
					case resp := <-conn.respChan:
						conn.bridge.sessionManager.HandleLifecycleMessage(resp.payload, resp.topic)
					case <-conn.ctx.Done():
						return
					}
				}
			}()

			b.logger.Info("Connection established",
				zap.String("sessionID", sessionID),
				zap.String("clientID", clientID),
				zap.Duration("elapsed", time.Since(startTime)))

			return conn, nil

		case errorMsg:
			if len(msgParts) < 2 {
				return nil, NewBridgeError("dial", "invalid error message format", nil)
			}
			errorType := msgParts[1]
			return nil, b.sessionManager.HandleSessionError(sessionID, errorType)

		default:
			return nil, NewBridgeError("dial", fmt.Sprintf("unexpected message type: %s", msgType), nil)
		}

	case <-time.After(cfg.DialTimeout):
		b.logger.Error("Handshake timeout",
			zap.Duration("elapsed", time.Since(startTime)))
		return nil, NewBridgeError("dial", "handshake timeout", nil)
	case <-ctx.Done():
		b.logger.Error("Context cancelled during handshake",
			zap.Error(ctx.Err()),
			zap.Duration("elapsed", time.Since(startTime)))
		return nil, NewBridgeError("dial", "context cancelled", ctx.Err())
	}
}

// SuspendSession suspends an active session for later resumption
func (b *MQTTNetBridge) SuspendSession(sessionID string) error {
	// Get session info first to get clientID
	session, exists := b.sessionManager.GetSession(sessionID)
	if !exists {
		b.logger.Error("Session not found", zap.String("sessionID", sessionID))
		return NewSessionNotFoundError(sessionID)
	}

	if session.State != BridgeSessionStateActive {
		b.logger.Error("Cannot suspend inactive session",
			zap.String("sessionID", sessionID),
			zap.String("state", session.State.String()))
		return NewSessionSuspendedError(sessionID)
	}

	// Prepare suspend message
	responseTopic := fmt.Sprintf(handshakeRequestTopic, b.rootTopic, session.Connection.remoteAddr, b.clientID)
	suspendPayload := fmt.Sprintf("%s:%s", suspendMsg, sessionID)

	// Send suspend message to the server bridge
	token := b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(suspendPayload))
	if token.Wait() && token.Error() != nil {
		b.logger.Error("Failed to send suspend message",
			zap.String("sessionID", sessionID),
			zap.Error(token.Error()))
		return NewBridgeError("suspend", "failed to send suspend message", token.Error())
	}

	// Subscribe to suspend response
	suspendResponseTopic := fmt.Sprintf(handshakeResponseTopic, b.rootTopic, session.Connection.remoteAddr, b.clientID)
	done := make(chan error, 1)

	token = b.mqttClient.Subscribe(suspendResponseTopic, b.qos, func(c mqtt.Client, m mqtt.Message) {
		b.logger.Debug("Received suspend response",
			zap.String("topic", m.Topic()),
			zap.String("payload", string(m.Payload())))

		msgParts := strings.Split(string(m.Payload()), ":")
		if len(msgParts) > 0 && msgParts[0] == "error" {
			done <- NewSessionNotFoundError(sessionID)
			return
		}
		done <- nil
	})
	defer b.mqttClient.Unsubscribe(suspendResponseTopic)

	if token.Wait() && token.Error() != nil {
		b.logger.Error("Failed to subscribe to suspend response",
			zap.String("sessionID", sessionID),
			zap.Error(token.Error()))
		return NewBridgeError("suspend", "failed to subscribe to suspend topic", token.Error())
	}

	// Wait for suspend acknowledgment with timeout
	select {
	case err := <-done:
		if err != nil {
			b.logger.Error("Suspend request failed",
				zap.String("sessionID", sessionID),
				zap.Error(err))
			return err
		}
		b.sessionManager.SuspendSession(sessionID, session.ClientID)
		b.logger.Info("Session suspended successfully", zap.String("sessionID", sessionID))
		return nil
	case <-time.After(100 * time.Millisecond):
		b.logger.Info("Session suspend request timed out, assuming success",
			zap.String("sessionID", sessionID))
		return nil
	}
}

// ResumeSession attempts to resume a suspended session
func (b *MQTTNetBridge) ResumeSession(ctx context.Context, targetBridgeID, sessionID string) (net.Conn, error) {
	session, exists := b.sessionManager.GetSession(sessionID)
	if exists && session.State == BridgeSessionStateActive {
		return nil, NewSessionActiveError(sessionID)
	}

	b.logger.Info("Resuming session", zap.String("sessionID", sessionID))
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

func (b *MQTTNetBridge) proxyConn(conn net.Conn, bConn net.Conn) {
	errChan := make(chan error, 2)
	done := make(chan struct{})

	// Copy from client to bridge
	go func() {
		_, err := io.Copy(bConn, conn)
		if err != nil && err != io.EOF && !isClosedConnError(err) {
			b.logger.Error("Error copying data from client to bridge", zap.Error(err))
		}
		errChan <- err
	}()

	// Copy from bridge to client
	go func() {
		_, err := io.Copy(conn, bConn)
		if err != nil && err != io.EOF && !isClosedConnError(err) {
			b.logger.Error("Error copying data from bridge to client", zap.Error(err))
		}
		errChan <- err
	}()

	// Wait for either copy operation to finish
	go func() {
		var proxyErr error
		for i := 0; i < 2; i++ {
			if err := <-errChan; err != nil && err != io.EOF && !isClosedConnError(err) {
				proxyErr = errors.Join(proxyErr, err)
			}
		}
		if proxyErr != nil {
			b.logger.Error("Proxy connection error",
				zap.Error(proxyErr))
		}
		close(done)
	}()

	// Wait for completion or context cancellation
	select {
	case <-done:
		b.logger.Debug("Proxy connection completed")
	case <-b.ctx.Done():
		b.logger.Debug("Proxy connection cancelled")
	}

	// Ensure both connections are closed
	conn.Close()
	bConn.Close()
}

// isClosedConnError returns true if the error is related to using a closed connection
func isClosedConnError(err error) bool {
	if err == nil {
		return false
	}
	str := err.Error()
	return strings.Contains(str, "use of closed network connection") ||
		strings.Contains(str, "context canceled") ||
		strings.Contains(str, "broken pipe")
}

// handleHandshake processes incoming handshake messages
func (b *MQTTNetBridge) handleHandshake(client mqtt.Client, msg mqtt.Message) {
	startTime := time.Now()
	payload := msg.Payload()
	payload = b.hooks.OnMessageReceived(payload)

	b.logger.Debug("Received handshake message",
		zap.String("payload", string(payload)),
		zap.Duration("elapsed", time.Since(startTime)))

	parts := strings.Split(msg.Topic(), "/")
	if len(parts) < 6 {
		b.logger.Error("Invalid handshake topic format",
			zap.String("topic", msg.Topic()),
			zap.Int("parts", len(parts)),
			zap.Duration("elapsed", time.Since(startTime)))
		return
	}

	msgParts := strings.Split(UnsafeString(payload), ":")
	msgType := msgParts[0]
	clientID := parts[len(parts)-1]

	if parts[len(parts)-2] != "request" {
		b.logger.Warn("Unexpected handshake message",
			zap.String("msgType", msgType),
			zap.String("requestType", parts[len(parts)-2]),
			zap.Duration("elapsed", time.Since(startTime)))
		return
	}

	responseTopic := fmt.Sprintf(handshakeResponseTopic, b.rootTopic, b.bridgeID, clientID)

	switch msgType {
	case connectMsg:
		timeout, err := time.ParseDuration(msgParts[1])
		if err != nil {
			timeout = defaultSessionTimeout
		}
		// Handle new connection
		sessionID := uuid.New().String()
		b.logger.Debug("Creating new connection",
			zap.String("sessionID", sessionID),
			zap.Duration("elapsed", time.Since(startTime)))

		conn := b.createNewConnection(sessionID)
		if conn == nil {
			err := b.sessionManager.HandleSessionError(sessionID, "failed_to_create_connection")
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, err.Error())))
			b.logger.Error("Failed to create connection",
				zap.String("sessionID", sessionID),
				zap.Duration("elapsed", time.Since(startTime)))
			return
		}

		b.handleNewConnection(conn, clientID, timeout, responseTopic, connectAckMsg)

	case resumeMsg:
		if len(msgParts) < 2 {
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errInvalidSession)))
			return
		}

		sessionID := msgParts[1]

		session, exists := b.sessionManager.GetSession(sessionID)
		if !exists {
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errSessionNotFound)))
			return
		}

		if session.State != BridgeSessionStateSuspended {
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errSessionActive)))
			return
		}

		// Update clientID for resumed session
		session.ClientID = clientID

		conn := b.createNewConnection(sessionID)
		if conn == nil {
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, "failed to create connection")))
			return
		}

		b.handleNewConnection(conn, clientID, session.Timeout, responseTopic, resumeAckMsg)

	case suspendMsg:
		if len(msgParts) < 2 {
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errInvalidSession)))
			return
		}

		sessionID := msgParts[1]
		session, exists := b.sessionManager.GetSession(sessionID)
		if !exists {
			err := b.sessionManager.HandleSessionError(sessionID, errSessionNotFound)
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, err.Error())))
			return
		}

		// Verify the client owns this session
		if session.ClientID != clientID {
			b.logger.Warn("Unauthorized suspend attempt",
				zap.String("sessionID", sessionID),
				zap.String("sessionClientID", session.ClientID),
				zap.String("requestingClientID", clientID))
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errUnauthorized)))
			return
		}

		if session.State != BridgeSessionStateActive {
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errInvalidState)))
			return
		}

		token := b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", suspendAckMsg, sessionID)))
		if ok := token.Wait(); !ok {
			b.logger.Error("Error sending suspend ack",
				zap.String("topic", responseTopic),
				zap.String("sessionID", sessionID))
			return
		}

		err := b.sessionManager.SuspendSession(sessionID, clientID)
		if err != nil {
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, err.Error())))
			return
		}

		b.logger.Info("Session suspended",
			zap.String("sessionID", sessionID),
			zap.String("clientID", clientID))

	case disconnectMsg:
		sessionID := msgParts[1]

		// Suspend the session, so that it can be resumed later, and so that it can be cleaned up by the ticker
		err := b.sessionManager.SuspendSession(sessionID, clientID)
		if err != nil {
			b.logger.Error("Failed to suspend session",
				zap.String("sessionID", sessionID),
				zap.Error(err))
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, err.Error())))
			return
		}

		// Send disconnect acknowledgment
		token := b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", disconnectAckMsg, sessionID)))
		if ok := token.Wait(); !ok {
			b.logger.Error("Error sending disconnect ack",
				zap.String("topic", responseTopic),
				zap.String("sessionID", sessionID))
			return
		}

		b.logger.Info("Session disconnected",
			zap.String("sessionID", sessionID),
			zap.String("clientID", clientID))
	}
}

// handleNewConnection processes a new connection request
func (b *MQTTNetBridge) handleNewConnection(conn *MQTTNetBridgeConn, clientID string, timeout time.Duration, responseTopic, ack string) {
	// Let SessionManager handle the session creation
	err := b.sessionManager.HandleConnectionEstablished(conn.sessionID, conn, clientID, timeout)
	if err != nil {
		errMsg := fmt.Sprintf("%s:%s", errorMsg, err.Error())
		b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(errMsg))
		return
	}

	// Send connection acknowledgment
	ackMsg := fmt.Sprintf("%s:%s:%s:%s", ack, conn.sessionID, conn.upTopic, conn.downTopic)
	b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(ackMsg))

	// Queue for Accept
	select {
	case b.acceptCh <- conn:
		conn.connMu.Lock()
		conn.connected = true
		conn.connMu.Unlock()
	default:
		b.logger.Warn("Accept channel full, dropping connection",
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
func (b *MQTTNetBridge) CleanupStaleSessions(timeout time.Duration) {
	b.sessionManager.CleanupStaleSessions(timeout)
}

// DisconnectSession disconnects an active session and cleans it up
func (b *MQTTNetBridge) DisconnectSession(sessionID string) error {
	session, exists := b.sessionManager.GetSession(sessionID)
	if !exists {
		return NewSessionNotFoundError(sessionID)
	}

	// Send disconnect request
	requestTopic := fmt.Sprintf(handshakeRequestTopic, b.rootTopic, session.Connection.remoteAddr.String(), b.clientID)
	msg := fmt.Sprintf("%s:%s", disconnectMsg, sessionID)
	token := b.mqttClient.Publish(requestTopic, b.qos, false, []byte(msg))
	if token.Wait() && token.Error() != nil {
		return NewBridgeError("disconnect", "disconnect request failed", token.Error())
	}

	return b.sessionManager.DisconnectSession(sessionID)
}

// startSessionCleanupTimer starts a timer to clean up a suspended session
func (b *MQTTNetBridge) startSessionCleanupTimer(sessionID string) {
	b.logger.Debug("Starting cleanup timer",
		zap.String("sessionID", sessionID),
		zap.Duration("timeout", defaultDisconnectTimeout))

	time.Sleep(defaultDisconnectTimeout)

	session, exists := b.sessionManager.GetSession(sessionID)
	if !exists {
		return
	}

	// Only clean up if still suspended and timeout has elapsed
	if session.State == BridgeSessionStateSuspended &&
		time.Since(session.LastSuspended) >= defaultDisconnectTimeout {
		b.sessionManager.RemoveSession(sessionID)
		b.logger.Debug("Cleaned up suspended session after timeout",
			zap.String("sessionID", sessionID))
	}
}

// HandleLifecycleMessage processes lifecycle messages for sessions
func (b *MQTTNetBridge) HandleLifecycleMessage(payload []byte, topic string) {
	msgParts := strings.Split(UnsafeString(payload), ":")
	msgType := msgParts[0]

	switch msgType {
	case suspendAckMsg:
		if len(msgParts) < 2 {
			b.logger.Error("Invalid suspend ack format",
				zap.String("topic", topic),
				zap.String("payload", string(payload)))
			return
		}
		sessionID := msgParts[1]

		b.logger.Debug("Received suspend ack",
			zap.String("sessionID", sessionID))

		err := b.sessionManager.SuspendSession(sessionID, b.clientID)
		if err != nil {
			b.logger.Error("Failed to suspend session",
				zap.String("sessionID", sessionID),
				zap.Error(err))
		}

	case resumeAckMsg:
		if len(msgParts) < 2 {
			b.logger.Error("Invalid resume ack format",
				zap.String("topic", topic),
				zap.String("payload", string(payload)))
			return
		}
		sessionID := msgParts[1]

		b.logger.Debug("Received resume ack",
			zap.String("sessionID", sessionID))

		session, exists := b.sessionManager.GetSession(sessionID)
		if exists {
			session.State = BridgeSessionStateActive
			session.LastActive = time.Now()
			b.sessionManager.AddSession(sessionID, session)
		}

	case disconnectAckMsg:
		if len(msgParts) < 2 {
			b.logger.Error("Invalid disconnect ack format",
				zap.String("topic", topic),
				zap.String("payload", string(payload)))
			return
		}
		sessionID := msgParts[1]
		b.logger.Info("Received disconnect ack",
			zap.String("sessionID", sessionID))

		session, exists := b.sessionManager.GetSession(sessionID)
		if exists {
			session.State = BridgeSessionStateClosed
			// Store connection to close after releasing lock
			conn := session.Connection
			session.Connection = nil
			b.sessionManager.RemoveSession(sessionID)

			// Close connection after releasing lock if it exists
			if conn != nil {
				conn.connMu.Lock()
				conn.closed = true
				close(conn.readBuf)
				conn.connMu.Unlock()
				conn.cancel()
			}
		}

	case errorMsg:
		if len(msgParts) < 2 {
			b.logger.Error("Invalid error message format",
				zap.String("topic", topic),
				zap.String("payload", string(payload)))
			return
		}
		b.logger.Error("Received error message",
			zap.String("error", string(payload)))
	}
}
