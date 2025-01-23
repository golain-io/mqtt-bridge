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
	sessionManager *SessionManager

	// Channel for new connections waiting to be accepted
	acceptCh chan *MQTTNetBridgeConn

	// Shutdown management
	ctx    context.Context
	cancel context.CancelFunc

	hooks *BridgeHooks
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

	defaultDisconnectTimeout = 2 * time.Second // Time to wait before cleaning up disconnected sessions
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
		rootTopic:      cfg.rootTopic,
		rootTopicParts: strings.Split(cfg.rootTopic, "/"),
		qos:            cfg.qos,
		acceptCh:       make(chan *MQTTNetBridgeConn, 100),
		ctx:            ctx,
		cancel:         cancel,
		hooks:          &BridgeHooks{logger: cfg.logger},
	}

	// Initialize session manager
	bridge.sessionManager = NewSessionManager(bridge, cfg.logger)

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
	for _, session := range b.sessionManager.sessions {
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
		// Check if session is still active before trying to disconnect
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
	b.sessionManager.HandleDisconnect(sessionID)
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
	}

	// Add mutex initialization if not already present
	conn.connMu = sync.RWMutex{}

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
		State: BridgeSessionStateActive,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	sessionID := cfg.SessionID
	clientID := uuid.New().String()

	// Subscribe to handshake response
	responseTopic := fmt.Sprintf(handshakeResponseTopic, b.rootTopic, targetBridgeID, clientID)
	respChan := make(chan struct {
		payload []byte
		topic   string
	}, 1)

	b.logger.Debug("Subscribing to handshake response topic",
		zap.String("topic", responseTopic),
		zap.Duration("elapsed", time.Since(startTime)))

	token := b.mqttClient.Subscribe(responseTopic, b.qos, func(client mqtt.Client, msg mqtt.Message) {
		select {
		case respChan <- struct {
			payload []byte
			topic   string
		}{
			payload: msg.Payload(),
			topic:   msg.Topic(),
		}:
			b.logger.Debug("Forwarded handshake response",
				zap.String("topic", msg.Topic()),
				zap.ByteString("payload", msg.Payload()),
				zap.Duration("elapsed", time.Since(startTime)))
		default:
			b.logger.Warn("Response channel full",
				zap.String("topic", msg.Topic()))
		}
	})

	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("handshake subscribe failed: %v", token.Error())
	}

	b.logger.Debug("Successfully subscribed to handshake response topic",
		zap.String("topic", responseTopic),
		zap.Duration("elapsed", time.Since(startTime)))

	// Send connect request
	requestTopic := fmt.Sprintf(handshakeRequestTopic, b.rootTopic, targetBridgeID, clientID)
	var msg string
	if sessionID != "" {
		msg = fmt.Sprintf("resume:%s", sessionID)
	} else {
		msg = connectMsg
	}

	b.logger.Debug("Sending connection request",
		zap.String("topic", requestTopic),
		zap.String("message", msg),
		zap.Duration("elapsed", time.Since(startTime)))

	token = b.mqttClient.Publish(requestTopic, b.qos, false, []byte(msg))
	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("handshake request failed: %v", token.Error())
	}

	b.logger.Debug("Successfully sent connection request, waiting for response",
		zap.Duration("elapsed", time.Since(startTime)))

	// Wait for connect_ack with timeout
	select {
	case resp := <-respChan:
		b.logger.Debug("Received handshake response",
			zap.String("topic", resp.topic),
			zap.ByteString("payload", resp.payload),
			zap.Duration("elapsed", time.Since(startTime)))

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

			b.logger.Debug("Processing connection acknowledgment",
				zap.String("sessionID", sessionID),
				zap.String("upTopic", upTopic),
				zap.String("downTopic", downTopic),
				zap.Duration("elapsed", time.Since(startTime)))

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
			b.sessionManager.AddSession(sessionID, &SessionInfo{
				ID:         sessionID,
				State:      BridgeSessionStateActive,
				LastActive: time.Now(),
				Connection: conn,
				Metadata:   make(map[string]string),
				ClientID:   clientID,
			})

			b.logger.Debug("Subscribing to session down topic",
				zap.String("topic", conn.downTopic),
				zap.Duration("elapsed", time.Since(startTime)))

			// Subscribe to session messages
			token = b.mqttClient.Subscribe(conn.downTopic, b.qos, b.handleIncomingData)
			if token.Wait() && token.Error() != nil {
				conn.Close()
				return nil, fmt.Errorf("session subscribe failed: %v", token.Error())
			}

			b.logger.Debug("Successfully subscribed to session down topic",
				zap.String("topic", conn.downTopic),
				zap.Duration("elapsed", time.Since(startTime)))

			conn.connMu.Lock()
			conn.connected = true
			conn.connMu.Unlock()

			// After successful handshake, update session info
			session, exists := b.sessionManager.GetSession(sessionID)
			if !exists {
				conn.Close()
				return nil, fmt.Errorf("session not found after handshake")
			}
			session.State = BridgeSessionStateActive
			session.LastActive = time.Now()
			session.Connection = conn

			go conn.handleLifecycleHandshake()

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
			var err error
			switch errorType {
			case errSessionActive:
				err = NewSessionActiveError(sessionID)
			case errSessionNotFound:
				err = NewSessionNotFoundError(sessionID)
			case errInvalidSession:
				err = NewInvalidSessionError(sessionID)
			case errSessionSuspended:
				err = NewSessionSuspendedError(sessionID)
			case "unauthorized":
				err = NewUnauthorizedError(sessionID)
			case "failed_to_create_connection":
				err = NewConnectionFailedError(sessionID, nil)
			default:
				err = NewBridgeError("dial", fmt.Sprintf("server error: %s", errorType), nil)
			}
			b.logger.Error("Received error from server",
				zap.String("errorType", errorType),
				zap.Error(err),
				zap.Duration("elapsed", time.Since(startTime)))
			return nil, err

		default:
			return nil, NewBridgeError("dial", fmt.Sprintf("unexpected message type: %s", msgType), nil)
		}

	case <-time.After(5 * time.Second):
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
	return b.sessionManager.SuspendSession(sessionID)
}

// ResumeSession attempts to resume a suspended session
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
	startTime := time.Now()
	payload := msg.Payload()
	payload = b.hooks.OnMessageReceived(payload)

	b.logger.Debug("Received handshake message",
		zap.String("topic", msg.Topic()),
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

	b.logger.Debug("Parsed handshake request",
		zap.String("msgType", msgType),
		zap.String("clientID", clientID),
		zap.Duration("elapsed", time.Since(startTime)))

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

		b.handleNewConnection(conn, clientID, responseTopic)
		b.logger.Debug("Completed new connection handling",
			zap.String("sessionID", sessionID),
			zap.Duration("elapsed", time.Since(startTime)))

	case resumeMsg:
		if len(msgParts) < 2 {
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errInvalidSession)))
			return
		}

		sessionID := msgParts[1]
		b.logger.Debug("Handling resume request",
			zap.String("sessionID", sessionID),
			zap.Duration("elapsed", time.Since(startTime)))

		session, exists := b.sessionManager.GetSession(sessionID)
		if !exists {
			err := b.sessionManager.HandleSessionError(sessionID, errSessionNotFound)
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, err.Error())))
			return
		}

		if session.State == BridgeSessionStateActive {
			// Add clientID info to error logging
			b.logger.Warn("Attempt to resume active session",
				zap.String("sessionID", sessionID),
				zap.String("currentClientID", session.ClientID),
				zap.String("requestingClientID", clientID),
				zap.Duration("elapsed", time.Since(startTime)))
			err := b.sessionManager.HandleSessionError(sessionID, errSessionActive)
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, err.Error())))
			return
		}

		if session.State != BridgeSessionStateSuspended {
			err := b.sessionManager.HandleSessionError(sessionID, errSessionSuspended)
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, err.Error())))
			return
		}

		// Update clientID for resumed session
		session.ClientID = clientID

		conn := b.createNewConnection(sessionID)
		if conn == nil {
			err := b.sessionManager.HandleSessionError(sessionID, "failed_to_create_connection")
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, err.Error())))
			return
		}

		b.handleNewConnection(conn, clientID, responseTopic)
		b.logger.Debug("Completed resume handling",
			zap.String("sessionID", sessionID),
			zap.Duration("elapsed", time.Since(startTime)))

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
			err := b.sessionManager.HandleSessionError(sessionID, "unauthorized")
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, err.Error())))
			return
		}

		if session.State != BridgeSessionStateActive {
			err := b.sessionManager.HandleSessionError(sessionID, errSessionActive)
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, err.Error())))
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
			session.Connection.Close()
		}

	case disconnectMsg:
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
			b.logger.Warn("Unauthorized disconnect attempt",
				zap.String("sessionID", sessionID),
				zap.String("sessionClientID", session.ClientID),
				zap.String("requestingClientID", clientID))
			err := b.sessionManager.HandleSessionError(sessionID, "unauthorized")
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, err.Error())))
			return
		}

		if session.State != BridgeSessionStateActive {
			err := b.sessionManager.HandleSessionError(sessionID, errSessionActive)
			b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, err.Error())))
			return
		}

		b.logger.Info("Disconnecting session",
			zap.String("sessionID", sessionID),
			zap.String("clientID", clientID))

		// Send disconnect acknowledgment instead of error message
		b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", disconnectAckMsg, sessionID)))

		// Mark session as suspended instead of deleting it
		session.State = BridgeSessionStateSuspended

		b.handleDisconnect(sessionID)
	}
}

// handleNewConnection processes a new connection after successful handshake
func (b *MQTTNetBridge) handleNewConnection(conn *MQTTNetBridgeConn, clientID, responseTopic string) {
	startTime := time.Now()
	// Send connection acknowledgment with session details
	response := fmt.Sprintf("%s:%s:%s:%s", connectAckMsg, conn.sessionID, conn.upTopic, conn.downTopic)

	b.logger.Debug("Sending connect_ack",
		zap.String("topic", responseTopic),
		zap.String("response", response),
		zap.Duration("elapsed", time.Since(startTime)))

	token := b.mqttClient.Publish(responseTopic, b.qos, false, []byte(response))
	if token.Wait() && token.Error() != nil {
		b.logger.Error("Failed to send connect_ack",
			zap.String("clientID", clientID),
			zap.Error(token.Error()),
			zap.Duration("elapsed", time.Since(startTime)))
		conn.Close()
		return
	}

	b.logger.Debug("Successfully sent connect_ack",
		zap.String("sessionID", conn.sessionID),
		zap.Duration("elapsed", time.Since(startTime)))

	// Create or update session info
	b.sessionManager.AddSession(conn.sessionID, &SessionInfo{
		ID:         conn.sessionID,
		ClientID:   clientID,
		State:      BridgeSessionStateActive,
		LastActive: time.Now(),
		Connection: conn,
		Metadata:   make(map[string]string),
	})

	// Queue for Accept after successful ack
	select {
	case b.acceptCh <- conn:
		conn.connMu.Lock()
		conn.connected = true
		conn.connMu.Unlock()
		b.logger.Info("Connection established",
			zap.String("sessionID", conn.sessionID),
			zap.String("clientID", clientID),
			zap.Duration("elapsed", time.Since(startTime)))
	default:
		sessions := b.sessionManager.GetAllSessions()
		b.logger.Warn("Accept channel full, dropping connection",
			zap.Int("total_connections", len(sessions)),
			zap.Int("accept_channel_size", len(b.acceptCh)),
			zap.String("sessionID", conn.sessionID),
			zap.Duration("elapsed", time.Since(startTime)))
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

// CleanupStaleSessions removes sessions that have been suspended longer than the timeout
func (b *MQTTNetBridge) CleanupStaleSessions(timeout time.Duration) {
	b.sessionManager.CleanupStaleSessions(timeout)
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
	return b.sessionManager.DisconnectSession(sessionID)
}
