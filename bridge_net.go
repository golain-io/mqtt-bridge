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

// First, let's define the hook interface

// MQTTNetBridge implements net.Listener over MQTT
type MQTTNetBridge struct {
	mqttClient mqtt.Client
	logger     *zap.Logger
	bridgeID   string // Our "listening address"
	rootTopic  string

	// Connection management
	connections map[string]*MQTTNetBridgeConn
	connMu      sync.RWMutex

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
	handshakeRequestTopic  = "%s/bridge/handshake/%s/request/%s"  // serverID, clientID
	handshakeResponseTopic = "%s/bridge/handshake/%s/response/%s" // serverID, clientID

	// Session topics
	sessionUpTopic   = "%s/bridge/session/%s/%s/up"   // serverID, sessionID
	sessionDownTopic = "%s/bridge/session/%s/%s/down" // serverID, sessionID

	// Message types
	connectMsg    = "connect"
	connectAckMsg = "connect_ack"
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
func NewMQTTNetBridge(mqttClient mqtt.Client, bridgeID string, opts ...func(*MQTTNetBridge)) *MQTTNetBridge {
	ctx, cancel := context.WithCancel(context.Background())
	bridge := &MQTTNetBridge{
		mqttClient:  mqttClient,
		bridgeID:    bridgeID,
		connections: make(map[string]*MQTTNetBridgeConn),
		acceptCh:    make(chan *MQTTNetBridgeConn),
		ctx:         ctx,
		cancel:      cancel,
	}

	for _, opt := range opts {
		opt(bridge)
	}

	// if no logger set to no-op
	if bridge.logger == nil {
		bridge.logger = zap.NewNop()
	}

	bridge.hooks = &BridgeHooks{logger: bridge.logger}

	// Subscribe to handshake requests if we're a server
	handshakeTopic := fmt.Sprintf("%s/bridge/handshake/%s/request/+", bridge.rootTopic, bridge.bridgeID)
	bridge.logger.Debug("Subscribing to handshake topic", zap.String("topic", handshakeTopic))
	token := mqttClient.Subscribe(handshakeTopic, 0, bridge.handleHandshake)
	if token.Wait() && token.Error() != nil {
		bridge.logger.Fatal("Failed to subscribe to handshake topic",
			zap.String("topic", handshakeTopic),
			zap.Error(token.Error()))
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

	// Close all existing connections
	b.connMu.Lock()
	for _, conn := range b.connections {
		conn.Close()
	}
	b.connMu.Unlock()

	// Unsubscribe from handshake topic
	handshakeTopic := fmt.Sprintf("%s/bridge/handshake/%s/request/+", b.rootTopic, b.bridgeID)
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

	token := c.bridge.mqttClient.Publish(topic, 0, false, b)
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

	// Unsubscribe based on role
	if c.role == "server" {
		token := c.bridge.mqttClient.Unsubscribe(c.upTopic)
		token.Wait()
	} else {
		token := c.bridge.mqttClient.Unsubscribe(c.downTopic)
		token.Wait()
	}

	delete(c.bridge.connections, c.sessionID)

	close(c.readBuf)
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

// handleIncomingData processes incoming MQTT messages
func (b *MQTTNetBridge) handleIncomingData(client mqtt.Client, msg mqtt.Message) {
	payload := b.hooks.OnMessageReceived(msg.Payload())

	b.logger.Debug("Received incoming data",
		zap.String("topic", msg.Topic()),
		zap.Int("bytes", len(payload)))

	parts := strings.Split(msg.Topic(), "/")
	if len(parts) < 6 {
		b.logger.Error("Invalid topic format", zap.String("topic", msg.Topic()))
		return
	}

	sessionID := parts[len(parts)-2]

	b.connMu.RLock()
	conn, exists := b.connections[sessionID]
	b.connMu.RUnlock()

	if !exists || conn.closed {
		b.logger.Debug("No active connection for session",
			zap.String("sessionID", sessionID))
		return
	}

	select {
	case conn.readBuf <- payload:
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
	conn := &MQTTNetBridgeConn{
		bridge:     b,
		sessionID:  sessionID,
		readBuf:    make(chan []byte, 100),
		localAddr:  b.Addr(),
		remoteAddr: &MQTTAddr{network: "mqtt", address: sessionID},
		upTopic:    fmt.Sprintf(sessionUpTopic, b.rootTopic, b.bridgeID, sessionID),
		downTopic:  fmt.Sprintf(sessionDownTopic, b.rootTopic, b.bridgeID, sessionID),
		role:       "server",
	}

	// Subscribe to session up topic for server with QoS 1 to ensure delivery
	token := b.mqttClient.Subscribe(conn.upTopic, 1, func(client mqtt.Client, msg mqtt.Message) {
		if conn.closed {
			return
		}

		payload := b.hooks.OnMessageReceived(msg.Payload())

		b.logger.Debug("Server received data",
			zap.String("sessionID", sessionID),
			zap.Int("bytes", len(payload)))

		select {
		case conn.readBuf <- payload:
			b.logger.Debug("Server forwarded data to connection",
				zap.String("sessionID", sessionID),
				zap.Int("bytes", len(payload)))
		default:
			b.logger.Warn("Server read buffer full, dropping message",
				zap.String("session", sessionID))
		}
	})

	if token.Wait() && token.Error() != nil {
		b.logger.Error("Failed to subscribe to session topic",
			zap.String("topic", conn.upTopic),
			zap.Error(token.Error()))
		return nil
	}

	b.connMu.Lock()
	b.connections[sessionID] = conn
	b.connMu.Unlock()

	return conn
}

// Dial creates a new connection to a specific bridge
func (b *MQTTNetBridge) Dial(ctx context.Context, targetBridgeID string) (net.Conn, error) {
	clientID := uuid.New().String()
	b.logger.Info("Initiating connection",
		zap.String("targetBridgeID", targetBridgeID),
		zap.String("clientID", clientID))

	// Subscribe to handshake response
	responseTopic := fmt.Sprintf(handshakeResponseTopic, b.rootTopic, targetBridgeID, clientID)
	respChan := make(chan string, 1)

	token := b.mqttClient.Subscribe(responseTopic, 0, func(_ mqtt.Client, msg mqtt.Message) {
		payload := b.hooks.OnMessageReceived(msg.Payload())
		select {
		case respChan <- UnsafeString(payload):
		default:
			b.logger.Warn("Response channel full")
		}
	})
	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("handshake subscribe failed: %v", token.Error())
	}
	defer b.mqttClient.Unsubscribe(responseTopic)

	// Send connect request
	requestTopic := fmt.Sprintf(handshakeRequestTopic, b.rootTopic, targetBridgeID, clientID)
	token = b.mqttClient.Publish(requestTopic, 0, false, []byte(connectMsg))
	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("handshake request failed: %v", token.Error())
	}

	// Wait for connect_ack
	select {
	case resp := <-respChan:
		parts := strings.Split(resp, ":")
		if len(parts) != 4 {
			return nil, fmt.Errorf("invalid handshake response format")
		}

		msgType := parts[0]
		if msgType != connectAckMsg {
			return nil, fmt.Errorf("unexpected message type: %s", msgType)
		}

		sessionID := parts[1]
		upTopic := parts[2]
		downTopic := parts[3]

		b.logger.Debug("Received connection acknowledgment",
			zap.String("sessionID", sessionID),
			zap.String("upTopic", upTopic),
			zap.String("downTopic", downTopic))

		// Create client connection
		conn := &MQTTNetBridgeConn{
			bridge:     b,
			sessionID:  sessionID,
			readBuf:    make(chan []byte, 100),
			localAddr:  b.Addr(),
			remoteAddr: &MQTTAddr{network: "mqtt", address: targetBridgeID},
			upTopic:    upTopic,
			downTopic:  downTopic,
			role:       "client",
		}

		// Store connection
		b.connMu.Lock()
		b.connections[sessionID] = conn
		b.connMu.Unlock()

		// Subscribe to session messages
		token = b.mqttClient.Subscribe(conn.downTopic, 1, b.handleIncomingData)
		if token.Wait() && token.Error() != nil {
			conn.Close()
			return nil, fmt.Errorf("session subscribe failed: %v", token.Error())
		}
		b.logger.Debug("Subscribed to session down topic",
			zap.String("topic", conn.downTopic))

		conn.connMu.Lock()
		conn.connected = true
		conn.connMu.Unlock()

		return conn, nil

	case <-time.After(5 * time.Second):
		return nil, fmt.Errorf("handshake timeout")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
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

// Add new handshake handler
func (b *MQTTNetBridge) handleHandshake(client mqtt.Client, msg mqtt.Message) {
	payload := msg.Payload()

	// Execute hooks when receiving data
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

	msgType := UnsafeString(payload)
	clientID := parts[len(parts)-1]

	b.logger.Debug("Parsed handshake request",
		zap.String("msgType", msgType),
		zap.String("clientID", clientID))

	if parts[len(parts)-2] == "request" && msgType == connectMsg {
		// Generate new session
		sessionID := uuid.New().String()
		b.logger.Debug("Creating new session",
			zap.String("clientID", clientID),
			zap.String("sessionID", sessionID))

		// Create connection
		conn := b.createNewConnection(sessionID)
		if conn == nil {
			b.logger.Error("Failed to create connection",
				zap.String("sessionID", sessionID))
			return
		}

		// Send connection acknowledgment with session details
		responseTopic := fmt.Sprintf(handshakeResponseTopic, b.rootTopic, b.bridgeID, clientID)
		response := fmt.Sprintf("%s:%s:%s:%s", connectAckMsg, sessionID, conn.upTopic, conn.downTopic)

		b.logger.Debug("Sending connect_ack",
			zap.String("topic", responseTopic),
			zap.String("response", response))

		token := b.mqttClient.Publish(responseTopic, 0, false, []byte(response))
		if token.Wait() && token.Error() != nil {
			b.logger.Error("Failed to send connect_ack",
				zap.String("clientID", clientID),
				zap.Error(token.Error()))
			conn.Close()
			return
		}

		// Queue for Accept after successful ack
		select {
		case b.acceptCh <- conn:
			conn.connMu.Lock()
			conn.connected = true
			conn.connMu.Unlock()
			b.logger.Info("Connection established",
				zap.String("sessionID", sessionID),
				zap.String("clientID", clientID))
		default:
			b.logger.Warn("Accept channel full, dropping connection",
				zap.Int("total_connections", len(b.connections)),
				zap.Int("accept_channel_size", len(b.acceptCh)),
				zap.String("sessionID", sessionID))
			conn.Close()
		}
	} else {
		b.logger.Warn("Unexpected handshake message",
			zap.String("msgType", msgType),
			zap.String("requestType", parts[3]))
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
