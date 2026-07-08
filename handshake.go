package bridge

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"log/slog"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"google.golang.org/grpc/resolver"
)

const (
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

// handleIncomingData processes incoming MQTT messages
func (b *MQTTNetBridge) handleIncomingData(client mqtt.Client, msg mqtt.Message) {
	payload := b.hooks.OnMessageReceived(msg.Payload())

	b.logger.Debug("Received incoming data",
		slog.String("topic", msg.Topic()),
		slog.Int("bytes", len(payload)),
		slog.String("payload", string(payload)))

	parts := strings.Split(msg.Topic(), "/")
	if len(parts) < 6 {
		b.logger.Error("Invalid topic format", slog.String("topic", msg.Topic()))
		return
	}

	sessionID := parts[len(parts)-2]
	conn, ok := b.sessionManager.SessionConnection(sessionID)
	if !ok {
		b.logger.Debug("No active session/connection",
			slog.String("sessionID", sessionID))
		return
	}

	conn.closeMu.RLock()
	closed := conn.closed
	conn.closeMu.RUnlock()
	if closed {
		b.logger.Debug("No active session/connection",
			slog.String("sessionID", sessionID))
		return
	}

	select {
	case conn.readBuf <- payload:
		b.logger.Debug("Forwarded data to connection",
			slog.String("sessionID", sessionID),
			slog.Int("bytes", len(payload)))
	default:
		b.logger.Warn("Read buffer full, dropping message",
			slog.String("session", sessionID))
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
			slog.String("topic", conn.upTopic),
			slog.Any("error", token.Error()))
		return nil
	}

	return conn
}

// handleHandshake processes incoming handshake messages
func (b *MQTTNetBridge) handleHandshake(client mqtt.Client, msg mqtt.Message) {
	payload := b.hooks.OnMessageReceived(msg.Payload())
	parts := strings.Split(msg.Topic(), "/")
	if len(parts) < 6 || parts[len(parts)-2] != "request" {
		b.logger.Error("Invalid handshake topic", slog.String("topic", msg.Topic()))
		return
	}
	msgParts := strings.SplitN(UnsafeString(payload), ":", 2)
	clientID := parts[len(parts)-1]
	responseTopic := fmt.Sprintf(handshakeResponseTopic, b.rootTopic, b.bridgeID, clientID)

	switch msgParts[0] {
	case connectMsg:
		b.handleConnect(clientID, responseTopic, msgParts)
	case resumeMsg:
		b.handleResume(clientID, responseTopic, msgParts)
	case suspendMsg:
		b.handleSuspend(clientID, responseTopic, msgParts)
	case disconnectMsg:
		b.handleDisconnect(clientID, responseTopic, msgParts)
	}
}

func (b *MQTTNetBridge) handleConnect(clientID, responseTopic string, msgParts []string) {
	startTime := time.Now()
	timeout := defaultSessionTimeout
	if len(msgParts) > 1 {
		if parsed, err := time.ParseDuration(msgParts[1]); err == nil {
			timeout = parsed
		}
	}

	sessionID := uuid.New().String()
	b.logger.Debug("Creating new connection",
		slog.String("sessionID", sessionID),
		slog.Duration("elapsed", time.Since(startTime)))

	conn := b.createNewConnection(sessionID)
	if conn == nil {
		err := b.sessionManager.HandleSessionError(sessionID, "failed_to_create_connection")
		b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, err.Error())))
		b.logger.Error("Failed to create connection",
			slog.String("sessionID", sessionID),
			slog.Duration("elapsed", time.Since(startTime)))
		return
	}

	b.handleNewConnection(conn, clientID, timeout, responseTopic, connectAckMsg)
}

func (b *MQTTNetBridge) handleResume(clientID, responseTopic string, msgParts []string) {
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
}

func (b *MQTTNetBridge) handleSuspend(clientID, responseTopic string, msgParts []string) {
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
			slog.String("sessionID", sessionID),
			slog.String("sessionClientID", session.ClientID),
			slog.String("requestingClientID", clientID))
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
			slog.String("topic", responseTopic),
			slog.String("sessionID", sessionID))
		return
	}

	err := b.sessionManager.SuspendSession(sessionID, clientID)
	if err != nil {
		b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, err.Error())))
		return
	}

	b.logger.Info("Session suspended",
		slog.String("sessionID", sessionID),
		slog.String("clientID", clientID))
}

func (b *MQTTNetBridge) handleDisconnect(clientID, responseTopic string, msgParts []string) {
	if len(msgParts) < 2 {
		b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, errInvalidSession)))
		return
	}

	sessionID := msgParts[1]

	// Suspend the session, so that it can be resumed later, and so that it can be cleaned up by the ticker
	err := b.sessionManager.SuspendSession(sessionID, clientID)
	if err != nil {
		b.logger.Error("Failed to suspend session",
			slog.String("sessionID", sessionID),
			slog.Any("error", err))
		b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", errorMsg, err.Error())))
		return
	}

	// Send disconnect acknowledgment
	token := b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(fmt.Sprintf("%s:%s", disconnectAckMsg, sessionID)))
	if ok := token.Wait(); !ok {
		b.logger.Error("Error sending disconnect ack",
			slog.String("topic", responseTopic),
			slog.String("sessionID", sessionID))
		return
	}

	b.logger.Info("Session disconnected",
		slog.String("sessionID", sessionID),
		slog.String("clientID", clientID))
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
			slog.String("sessionID", conn.sessionID))
		conn.Close()
	}
}
