package bridge

import (
	"context"
	"fmt"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"log/slog"
)

var (
	msgTypeRegex = regexp.MustCompile(`^([^:]+):(.+)$`)
	ackRegex     = regexp.MustCompile(`^([\w-]+):([\d\D]+?up):([\d\D]+?down)$`)
)

// Dial creates a new connection to a specific bridge
func (b *MQTTNetBridge) Dial(ctx context.Context, targetBridgeID string, opts ...SessionOption) (net.Conn, error) {
	startTime := time.Now()
	b.logger.Debug("Starting Dial operation",
		slog.String("targetBridgeID", targetBridgeID))

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
			slog.String("topic", string(m.Topic())),
			slog.String("payload", string(m.Payload())))
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
				slog.String("topic", m.Topic()))
		}
	}

	// Subscribe to response topic
	token := b.mqttClient.Subscribe(responseTopic, b.qos, handler)
	if token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("handshake subscribe failed: %v", token.Error())
	}
	// ponytail: Dial subscribes per attempt; without unsubscribe the client accumulates
	// handshake response topics (see devicebridge pool.go). Drop after ack or error.
	defer func() { b.mqttClient.Unsubscribe(responseTopic) }()

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
		matches := msgTypeRegex.FindStringSubmatch(UnsafeString(payload))
		if len(matches) != 3 {
			return nil, NewBridgeError("dial", "invalid message format", nil)
		}

		msgType := matches[1]
		msg := matches[2]

		switch msgType {
		case connectAckMsg, resumeAckMsg:
			// Parse remaining content for acknowledgment messages using regex
			// Format: sessionID:upTopic:downTopic
			ackMatches := ackRegex.FindStringSubmatch(msg)

			if len(ackMatches) != 4 {
				return nil, NewBridgeError("dial", "invalid acknowledgment format", nil)
			}

			sessionID = ackMatches[1]
			upTopic := ackMatches[2]
			downTopic := ackMatches[3]

			b.logger.Debug("upTopic", slog.String("upTopic", upTopic))
			b.logger.Debug("downTopic", slog.String("downTopic", downTopic))

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
				slog.String("sessionID", sessionID),
				slog.String("clientID", clientID),
				slog.Duration("elapsed", time.Since(startTime)))

			return conn, nil

		case errorMsg:
			errorType := msg
			return nil, b.sessionManager.HandleSessionError(sessionID, errorType)

		default:
			return nil, NewBridgeError("dial", fmt.Sprintf("unexpected message type: %s", msgType), nil)
		}

	case <-time.After(cfg.DialTimeout):
		b.logger.Error("Handshake timeout",
			slog.Duration("elapsed", time.Since(startTime)))
		return nil, NewBridgeError("dial", "handshake timeout", nil)
	case <-ctx.Done():
		b.logger.Error("Context cancelled during handshake",
			slog.Any("error", ctx.Err()),
			slog.Duration("elapsed", time.Since(startTime)))
		return nil, NewBridgeError("dial", "context cancelled", ctx.Err())
	}
}

// SuspendSession suspends an active session for later resumption
func (b *MQTTNetBridge) SuspendSession(sessionID string) error {
	// Get session info first to get clientID
	session, exists := b.sessionManager.GetSession(sessionID)
	if !exists {
		b.logger.Error("Session not found", slog.String("sessionID", sessionID))
		return NewSessionNotFoundError("suspend", sessionID)
	}

	if session.State != BridgeSessionStateActive {
		b.logger.Error("Cannot suspend inactive session",
			slog.String("sessionID", sessionID),
			slog.String("state", session.State.String()))
		return NewSessionSuspendedError("suspend", sessionID)
	}

	// Prepare suspend message
	responseTopic := fmt.Sprintf(handshakeRequestTopic, b.rootTopic, session.Connection.remoteAddr, b.clientID)
	suspendPayload := fmt.Sprintf("%s:%s", suspendMsg, sessionID)

	// Send suspend message to the server bridge
	token := b.mqttClient.Publish(responseTopic, b.qos, false, UnsafeBytes(suspendPayload))
	if token.Wait() && token.Error() != nil {
		b.logger.Error("Failed to send suspend message",
			slog.String("sessionID", sessionID),
			slog.Any("error", token.Error()))
		return NewBridgeError("suspend", "failed to send suspend message", token.Error())
	}

	// Subscribe to suspend response
	suspendResponseTopic := fmt.Sprintf(handshakeResponseTopic, b.rootTopic, session.Connection.remoteAddr, b.clientID)
	done := make(chan error, 1)

	token = b.mqttClient.Subscribe(suspendResponseTopic, b.qos, func(c mqtt.Client, m mqtt.Message) {
		b.logger.Debug("Received suspend response",
			slog.String("topic", m.Topic()),
			slog.String("payload", string(m.Payload())))

		msgParts := strings.Split(string(m.Payload()), ":")
		if len(msgParts) > 0 && msgParts[0] == "error" {
			done <- NewSessionNotFoundError("suspend", sessionID)
			return
		}
		done <- nil
	})
	defer b.mqttClient.Unsubscribe(suspendResponseTopic)

	if token.Wait() && token.Error() != nil {
		b.logger.Error("Failed to subscribe to suspend response",
			slog.String("sessionID", sessionID),
			slog.Any("error", token.Error()))
		return NewBridgeError("suspend", "failed to subscribe to suspend topic", token.Error())
	}

	// Wait for suspend acknowledgment with timeout
	select {
	case err := <-done:
		if err != nil {
			b.logger.Error("Suspend request failed",
				slog.String("sessionID", sessionID),
				slog.Any("error", err))
			return err
		}
		b.sessionManager.SuspendSession(sessionID, session.ClientID)
		b.logger.Info("Session suspended successfully", slog.String("sessionID", sessionID))
		return nil
	case <-time.After(100 * time.Millisecond):
		b.logger.Info("Session suspend request timed out, assuming success",
			slog.String("sessionID", sessionID))
		return nil
	}
}

// ResumeSession attempts to resume a suspended session
func (b *MQTTNetBridge) ResumeSession(ctx context.Context, targetBridgeID, sessionID string) (net.Conn, error) {
	session, exists := b.sessionManager.GetSession(sessionID)
	if exists && session.State == BridgeSessionStateActive {
		return nil, NewSessionActiveError("resume", sessionID)
	}

	b.logger.Info("Resuming session", slog.String("sessionID", sessionID))
	return b.Dial(ctx, targetBridgeID, WithSessionID(sessionID), WithSessionState(BridgeSessionStateActive))
}
