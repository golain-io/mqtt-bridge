package bridge

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

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

// SessionManager handles session lifecycle and state management
type SessionManager struct {
	bridge *MQTTNetBridge
	logger *zap.Logger

	// Session management
	sessions   map[string]*SessionInfo
	sessionsMu sync.RWMutex

	// Channel maps for session events
	sessionSuspendedChanMap   map[string]chan struct{}
	sessionSuspendedChanMapMu sync.RWMutex

	sessionResumeChanMap   map[string]chan struct{}
	sessionResumeChanMapMu sync.RWMutex

	sessionErrorChanMap   map[string]chan struct{}
	sessionErrorChanMapMu sync.RWMutex

	// Context for cleanup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewSessionManager creates a new session manager
func NewSessionManager(bridge *MQTTNetBridge, logger *zap.Logger) *SessionManager {
	ctx, cancel := context.WithCancel(context.Background())
	sm := &SessionManager{
		bridge:                  bridge,
		logger:                  logger,
		sessions:                make(map[string]*SessionInfo),
		sessionSuspendedChanMap: make(map[string]chan struct{}),
		sessionResumeChanMap:    make(map[string]chan struct{}),
		sessionErrorChanMap:     make(map[string]chan struct{}),
		ctx:                     ctx,
		cancel:                  cancel,
	}

	// Start cleanup task
	sm.startCleanupTask(5*time.Minute, defaultSessionTimeout)

	return sm
}

// GetSession retrieves a session by ID
func (sm *SessionManager) GetSession(sessionID string) (*SessionInfo, bool) {
	sm.sessionsMu.RLock()
	defer sm.sessionsMu.RUnlock()
	session, exists := sm.sessions[sessionID]
	return session, exists
}

// AddSession adds a new session to the manager
func (sm *SessionManager) AddSession(sessionID string, session *SessionInfo) {
	sm.sessionsMu.Lock()
	defer sm.sessionsMu.Unlock()
	sm.sessions[sessionID] = session
}

// RemoveSession removes a session from the manager
func (sm *SessionManager) RemoveSession(sessionID string) {
	sm.sessionsMu.Lock()
	defer sm.sessionsMu.Unlock()
	delete(sm.sessions, sessionID)
}

// CreateSession creates a new session
func (sm *SessionManager) CreateSession(sessionID, clientID string) *SessionInfo {
	sm.sessionsMu.Lock()
	defer sm.sessionsMu.Unlock()

	// Create a new connection
	conn := &MQTTNetBridgeConn{
		ctx:        sm.ctx,
		cancel:     func() {}, // Will be set by the bridge when needed
		bridge:     sm.bridge,
		sessionID:  sessionID,
		readBuf:    make(chan []byte, 100),
		localAddr:  sm.bridge.Addr(),
		remoteAddr: &MQTTAddr{network: "mqtt", address: clientID},
		upTopic:    fmt.Sprintf(sessionUpTopic, sm.bridge.rootTopic, sm.bridge.bridgeID, sessionID),
		downTopic:  fmt.Sprintf(sessionDownTopic, sm.bridge.rootTopic, sm.bridge.bridgeID, sessionID),
		role:       "server",
		connMu:     sync.RWMutex{},
	}

	session := &SessionInfo{
		ID:         sessionID,
		ClientID:   clientID,
		State:      BridgeSessionStateActive,
		LastActive: time.Now(),
		Connection: conn,
		Metadata:   make(map[string]string),
	}

	sm.sessions[sessionID] = session
	return session
}

// SuspendSession suspends an active session
func (sm *SessionManager) SuspendSession(sessionID string) error {
	sm.sessionsMu.Lock()
	session, exists := sm.sessions[sessionID]
	if !exists {
		sm.sessionsMu.Unlock()
		return NewSessionNotFoundError(sessionID)
	}

	if session.State != BridgeSessionStateActive {
		sm.sessionsMu.Unlock()
		return NewBridgeError("suspend", fmt.Sprintf("session %s is not active", sessionID), nil)
	}

	clientID := session.ClientID
	remoteAddr := session.Connection.remoteAddr.String()
	sm.sessionsMu.Unlock()

	// Create suspend channel
	suspendChan := make(chan struct{})
	sm.sessionSuspendedChanMapMu.Lock()
	sm.sessionSuspendedChanMap[sessionID] = suspendChan
	sm.sessionSuspendedChanMapMu.Unlock()

	// Send suspend request
	requestTopic := fmt.Sprintf(handshakeRequestTopic, sm.bridge.rootTopic, remoteAddr, clientID)
	msg := fmt.Sprintf("%s:%s", suspendMsg, sessionID)
	token := sm.bridge.mqttClient.Publish(requestTopic, sm.bridge.qos, false, []byte(msg))
	if token.Wait() && token.Error() != nil {
		sm.sessionSuspendedChanMapMu.Lock()
		delete(sm.sessionSuspendedChanMap, sessionID)
		sm.sessionSuspendedChanMapMu.Unlock()
		return NewBridgeError("suspend", "suspend request failed", token.Error())
	}

	// Wait for acknowledgment
	select {
	case <-suspendChan:
		sm.sessionSuspendedChanMapMu.Lock()
		delete(sm.sessionSuspendedChanMap, sessionID)
		sm.sessionSuspendedChanMapMu.Unlock()

		sm.sessionsMu.Lock()
		session.State = BridgeSessionStateSuspended
		session.LastSuspended = time.Now()
		sm.sessionsMu.Unlock()

		if session.Connection != nil {
			session.Connection.Close()
		}
		return nil

	case <-time.After(5 * time.Second):
		sm.sessionSuspendedChanMapMu.Lock()
		delete(sm.sessionSuspendedChanMap, sessionID)
		sm.sessionSuspendedChanMapMu.Unlock()
		return NewBridgeError("suspend", "suspend timeout", nil)
	}
}

// ResumeSession attempts to resume a suspended session
func (sm *SessionManager) ResumeSession(sessionID string) error {
	sm.sessionsMu.Lock()
	session, exists := sm.sessions[sessionID]
	if !exists {
		sm.sessionsMu.Unlock()
		return NewSessionNotFoundError(sessionID)
	}

	if session.State != BridgeSessionStateSuspended {
		sm.sessionsMu.Unlock()
		return NewBridgeError("resume", fmt.Sprintf("session %s is not suspended", sessionID), nil)
	}

	session.State = BridgeSessionStateActive
	session.LastActive = time.Now()
	sm.sessionsMu.Unlock()

	return nil
}

// DisconnectSession disconnects and cleans up a session
func (sm *SessionManager) DisconnectSession(sessionID string) error {
	sm.sessionsMu.Lock()
	session, exists := sm.sessions[sessionID]
	if !exists {
		sm.sessionsMu.Unlock()
		return NewSessionNotFoundError(sessionID)
	}

	if session.State != BridgeSessionStateActive {
		sm.sessionsMu.Unlock()
		return NewBridgeError("disconnect", fmt.Sprintf("session %s is not active", sessionID), nil)
	}

	clientID := session.ClientID
	remoteAddr := session.Connection.remoteAddr.String()
	conn := session.Connection
	sm.sessionsMu.Unlock()

	// Send disconnect request
	requestTopic := fmt.Sprintf(handshakeRequestTopic, sm.bridge.rootTopic, remoteAddr, clientID)
	msg := fmt.Sprintf("%s:%s", disconnectMsg, sessionID)
	token := sm.bridge.mqttClient.Publish(requestTopic, sm.bridge.qos, false, []byte(msg))
	if token.Wait() && token.Error() != nil {
		return NewBridgeError("disconnect", "disconnect request failed", token.Error())
	}

	// Clean up session
	sm.sessionsMu.Lock()
	session.State = BridgeSessionStateClosed
	delete(sm.sessions, sessionID)
	sm.sessionsMu.Unlock()

	// Close connection
	if conn != nil {
		conn.connMu.Lock()
		conn.closed = true
		if conn.readBuf != nil {
			close(conn.readBuf)
			conn.readBuf = nil
		}
		conn.connMu.Unlock()
	}

	return nil
}

// CleanupStaleSessions removes sessions that have been suspended longer than the timeout
func (sm *SessionManager) CleanupStaleSessions(timeout time.Duration) {
	if timeout == 0 {
		timeout = defaultSessionTimeout
	}

	sm.sessionsMu.Lock()
	defer sm.sessionsMu.Unlock()

	now := time.Now()
	for id, session := range sm.sessions {
		if (session.State == BridgeSessionStateSuspended && now.Sub(session.LastSuspended) > timeout) ||
			session.State == BridgeSessionStateClosed {
			delete(sm.sessions, id)
			sm.logger.Debug("Cleaned up stale session",
				zap.String("sessionID", id),
				zap.String("state", session.State.String()),
				zap.Duration("age", now.Sub(session.LastSuspended)))
		}
	}
}

// startCleanupTask starts a periodic cleanup of stale sessions
func (sm *SessionManager) startCleanupTask(interval, timeout time.Duration) {
	if interval == 0 {
		interval = 5 * time.Minute
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				sm.CleanupStaleSessions(timeout)
			case <-sm.ctx.Done():
				return
			}
		}
	}()
}

// Close stops the session manager and cleans up resources
func (sm *SessionManager) Close() {
	sm.cancel()
}

// GetAllSessions returns all sessions
func (sm *SessionManager) GetAllSessions() map[string]*SessionInfo {
	sm.sessionsMu.RLock()
	defer sm.sessionsMu.RUnlock()

	// Create a copy of the sessions map
	sessions := make(map[string]*SessionInfo, len(sm.sessions))
	for k, v := range sm.sessions {
		sessions[k] = v
	}
	return sessions
}

// HandleSessionError processes session error messages
func (sm *SessionManager) HandleSessionError(sessionID string, errorType string) error {
	sm.sessionsMu.RLock()
	session, exists := sm.sessions[sessionID]
	sm.sessionsMu.RUnlock()

	if !exists {
		return NewSessionNotFoundError(sessionID)
	}

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
		err = NewBridgeError("session", fmt.Sprintf("server error: %s", errorType), nil)
	}

	sm.logger.Error("Session error",
		zap.String("sessionID", sessionID),
		zap.String("errorType", errorType),
		zap.Error(err))

	// Close connection if exists
	if session.Connection != nil {
		session.Connection.Close()
	}

	return err
}

// HandleDisconnect processes a disconnect message for a session
func (sm *SessionManager) HandleDisconnect(sessionID string) {
	sm.logger.Debug("Handling session disconnect",
		zap.String("sessionID", sessionID))

	session, exists := sm.GetSession(sessionID)
	if !exists {
		sm.logger.Debug("No session found for disconnect",
			zap.String("sessionID", sessionID))
		return
	}

	sm.logger.Debug("Found session for disconnect",
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

	sm.logger.Debug("Marked session as suspended",
		zap.String("sessionID", sessionID))

	// Start cleanup timer
	go sm.startSessionCleanupTimer(sessionID)
}

// startSessionCleanupTimer starts a timer to clean up a suspended session
func (sm *SessionManager) startSessionCleanupTimer(sessionID string) {
	sm.logger.Debug("Starting cleanup timer",
		zap.String("sessionID", sessionID),
		zap.Duration("timeout", defaultDisconnectTimeout))

	time.Sleep(defaultDisconnectTimeout)

	session, exists := sm.GetSession(sessionID)
	if !exists {
		return
	}

	// Only clean up if still suspended and timeout has elapsed
	if session.State == BridgeSessionStateSuspended &&
		time.Since(session.LastSuspended) >= defaultDisconnectTimeout {
		sm.RemoveSession(sessionID)
		sm.logger.Debug("Cleaned up suspended session after timeout",
			zap.String("sessionID", sessionID))
	}
}

// HandleLifecycleMessage processes lifecycle messages for sessions
func (sm *SessionManager) HandleLifecycleMessage(payload []byte, topic string) {
	msgParts := strings.Split(UnsafeString(payload), ":")
	msgType := msgParts[0]

	switch msgType {
	case suspendAckMsg:
		if len(msgParts) < 2 {
			sm.logger.Error("Invalid suspend ack format",
				zap.String("topic", topic),
				zap.String("payload", string(payload)))
			return
		}
		sessionID := msgParts[1]

		// Signal suspend acknowledgment if channel exists
		sm.sessionSuspendedChanMapMu.RLock()
		suspendChan, exists := sm.sessionSuspendedChanMap[sessionID]
		sm.sessionSuspendedChanMapMu.RUnlock()
		if exists {
			select {
			case suspendChan <- struct{}{}:
				sm.logger.Debug("Sent suspend acknowledgment",
					zap.String("sessionID", sessionID))
			default:
				sm.logger.Warn("Failed to send suspend acknowledgment - channel full",
					zap.String("sessionID", sessionID))
			}
		}

		sm.sessionsMu.Lock()
		if session, exists := sm.sessions[sessionID]; exists {
			session.State = BridgeSessionStateSuspended
			session.LastSuspended = time.Now()
			// Store connection to close after releasing lock
			conn := session.Connection
			session.Connection = nil
			sm.sessionsMu.Unlock()

			// Close connection after releasing lock if it exists
			if conn != nil {
				conn.connMu.Lock()
				conn.closed = true
				close(conn.readBuf)
				conn.connMu.Unlock()
				conn.cancel()
			}
		} else {
			sm.sessionsMu.Unlock()
		}

	case resumeAckMsg:
		if len(msgParts) < 2 {
			sm.logger.Error("Invalid resume ack format",
				zap.String("topic", topic),
				zap.String("payload", string(payload)))
			return
		}
		sessionID := msgParts[1]
		sm.sessionsMu.Lock()
		if session, exists := sm.sessions[sessionID]; exists {
			session.State = BridgeSessionStateActive
			session.LastActive = time.Now()
		}
		sm.sessionsMu.Unlock()

	case disconnectAckMsg:
		if len(msgParts) < 2 {
			sm.logger.Error("Invalid disconnect ack format",
				zap.String("topic", topic),
				zap.String("payload", string(payload)))
			return
		}
		sessionID := msgParts[1]
		sm.logger.Info("Received disconnect ack",
			zap.String("sessionID", sessionID))

		sm.sessionsMu.Lock()
		if session, exists := sm.sessions[sessionID]; exists {
			session.State = BridgeSessionStateClosed
			// Store connection to close after releasing lock
			conn := session.Connection
			session.Connection = nil
			sm.sessionsMu.Unlock()

			// Close connection after releasing lock if it exists
			if conn != nil {
				conn.connMu.Lock()
				conn.closed = true
				close(conn.readBuf)
				conn.connMu.Unlock()
				conn.cancel()
			}
		} else {
			sm.sessionsMu.Unlock()
		}

	case errorMsg:
		if len(msgParts) < 2 {
			sm.logger.Error("Invalid error message format",
				zap.String("topic", topic),
				zap.String("payload", string(payload)))
			return
		}
		sm.logger.Error("Received error message",
			zap.String("error", string(payload)))
	}
}
