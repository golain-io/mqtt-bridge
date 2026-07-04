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
	Timeout       time.Duration
}

// ISessionStore defines the interface for session storage providers
type ISessionStore interface {
	GetDBPath() string
	GetStoredSessions() (map[string]*SessionInfo, error)
}

// SessionManager handles session lifecycle and state management
type SessionManager struct {
	bridge *MQTTNetBridge
	logger *zap.Logger

	// Session storage
	store ISessionStore

	// Session management
	sessions   map[string]*SessionInfo
	sessionsMu sync.RWMutex

	// Context for cleanup
	ctx    context.Context
	cancel context.CancelFunc
}

// NewSessionManager creates a new session manager
func NewSessionManager(bridge *MQTTNetBridge, logger *zap.Logger, cleanUpInterval time.Duration) *SessionManager {
	ctx, cancel := context.WithCancel(context.Background())

	// Initialize with default values
	sm := &SessionManager{
		bridge:   bridge,
		logger:   logger,
		sessions: make(map[string]*SessionInfo),
		ctx:      ctx,
		cancel:   cancel,
	}

	sm.startCleanupTask(cleanUpInterval)
	return sm
}

func (sm *SessionManager) newConn(sessionID, clientID string) *MQTTNetBridgeConn {
	ctx, cancel := context.WithCancel(sm.bridge.ctx)
	return &MQTTNetBridgeConn{
		ctx:        ctx,
		cancel:     cancel,
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
}

// loadSessions loads all sessions from the storage
func (sm *SessionManager) loadSessions() error {
	if sm.store == nil {
		return nil
	}

	sessions, err := sm.store.GetStoredSessions()
	if err != nil {
		return fmt.Errorf("failed to get stored sessions: %w", err)
	}

	sm.sessionsMu.Lock()
	defer sm.sessionsMu.Unlock()

	for id, session := range sessions {
		// Create a new connection for active sessions
		if session.State == BridgeSessionStateActive {
			conn := sm.newConn(session.ID, session.ClientID)
			session.Connection = conn
		}

		sm.sessions[id] = session
		sm.logger.Info("Loaded session from storage",
			zap.String("sessionID", id),
			zap.String("state", session.State.String()),
			zap.String("clientID", session.ClientID))
	}

	return nil
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
func (sm *SessionManager) CreateSession(sessionID, clientID string, timeout time.Duration) *SessionInfo {
	sm.sessionsMu.Lock()
	defer sm.sessionsMu.Unlock()

	// Create a new connection
	conn := sm.newConn(sessionID, clientID)

	session := &SessionInfo{
		ID:         sessionID,
		ClientID:   clientID,
		State:      BridgeSessionStateActive,
		LastActive: time.Now(),
		Connection: conn,
		Metadata:   make(map[string]string),
		Timeout:    timeout,
	}

	sm.sessions[sessionID] = session
	return session
}

// SuspendSession suspends an active session
func (sm *SessionManager) SuspendSession(sessionID string, clientID string) error {
	sm.sessionsMu.Lock()

	session, exists := sm.sessions[sessionID]
	if !exists {
		sm.sessionsMu.Unlock()
		return NewSessionNotFoundError("suspend", sessionID)
	}

	// Verify client owns this session
	if session.ClientID != clientID {
		sm.sessionsMu.Unlock()
		return NewUnauthorizedError("suspend", sessionID)
	}

	// Only suspend active sessions
	if session.State != BridgeSessionStateActive {
		sm.sessionsMu.Unlock()
		return NewInvalidStateError(sessionID)
	}

	// Store connection to close after releasing lock
	conn := session.Connection

	// Update session state
	session.State = BridgeSessionStateSuspended
	session.LastSuspended = time.Now()
	session.Connection = nil
	sm.sessions[sessionID] = session
	sm.sessionsMu.Unlock()

	// Call hooks after updating state
	if sm.bridge.hooks != nil {
		if err := sm.bridge.hooks.OnSessionSuspended(session); err != nil {
			sm.logger.Error("Failed to execute OnSessionSuspended hooks",
				zap.Error(err))
		}
	}

	// Close connection after releasing lock if it exists
	if conn != nil {
		// Use Close() which handles context cancellation, channel closing, and cleanup
		// sync.Once ensures idempotency if called multiple times
		conn.Close()
	}

	return nil
}

// ResumeSession attempts to resume a suspended session
func (sm *SessionManager) ResumeSession(sessionID string) error {
	sm.sessionsMu.Lock()
	session, exists := sm.sessions[sessionID]
	if !exists {
		sm.sessionsMu.Unlock()
		return NewSessionNotFoundError("resume", sessionID)
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
		return NewSessionNotFoundError("disconnect", sessionID)
	}

	clientID := session.ClientID
	remoteAddr := session.Connection.remoteAddr.String()
	conn := session.Connection
	session.State = BridgeSessionStateClosed
	sm.sessionsMu.Unlock()

	// Call hook for disconnected session
	if sm.bridge.hooks != nil {
		if err := sm.bridge.hooks.OnSessionDisconnected(session); err != nil {
			sm.logger.Error("Failed to execute OnSessionDisconnected hooks",
				zap.Error(err))
		}
	}

	// Clean up session
	sm.sessionsMu.Lock()
	delete(sm.sessions, sessionID)
	sm.sessionsMu.Unlock()

	sm.logger.Info("Disconnected session",
		zap.String("sessionID", sessionID),
		zap.String("clientID", clientID),
		zap.String("targetBridgeID", remoteAddr))

	// Close connection
	if conn != nil {
		conn.Close()
	}

	return nil
}

// CleanupStaleSessions removes sessions that have been suspended longer than the timeout
func (sm *SessionManager) CleanupStaleSessions() {

	sm.sessionsMu.Lock()
	defer sm.sessionsMu.Unlock()

	for id, session := range sm.sessions {
		timeout := defaultSessionTimeout
		if session.Timeout > 0 {
			timeout = session.Timeout
		}

		if time.Since(session.LastSuspended) > timeout ||
			session.State == BridgeSessionStateClosed {
			delete(sm.sessions, id)
			sm.logger.Debug("Cleaned up stale session",
				zap.String("sessionID", id),
				zap.String("state", session.State.String()),
				zap.Duration("sessionTimeout", timeout),
				zap.Duration("timeSinceSuspended", time.Since(session.LastSuspended)))
		}
	}
}

// startCleanupTask starts a periodic cleanup of stale sessions
func (sm *SessionManager) startCleanupTask(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				sm.CleanupStaleSessions()
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
	_, exists := sm.sessions[sessionID]
	sm.sessionsMu.RUnlock()

	if !exists {
		return NewSessionNotFoundError("session", sessionID)
	}

	var err error
	switch errorType {
	case errSessionActive:
		err = NewSessionActiveError("session", sessionID)
	case errSessionNotFound:
		err = NewSessionNotFoundError("session", sessionID)
	case errInvalidSession:
		err = NewInvalidSessionError("session", sessionID)
	case errSessionSuspended:
		err = NewSessionSuspendedError("session", sessionID)
	default:
		err = NewBridgeError("session", fmt.Sprintf("server error: %s", errorType), nil)
	}

	sm.logger.Warn("Session error",
		zap.String("sessionID", sessionID),
		zap.String("errorType", errorType),
		zap.Error(err))

	return err
}

// HandleDisconnect processes a disconnect message for a session
func (sm *SessionManager) HandleDisconnect(clientID, sessionID string) error {
	sm.logger.Debug("Handling session disconnect",
		zap.String("sessionID", sessionID))

	session, exists := sm.GetSession(sessionID)
	if !exists {
		sm.logger.Debug("No session found for disconnect",
			zap.String("sessionID", sessionID))
		return NewSessionNotFoundError("disconnect", sessionID)
	}

	sm.logger.Debug("Found session for disconnect",
		zap.String("sessionID", sessionID),
		zap.String("currentState", session.State.String()))

	// Verify the client owns this session
	if session.ClientID != clientID {
		sm.logger.Warn("Unauthorized disconnect attempt",
			zap.String("sessionID", sessionID),
			zap.String("sessionClientID", session.ClientID),
			zap.String("requestingClientID", clientID))
		return NewUnauthorizedError("disconnect", sessionID)
	}

	if session.State != BridgeSessionStateActive {
		return NewSessionActiveError("disconnect", sessionID)
	}

	sm.logger.Info("Disconnecting session",
		zap.String("sessionID", sessionID),
		zap.String("clientID", clientID))

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

	return nil
}

// HandleLifecycleMessage processes lifecycle messages for sessions
func (sm *SessionManager) HandleLifecycleMessage(payload []byte, topic string) {
	msgParts := strings.Split(UnsafeString(payload), ":")
	msgType := msgParts[0]

	sm.logger.Info("Received lifecycle msg",
		zap.String("type", msgType))

	switch msgType {
	case suspendAckMsg:
		if len(msgParts) < 2 {
			sm.logger.Error("Invalid suspend ack format",
				zap.String("topic", topic),
				zap.String("payload", string(payload)))
			return
		}
		sessionID := msgParts[1]

		sm.SuspendSession(sessionID, "")

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
				// Use Close() which handles context cancellation, channel closing, and cleanup
				// sync.Once ensures idempotency if called multiple times
				conn.Close()
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

// HandleConnectionEstablished handles a new or resumed connection
func (sm *SessionManager) HandleConnectionEstablished(sessionID string, conn *MQTTNetBridgeConn, clientID string, timeout time.Duration) error {
	sm.sessionsMu.Lock()
	defer sm.sessionsMu.Unlock()

	// Check if session exists
	session, exists := sm.sessions[sessionID]
	if exists {
		// For existing sessions, verify state
		if session.State == BridgeSessionStateActive {
			return NewSessionActiveError("connect", sessionID)
		}
		// Update existing session
		session.State = BridgeSessionStateActive
		session.LastActive = time.Now()
		session.Connection = conn
		session.ClientID = clientID

		// Call hook for resumed session
		if sm.bridge.hooks != nil {
			if err := sm.bridge.hooks.OnSessionResumed(session); err != nil {
				sm.logger.Error("Failed to execute OnSessionResumed hooks",
					zap.Error(err))
			}
		}
	} else {
		// Create new session
		session = &SessionInfo{
			ID:            sessionID,
			State:         BridgeSessionStateActive,
			LastActive:    time.Now(),
			Connection:    conn,
			Metadata:      make(map[string]string),
			ClientID:      clientID,
			Timeout:       timeout,
			LastSuspended: time.Now(),
		}
		sm.sessions[sessionID] = session

		// Call hook for new session
		if sm.bridge.hooks != nil {
			if err := sm.bridge.hooks.OnSessionCreated(session); err != nil {
				sm.logger.Error("Failed to execute OnSessionCreated hooks",
					zap.Error(err))
			}
		}
	}

	return nil
}

// UpdateStore updates the session store and loads sessions from it
func (sm *SessionManager) UpdateStore(store ISessionStore) error {
	sm.sessionsMu.Lock()
	defer sm.sessionsMu.Unlock()

	// Store existing sessions by client ID for merging
	existingSessions := make(map[string]*SessionInfo)
	for _, session := range sm.sessions {
		existingSessions[session.ClientID] = session
	}

	// Update store
	sm.store = store

	// Load sessions from storage
	sessions, err := store.GetStoredSessions()
	if err != nil {
		return fmt.Errorf("failed to get stored sessions: %w", err)
	}

	// Merge stored sessions with existing ones
	for id, storedSession := range sessions {
		// If we have an existing session for this client, keep it
		if existingSession, exists := existingSessions[storedSession.ClientID]; exists {
			// Update stored session with existing connection if active
			if existingSession.State == BridgeSessionStateActive {
				storedSession.Connection = existingSession.Connection
				storedSession.State = BridgeSessionStateActive
				storedSession.LastActive = existingSession.LastActive
			}
		}

		// Create new connection for active sessions that don't exist in memory
		if storedSession.State == BridgeSessionStateActive && storedSession.Connection == nil {
			conn := sm.newConn(storedSession.ID, storedSession.ClientID)
			storedSession.Connection = conn
		}

		// Update or add the session
		sm.sessions[id] = storedSession
		sm.logger.Info("Loaded/Updated session from storage",
			zap.String("sessionID", id),
			zap.String("state", storedSession.State.String()),
			zap.String("clientID", storedSession.ClientID))
	}

	sm.logger.Info("Loaded/Updated sessions from storage",
		zap.Int("count", len(sm.sessions)))

	return nil
}
