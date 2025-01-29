package hooks

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"

	bridge "github.com/golain-io/mqtt-bridge"
)

// SQLiteHook implements BridgeHook interface to provide session persistence
type SQLiteHook struct {
	bridge.BridgeHookBase
	logger    *zap.Logger
	isRunning atomic.Bool
	id        string
	db        *sql.DB
}

// SQLiteConfig holds configuration for SQLite hook
type SQLiteConfig struct {
	DBPath string // Path to SQLite database file
}

// NewSQLiteHook creates a new SQLiteHook instance
func NewSQLiteHook(logger *zap.Logger) *SQLiteHook {
	return &SQLiteHook{
		logger: logger,
		id:     "sqlite_hook",
	}
}

// OnSessionCreated handles new session creation
func (h *SQLiteHook) OnSessionCreated(session *bridge.SessionInfo) error {
	if !h.isRunning.Load() {
		return nil
	}

	metadata := map[string]string{
		"event": "created",
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	_, err = h.db.Exec(`
		INSERT INTO sessions 
		(session_id, state, client_id, last_active, metadata, updated_at) 
		VALUES (?, ?, ?, ?, ?, ?)`,
		session.ID, "active", session.ClientID, time.Now(), metadataJSON, time.Now())

	if err != nil {
		return fmt.Errorf("failed to store session state: %v", err)
	}

	h.logger.Debug("Stored new session",
		zap.String("sessionID", session.ID),
		zap.String("clientID", session.ClientID))

	return nil
}

// OnSessionResumed handles session resumption
func (h *SQLiteHook) OnSessionResumed(session *bridge.SessionInfo) error {
	if !h.isRunning.Load() {
		return nil
	}

	metadata := map[string]string{
		"event": "resumed",
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	_, err = h.db.Exec(`
		UPDATE sessions 
		SET state = ?, last_active = ?, client_id = ?, metadata = ?, updated_at = ?
		WHERE session_id = ?`,
		"active", time.Now(), session.ClientID, metadataJSON, time.Now(), session.ID)

	if err != nil {
		return fmt.Errorf("failed to update session state: %v", err)
	}

	h.logger.Debug("Updated resumed session",
		zap.String("sessionID", session.ID),
		zap.String("clientID", session.ClientID))

	return nil
}

// OnSessionSuspended handles session suspension
func (h *SQLiteHook) OnSessionSuspended(session *bridge.SessionInfo) error {
	if !h.isRunning.Load() {
		return nil
	}

	metadata := map[string]string{
		"event": "suspended",
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	_, err = h.db.Exec(`
		UPDATE sessions 
		SET state = ?, last_suspended = ?, metadata = ?, updated_at = ?
		WHERE session_id = ?`,
		"suspended", time.Now(), metadataJSON, time.Now(), session.ID)

	if err != nil {
		return fmt.Errorf("failed to update session state: %v", err)
	}

	h.logger.Debug("Updated suspended session",
		zap.String("sessionID", session.ID))

	return nil
}

// OnSessionDisconnected handles session disconnection
func (h *SQLiteHook) OnSessionDisconnected(session *bridge.SessionInfo) error {
	if !h.isRunning.Load() {
		return nil
	}

	metadata := map[string]string{
		"event": "disconnected",
	}
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %v", err)
	}

	_, err = h.db.Exec(`
		UPDATE sessions 
		SET state = ?, last_suspended = ?, metadata = ?, updated_at = ?
		WHERE session_id = ?`,
		"closed", time.Now(), metadataJSON, time.Now(), session.ID)

	if err != nil {
		return fmt.Errorf("failed to update session state: %v", err)
	}

	h.logger.Debug("Updated disconnected session",
		zap.String("sessionID", session.ID))

	return nil
}

// Provides indicates whether this hook provides the specified functionality
func (h *SQLiteHook) Provides(b byte) bool {
	return b == bridge.OnSessionCreated ||
		b == bridge.OnSessionResumed ||
		b == bridge.OnSessionSuspended ||
		b == bridge.OnSessionDisconnected
}

// Init initializes the hook with the provided configuration
func (h *SQLiteHook) Init(config any) error {
	cfg, ok := config.(*SQLiteConfig)
	if !ok {
		return fmt.Errorf("invalid configuration type for SQLite hook")
	}

	// Open SQLite database
	db, err := sql.Open("sqlite3", cfg.DBPath)
	if err != nil {
		return fmt.Errorf("failed to open database: %v", err)
	}

	// Create sessions table if it doesn't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS sessions (
			session_id TEXT PRIMARY KEY,
			state TEXT NOT NULL,
			client_id TEXT NOT NULL,
			last_active DATETIME,
			last_suspended DATETIME,
			metadata TEXT,
			updated_at DATETIME NOT NULL
		)`)
	if err != nil {
		db.Close()
		return fmt.Errorf("failed to create sessions table: %v", err)
	}

	h.db = db
	h.isRunning.Store(true)
	return nil
}

// Stop gracefully stops the hook
func (h *SQLiteHook) Stop() error {
	h.isRunning.Store(false)
	if h.db != nil {
		return h.db.Close()
	}
	return nil
}

// ID returns the unique identifier for this hook
func (h *SQLiteHook) ID() string {
	return h.id
}

// GetStoredSessions implements the SessionStore interface to retrieve all stored sessions
func (h *SQLiteHook) GetStoredSessions() (map[string]*bridge.SessionInfo, error) {
	if !h.isRunning.Load() {
		return nil, nil
	}

	rows, err := h.db.Query(`
		SELECT session_id, state, client_id, last_active, last_suspended, metadata
		FROM sessions
		WHERE state != 'closed'`)
	if err != nil {
		return nil, fmt.Errorf("failed to query sessions: %v", err)
	}
	defer rows.Close()

	sessions := make(map[string]*bridge.SessionInfo)
	for rows.Next() {
		var sessionID, state, clientID string
		var lastActive, lastSuspended sql.NullTime
		var metadataJSON []byte
		var metadata map[string]string

		err := rows.Scan(&sessionID, &state, &clientID, &lastActive, &lastSuspended, &metadataJSON)
		if err != nil {
			return nil, fmt.Errorf("failed to scan session row: %v", err)
		}

		if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %v", err)
		}

		// Convert state string to BridgeSessionState
		var bridgeState bridge.BridgeSessionState
		switch state {
		case "active":
			bridgeState = bridge.BridgeSessionStateActive
		case "suspended":
			bridgeState = bridge.BridgeSessionStateSuspended
		case "closed":
			bridgeState = bridge.BridgeSessionStateClosed
		}

		sessions[sessionID] = &bridge.SessionInfo{
			ID:            sessionID,
			ClientID:      clientID,
			State:         bridgeState,
			LastActive:    lastActive.Time,
			LastSuspended: lastSuspended.Time,
			Metadata:      metadata,
		}
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating session rows: %v", err)
	}

	h.logger.Debug("Retrieved stored sessions",
		zap.Int("count", len(sessions)))

	return sessions, nil
}
