package bridge

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
)

const (
	// SQLite table creation query
	createTableQuery = `
	CREATE TABLE IF NOT EXISTS sessions (
		id TEXT PRIMARY KEY,
		client_id TEXT,
		state INTEGER,
		last_active DATETIME,
		last_suspended DATETIME,
		metadata TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`

	// Default SQLite database file
	defaultDBFile = "bridge_sessions.db"
)

// SQLiteHookOptions contains configuration settings for the SQLite hook
type SQLiteHookOptions struct {
	DBFile string // Path to SQLite database file
}

// SQLiteHook is a persistent storage hook using SQLite as a backend
type SQLiteHook struct {
	db     *sql.DB
	logger *zap.Logger
	config *SQLiteHookOptions
	mu     sync.RWMutex
}

// ID returns the id of the hook
func (h *SQLiteHook) ID() string {
	return "sqlite-db"
}

// Provides indicates which hook methods this hook provides
func (h *SQLiteHook) Provides(b byte) bool {
	return true // This hook provides all methods
}

// Init initializes and connects to the SQLite database
func (h *SQLiteHook) Init(config any) error {
	if _, ok := config.(*SQLiteHookOptions); !ok && config != nil {
		return fmt.Errorf("invalid config type")
	}

	if config == nil {
		config = &SQLiteHookOptions{
			DBFile: defaultDBFile,
		}
	}

	h.config = config.(*SQLiteHookOptions)

	// Open SQLite database
	db, err := sql.Open("sqlite3", h.config.DBFile)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	h.db = db

	// Create tables if they don't exist
	if _, err := h.db.Exec(createTableQuery); err != nil {
		return fmt.Errorf("failed to create tables: %w", err)
	}

	return nil
}

// Stop closes the SQLite database connection
func (h *SQLiteHook) Stop() error {
	if h.db != nil {
		return h.db.Close()
	}
	return nil
}

// OnMessageReceived is called when a message is received
func (h *SQLiteHook) OnMessageReceived(msg []byte) []byte {
	return msg // Pass through
}

// SaveSession saves or updates a session in the database
func (h *SQLiteHook) SaveSession(session *SessionInfo) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.db == nil {
		return fmt.Errorf("database not initialized")
	}

	// Convert metadata map to JSON
	metadataJSON, err := json.Marshal(session.Metadata)
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	// Upsert query
	query := `
		INSERT INTO sessions (id, client_id, state, last_active, last_suspended, metadata)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			client_id = excluded.client_id,
			state = excluded.state,
			last_active = excluded.last_active,
			last_suspended = excluded.last_suspended,
			metadata = excluded.metadata;
	`

	_, err = h.db.Exec(query,
		session.ID,
		session.ClientID,
		int(session.State),
		session.LastActive,
		session.LastSuspended,
		string(metadataJSON),
	)

	if err != nil {
		return fmt.Errorf("failed to save session: %w", err)
	}

	return nil
}

// LoadSession loads a session from the database
func (h *SQLiteHook) LoadSession(sessionID string) (*SessionInfo, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.db == nil {
		return nil, fmt.Errorf("database not initialized")
	}

	query := `
		SELECT client_id, state, last_active, last_suspended, metadata
		FROM sessions
		WHERE id = ?;
	`

	var (
		clientID      string
		state         int
		lastActive    time.Time
		lastSuspended time.Time
		metadataStr   string
	)

	err := h.db.QueryRow(query, sessionID).Scan(
		&clientID,
		&state,
		&lastActive,
		&lastSuspended,
		&metadataStr,
	)

	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to load session: %w", err)
	}

	// Parse metadata JSON
	var metadata map[string]string
	if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
	}

	return &SessionInfo{
		ID:            sessionID,
		ClientID:      clientID,
		State:         BridgeSessionState(state),
		LastActive:    lastActive,
		LastSuspended: lastSuspended,
		Metadata:      metadata,
	}, nil
}

// LoadAllSessions loads all sessions from the database
func (h *SQLiteHook) LoadAllSessions() (map[string]*SessionInfo, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.db == nil {
		return nil, fmt.Errorf("database not initialized")
	}

	query := `
		SELECT id, client_id, state, last_active, last_suspended, metadata
		FROM sessions;
	`

	rows, err := h.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query sessions: %w", err)
	}
	defer rows.Close()

	sessions := make(map[string]*SessionInfo)
	for rows.Next() {
		var (
			id            string
			clientID      string
			state         int
			lastActive    time.Time
			lastSuspended time.Time
			metadataStr   string
		)

		if err := rows.Scan(&id, &clientID, &state, &lastActive, &lastSuspended, &metadataStr); err != nil {
			return nil, fmt.Errorf("failed to scan session row: %w", err)
		}

		var metadata map[string]string
		if err := json.Unmarshal([]byte(metadataStr), &metadata); err != nil {
			return nil, fmt.Errorf("failed to unmarshal metadata: %w", err)
		}

		sessions[id] = &SessionInfo{
			ID:            id,
			ClientID:      clientID,
			State:         BridgeSessionState(state),
			LastActive:    lastActive,
			LastSuspended: lastSuspended,
			Metadata:      metadata,
		}
	}

	return sessions, nil
}

// DeleteSession deletes a session from the database
func (h *SQLiteHook) DeleteSession(sessionID string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.db == nil {
		return fmt.Errorf("database not initialized")
	}

	query := `DELETE FROM sessions WHERE id = ?;`
	_, err := h.db.Exec(query, sessionID)
	if err != nil {
		return fmt.Errorf("failed to delete session: %w", err)
	}

	return nil
}

// CleanupStaleSessions removes sessions that have been suspended longer than the timeout
func (h *SQLiteHook) CleanupStaleSessions(timeout time.Duration) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.db == nil {
		return fmt.Errorf("database not initialized")
	}

	query := `
		DELETE FROM sessions
		WHERE state = ? AND
		      (julianday('now') - julianday(last_suspended)) * 24 * 60 * 60 > ?;
	`

	_, err := h.db.Exec(query, int(BridgeSessionStateSuspended), int(timeout.Seconds()))
	if err != nil {
		return fmt.Errorf("failed to cleanup stale sessions: %w", err)
	}

	return nil
}
