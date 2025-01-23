package bridge

import (
	"os"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestSQLiteHook(t *testing.T) {
	// Create a temporary database file
	tmpFile := "test_sessions.db"
	defer os.Remove(tmpFile)

	// Create logger
	logger, _ := zap.NewDevelopment()

	// Create hook
	hook := &SQLiteHook{
		logger: logger,
	}

	// Test initialization
	t.Run("Init", func(t *testing.T) {
		err := hook.Init(&SQLiteHookOptions{
			DBFile: tmpFile,
		})
		if err != nil {
			t.Fatalf("Failed to initialize SQLite hook: %v", err)
		}
	})

	// Test session operations
	t.Run("Session Operations", func(t *testing.T) {
		// Create a test session
		session := &SessionInfo{
			ID:            "test-session",
			ClientID:      "test-client",
			State:         BridgeSessionStateActive,
			LastActive:    time.Now(),
			LastSuspended: time.Time{},
			Metadata: map[string]string{
				"key1": "value1",
				"key2": "value2",
			},
		}

		// Test SaveSession
		err := hook.SaveSession(session)
		if err != nil {
			t.Fatalf("Failed to save session: %v", err)
		}

		// Test LoadSession
		loaded, err := hook.LoadSession(session.ID)
		if err != nil {
			t.Fatalf("Failed to load session: %v", err)
		}
		if loaded == nil {
			t.Fatal("Expected to load session but got nil")
		}

		// Compare session data
		if loaded.ID != session.ID {
			t.Errorf("Session ID mismatch. Got %s, want %s", loaded.ID, session.ID)
		}
		if loaded.ClientID != session.ClientID {
			t.Errorf("Client ID mismatch. Got %s, want %s", loaded.ClientID, session.ClientID)
		}
		if loaded.State != session.State {
			t.Errorf("State mismatch. Got %v, want %v", loaded.State, session.State)
		}
		if len(loaded.Metadata) != len(session.Metadata) {
			t.Errorf("Metadata length mismatch. Got %d, want %d", len(loaded.Metadata), len(session.Metadata))
		}
		for k, v := range session.Metadata {
			if loaded.Metadata[k] != v {
				t.Errorf("Metadata value mismatch for key %s. Got %s, want %s", k, loaded.Metadata[k], v)
			}
		}

		// Test LoadAllSessions
		sessions, err := hook.LoadAllSessions()
		if err != nil {
			t.Fatalf("Failed to load all sessions: %v", err)
		}
		if len(sessions) != 1 {
			t.Errorf("Expected 1 session, got %d", len(sessions))
		}

		// Test session state changes
		session.State = BridgeSessionStateSuspended
		session.LastSuspended = time.Now()
		err = hook.SaveSession(session)
		if err != nil {
			t.Fatalf("Failed to update session: %v", err)
		}

		loaded, err = hook.LoadSession(session.ID)
		if err != nil {
			t.Fatalf("Failed to load updated session: %v", err)
		}
		if loaded.State != BridgeSessionStateSuspended {
			t.Errorf("State not updated. Got %v, want %v", loaded.State, BridgeSessionStateSuspended)
		}

		// Test cleanup of stale sessions
		time.Sleep(1 * time.Second) // Ensure some time has passed
		err = hook.CleanupStaleSessions(500 * time.Millisecond)
		if err != nil {
			t.Fatalf("Failed to cleanup stale sessions: %v", err)
		}

		// Verify session was cleaned up
		sessions, err = hook.LoadAllSessions()
		if err != nil {
			t.Fatalf("Failed to load sessions after cleanup: %v", err)
		}
		if len(sessions) != 0 {
			t.Errorf("Expected 0 sessions after cleanup, got %d", len(sessions))
		}
	})

	// Test session deletion
	t.Run("DeleteSession", func(t *testing.T) {
		// Create and save a test session
		session := &SessionInfo{
			ID:       "test-delete-session",
			ClientID: "test-client",
			State:    BridgeSessionStateActive,
		}

		err := hook.SaveSession(session)
		if err != nil {
			t.Fatalf("Failed to save session: %v", err)
		}

		// Delete the session
		err = hook.DeleteSession(session.ID)
		if err != nil {
			t.Fatalf("Failed to delete session: %v", err)
		}

		// Verify session was deleted
		loaded, err := hook.LoadSession(session.ID)
		if err != nil {
			t.Fatalf("Failed to check deleted session: %v", err)
		}
		if loaded != nil {
			t.Error("Session still exists after deletion")
		}
	})

	// Test hook cleanup
	t.Run("Stop", func(t *testing.T) {
		err := hook.Stop()
		if err != nil {
			t.Fatalf("Failed to stop SQLite hook: %v", err)
		}
	})
}
