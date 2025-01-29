package hooks

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	bridge "github.com/golain-io/mqtt-bridge"
)

func TestSQLiteHook(t *testing.T) {
	// Setup logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// Helper function to create a new hook instance
	createHook := func(t *testing.T, dbPath string) *SQLiteHook {
		hook := NewSQLiteHook(logger)
		err := hook.Init(&SQLiteConfig{
			DBPath: dbPath,
		})
		require.NoError(t, err)
		return hook
	}

	// Helper function to query sessions directly from DB
	getStoredSessions := func(db *sql.DB) (map[string]*bridge.SessionInfo, error) {
		rows, err := db.Query(`
			SELECT session_id, state, client_id, last_active, last_suspended, metadata 
			FROM sessions 
			WHERE state != 'closed'`)
		if err != nil {
			return nil, err
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
				return nil, err
			}

			if err := json.Unmarshal(metadataJSON, &metadata); err != nil {
				return nil, err
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
		return sessions, nil
	}

	t.Run("Session Lifecycle", func(t *testing.T) {
		dbPath := "test_lifecycle.db"
		defer os.Remove(dbPath)

		hook := createHook(t, dbPath)
		defer hook.Stop()

		// Create new session
		session1 := &bridge.SessionInfo{
			ID:         "session1",
			ClientID:   "client1",
			State:      bridge.BridgeSessionStateActive,
			LastActive: time.Now(),
			Metadata:   make(map[string]string),
		}

		// Test session creation
		err := hook.OnSessionCreated(session1)
		require.NoError(t, err)

		// Verify session creation
		sessions, err := getStoredSessions(hook.db)
		require.NoError(t, err)
		storedSession, exists := sessions["session1"]
		require.True(t, exists)
		assert.Equal(t, "active", stateToString(storedSession.State))
		assert.Equal(t, "client1", storedSession.ClientID)

		// Test session suspension
		err = hook.OnSessionSuspended(session1)
		require.NoError(t, err)

		// Verify suspension
		sessions, err = getStoredSessions(hook.db)
		require.NoError(t, err)
		storedSession, exists = sessions["session1"]
		require.True(t, exists)
		assert.Equal(t, "suspended", stateToString(storedSession.State))
		assert.False(t, storedSession.LastSuspended.IsZero())

		// Test session resumption
		err = hook.OnSessionResumed(session1)
		require.NoError(t, err)

		// Verify resumption
		sessions, err = getStoredSessions(hook.db)
		require.NoError(t, err)
		storedSession, exists = sessions["session1"]
		require.True(t, exists)
		assert.Equal(t, "active", stateToString(storedSession.State))
		assert.False(t, storedSession.LastActive.IsZero())

		// Test session disconnection
		err = hook.OnSessionDisconnected(session1)
		require.NoError(t, err)

		// Verify session is not returned (as it's closed)
		sessions, err = getStoredSessions(hook.db)
		require.NoError(t, err)
		_, exists = sessions["session1"]
		assert.False(t, exists)
	})

	t.Run("Database Persistence", func(t *testing.T) {
		dbPath := "test_persistence.db"
		defer os.Remove(dbPath)

		// Create first hook instance and session
		hook := createHook(t, dbPath)
		session := &bridge.SessionInfo{
			ID:         "session2",
			ClientID:   "client2",
			State:      bridge.BridgeSessionStateActive,
			LastActive: time.Now(),
			Metadata:   make(map[string]string),
		}
		err := hook.OnSessionCreated(session)
		require.NoError(t, err)

		// Stop the first hook
		err = hook.Stop()
		require.NoError(t, err)

		// Create a new hook instance with the same database
		newHook := createHook(t, dbPath)
		defer newHook.Stop()

		// Verify session is still there
		sessions, err := getStoredSessions(newHook.db)
		require.NoError(t, err)
		storedSession, exists := sessions["session2"]
		require.True(t, exists)
		assert.Equal(t, "active", stateToString(storedSession.State))
		assert.Equal(t, "client2", storedSession.ClientID)
	})

	t.Run("Concurrent Access", func(t *testing.T) {
		dbPath := "test_concurrent.db"
		defer os.Remove(dbPath)

		hook := createHook(t, dbPath)
		defer hook.Stop()

		// Create multiple goroutines accessing the hook simultaneously
		const numGoroutines = 10
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func(id int) {
				defer wg.Done()
				sessionID := fmt.Sprintf("concurrent_session_%d", id)
				clientID := fmt.Sprintf("client%d", id)

				// Create session
				session := &bridge.SessionInfo{
					ID:         sessionID,
					ClientID:   clientID,
					State:      bridge.BridgeSessionStateActive,
					LastActive: time.Now(),
					Metadata:   make(map[string]string),
				}

				// Test lifecycle
				err := hook.OnSessionCreated(session)
				assert.NoError(t, err)

				time.Sleep(10 * time.Millisecond)

				err = hook.OnSessionSuspended(session)
				assert.NoError(t, err)

				err = hook.OnSessionResumed(session)
				assert.NoError(t, err)
			}(i)
		}

		// Wait for all goroutines to complete
		wg.Wait()

		// Verify all sessions are stored correctly
		sessions, err := getStoredSessions(hook.db)
		require.NoError(t, err)

		// Each session should be in active state
		for i := 0; i < numGoroutines; i++ {
			sessionID := fmt.Sprintf("concurrent_session_%d", i)
			session, exists := sessions[sessionID]
			require.True(t, exists, "Session %s should exist", sessionID)
			assert.Equal(t, "active", stateToString(session.State), "Session %s should be active", sessionID)
		}
	})
}

// echoHandler echoes back any data received on the connection
func echoHandler(t *testing.T, conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				t.Logf("Error reading from connection: %v", err)
			}
			return
		}
		_, err = conn.Write(buf[:n])
		if err != nil {
			t.Logf("Error writing to connection: %v", err)
			return
		}
	}
}

func TestBridgeSessionLoading(t *testing.T) {
	// Setup logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// Create temp SQLite database and hook
	dbPath := "test_bridge_loading.db"
	defer os.Remove(dbPath)

	hook := NewSQLiteHook(logger)
	err := hook.Init(&SQLiteConfig{
		DBPath: dbPath,
	})
	require.NoError(t, err)
	defer hook.Stop()

	// Create test sessions with different states
	testSessions := []struct {
		session  *bridge.SessionInfo
		actions  []string // sequence of actions to perform
		endState string   // expected final state
	}{
		{
			session: &bridge.SessionInfo{
				ID:         "session1",
				ClientID:   "client1",
				State:      bridge.BridgeSessionStateActive,
				LastActive: time.Now(),
				Metadata:   make(map[string]string),
			},
			actions:  []string{"create"},
			endState: "active",
		},
		{
			session: &bridge.SessionInfo{
				ID:         "session2",
				ClientID:   "client2",
				State:      bridge.BridgeSessionStateActive,
				LastActive: time.Now(),
				Metadata:   make(map[string]string),
			},
			actions:  []string{"create", "suspend"},
			endState: "suspended",
		},
		{
			session: &bridge.SessionInfo{
				ID:         "session3",
				ClientID:   "client3",
				State:      bridge.BridgeSessionStateActive,
				LastActive: time.Now(),
				Metadata:   make(map[string]string),
			},
			actions:  []string{"create", "suspend", "resume"},
			endState: "active",
		},
	}

	// Create sessions in database with their respective states
	for _, ts := range testSessions {
		for _, action := range ts.actions {
			var err error
			switch action {
			case "create":
				err = hook.OnSessionCreated(ts.session)
			case "suspend":
				err = hook.OnSessionSuspended(ts.session)
			case "resume":
				err = hook.OnSessionResumed(ts.session)
			}
			require.NoError(t, err)
		}
	}

	// Create a client bridge to test connections
	clientOpts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-client")

	clientClient := mqtt.NewClient(clientOpts)
	token := clientClient.Connect()
	require.True(t, token.WaitTimeout(5*time.Second))
	require.NoError(t, token.Error())
	defer clientClient.Disconnect(250)

	clientBridge := bridge.NewMQTTNetBridge(clientClient, "bridge-test-client",
		bridge.WithRootTopic("/test/sqlite"),
		bridge.WithLogger(logger),
		bridge.WithQoS(1),
	)
	defer clientBridge.Close()

	t.Run("Session Loading Without Hook", func(t *testing.T) {
		// Create MQTT client for bridge
		mqttOpts := mqtt.NewClientOptions().
			AddBroker("tcp://localhost:1883").
			SetClientID("bridge-test-sqlite")

		mqttClient := mqtt.NewClient(mqttOpts)
		token := mqttClient.Connect()
		require.True(t, token.WaitTimeout(5*time.Second))
		require.NoError(t, token.Error())
		defer mqttClient.Disconnect(250)

		// Create bridge without hook first
		bridgeID := "test-bridge-sqlite"
		rootTopic := "/test/sqlite"
		b := bridge.NewMQTTNetBridge(mqttClient, bridgeID,
			bridge.WithRootTopic(rootTopic),
			bridge.WithLogger(logger),
			bridge.WithQoS(1),
		)
		defer b.Close()

		// Start server goroutine to accept connections
		go func() {
			for {
				conn, err := b.Accept()
				if err != nil {
					return
				}
				go echoHandler(t, conn)
			}
		}()

		// Try to connect before adding hook - should fail
		ctx := context.Background()
		_, err = clientBridge.Dial(ctx, bridgeID, bridge.WithSessionID("session1"))
		assert.Error(t, err, "Should not be able to connect with session1 before adding hook")
	})

	t.Run("Session Loading With Hook", func(t *testing.T) {
		// Create MQTT client for bridge
		mqttOpts := mqtt.NewClientOptions().
			AddBroker("tcp://localhost:1883").
			SetClientID("bridge-test-sqlite-2")

		mqttClient := mqtt.NewClient(mqttOpts)
		token := mqttClient.Connect()
		require.True(t, token.WaitTimeout(5*time.Second))
		require.NoError(t, token.Error())
		defer mqttClient.Disconnect(250)

		// Create bridge with hook
		bridgeID := "test-bridge-sqlite-2"
		rootTopic := "/test/sqlite"
		b := bridge.NewMQTTNetBridge(mqttClient, bridgeID,
			bridge.WithRootTopic(rootTopic),
			bridge.WithLogger(logger),
			bridge.WithQoS(1),
		)
		defer b.Close()

		// Start server goroutine to accept connections
		go func() {
			for {
				conn, err := b.Accept()
				if err != nil {
					return
				}
				go echoHandler(t, conn)
			}
		}()

		// Add hook to bridge
		err = b.AddHook(hook, &SQLiteConfig{
			DBPath: dbPath,
		})
		require.NoError(t, err)

		// Small delay to allow sessions to load
		time.Sleep(100 * time.Millisecond)

		ctx := context.Background()

		// Try to connect with session1 (active) - should fail as it's not suspended
		conn1, err := clientBridge.Dial(ctx, bridgeID, bridge.WithSessionID("session1"))
		assert.Error(t, err, "Should not be able to connect with session1")
		if conn1 != nil {
			conn1.Close()
		}

		// Try to connect with session2 (suspended) - should succeed
		conn2, err := clientBridge.Dial(ctx, bridgeID, bridge.WithSessionID("session2"))
		assert.NoError(t, err, "Should be able to connect with suspended session2")
		if conn2 != nil {
			conn2.Close()
		}

		// Try to connect with session3 (active) - should fail
		conn3, err := clientBridge.Dial(ctx, bridgeID, bridge.WithSessionID("session3"))
		assert.Error(t, err, "Should not be able to connect with session3")
		if conn3 != nil {
			conn3.Close()
		}
	})

	t.Run("Session State After Bridge Restart", func(t *testing.T) {
		// Create a new bridge instance
		serverOpts := mqtt.NewClientOptions().
			AddBroker("tcp://localhost:1883").
			SetClientID("bridge-test-server-restart")

		serverClient := mqtt.NewClient(serverOpts)
		token := serverClient.Connect()
		require.True(t, token.WaitTimeout(5*time.Second))
		require.NoError(t, token.Error())
		defer serverClient.Disconnect(250)

		testBridge := bridge.NewMQTTNetBridge(serverClient, "test-bridge-restart",
			bridge.WithRootTopic("/test/sqlite"),
			bridge.WithLogger(logger),
			bridge.WithQoS(1),
		)

		// Add hook to bridge
		err = testBridge.AddHook(hook, &SQLiteConfig{
			DBPath: dbPath,
		})
		require.NoError(t, err)

		// Start server goroutine to accept connections
		go func() {
			for {
				conn, err := testBridge.Accept()
				if err != nil {
					return
				}
				go echoHandler(t, conn)
			}
		}()

		// Small delay to ensure subscriptions are established
		time.Sleep(100 * time.Millisecond)

		// Create multiple active sessions
		ctx := context.Background()
		conn1, err := clientBridge.Dial(ctx, "test-bridge-restart")
		assert.NoError(t, err)
		defer conn1.Close()
		sessionID1 := conn1.(*bridge.MQTTNetBridgeConn).SessionID()

		conn2, err := clientBridge.Dial(ctx, "test-bridge-restart")
		assert.NoError(t, err)
		defer conn2.Close()
		sessionID2 := conn2.(*bridge.MQTTNetBridgeConn).SessionID()

		// Manually suspend one session
		err = clientBridge.SuspendSession(sessionID1)
		assert.NoError(t, err)

		// Small delay to allow suspension to complete
		time.Sleep(100 * time.Millisecond)

		// Query database directly to verify initial states
		rows, err := hook.db.Query(`
			SELECT session_id, state FROM sessions 
			WHERE session_id IN (?, ?)`, sessionID1, sessionID2)
		require.NoError(t, err)
		defer rows.Close()

		states := make(map[string]string)
		for rows.Next() {
			var sessionID, state string
			err := rows.Scan(&sessionID, &state)
			require.NoError(t, err)
			states[sessionID] = state
		}

		assert.Equal(t, "suspended", states[sessionID1])
		assert.Equal(t, "active", states[sessionID2])

		// Close the bridge
		err = testBridge.Close()
		assert.NoError(t, err)

		// Small delay to allow session updates in database
		time.Sleep(100 * time.Millisecond)

		// Query database to verify all sessions are suspended
		rows, err = hook.db.Query(`
			SELECT session_id, state FROM sessions 
			WHERE session_id IN (?, ?)`, sessionID1, sessionID2)
		require.NoError(t, err)
		defer rows.Close()

		states = make(map[string]string)
		for rows.Next() {
			var sessionID, state string
			err := rows.Scan(&sessionID, &state)
			require.NoError(t, err)
			states[sessionID] = state
		}

		for id, state := range states {
			assert.Equal(t, "suspended", state,
				"Session %s should be suspended after bridge close", id)
		}

		// Create a new bridge instance with the same hook
		serverOpts = mqtt.NewClientOptions().
			AddBroker("tcp://localhost:1883").
			SetClientID("bridge-test-server-restart-2")

		serverClient = mqtt.NewClient(serverOpts)
		token = serverClient.Connect()
		require.True(t, token.WaitTimeout(5*time.Second))
		require.NoError(t, token.Error())
		defer serverClient.Disconnect(250)

		newBridge := bridge.NewMQTTNetBridge(serverClient, "test-bridge-restart-2",
			bridge.WithRootTopic("/test/sqlite"),
			bridge.WithLogger(logger),
			bridge.WithQoS(1),
		)

		// Add hook to new bridge
		err = newBridge.AddHook(hook, &SQLiteConfig{
			DBPath: dbPath,
		})
		require.NoError(t, err)

		// Small delay to allow sessions to load
		time.Sleep(100 * time.Millisecond)

		// Try to resume a session - should work since it's suspended
		conn3, err := clientBridge.ResumeSession(ctx, "test-bridge-restart-2", sessionID1)
		assert.NoError(t, err, "Should be able to resume suspended session")
		if conn3 != nil {
			conn3.Close()
		}
	})
}

// Helper function to convert BridgeSessionState to string
func stateToString(state bridge.BridgeSessionState) string {
	switch state {
	case bridge.BridgeSessionStateActive:
		return "active"
	case bridge.BridgeSessionStateSuspended:
		return "suspended"
	case bridge.BridgeSessionStateClosed:
		return "closed"
	default:
		return "unknown"
	}
}
