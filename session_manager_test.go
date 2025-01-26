package bridge

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

type testBridges struct {
	serverBridge *MQTTNetBridge
	clientBridge *MQTTNetBridge
}

func setupTestSessionManager(t *testing.T) (*testBridges, func()) {
	// Setup logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	rootTopic := "/test"

	// Create server MQTT client
	serverOpts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("session-test-server")

	serverClient := mqtt.NewClient(serverOpts)
	if token := serverClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect server to MQTT: %v", token.Error())
	}

	// Create client MQTT client
	clientOpts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("session-test-client")

	clientClient := mqtt.NewClient(clientOpts)
	if token := clientClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect client to MQTT: %v", token.Error())
	}

	// Create server bridge
	serverBridge := NewMQTTNetBridge(serverClient, "session-test-server",
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(1),
	)

	// Create client bridge
	clientBridge := NewMQTTNetBridge(clientClient, "session-test-client",
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(1),
	)

	// Small delay to ensure subscriptions are established
	time.Sleep(100 * time.Millisecond)

	cleanup := func() {
		serverBridge.Close()
		clientBridge.Close()
		serverClient.Disconnect(250)
		clientClient.Disconnect(250)
	}

	// Start server goroutine to accept connections
	serverErr := make(chan error, 1)
	go func() {
		for {
			conn, err := serverBridge.Accept()
			if err != nil {
				serverErr <- err
				return
			}
			go echoHandler(t, conn)
		}
	}()

	return &testBridges{
		serverBridge: serverBridge,
		clientBridge: clientBridge,
	}, cleanup
}

func TestSessionLifecycle(t *testing.T) {

	// Test session creation and retrieval
	t.Run("Session Creation and Retrieval", func(t *testing.T) {
		bridges, cleanup := setupTestSessionManager(t)
		// Establish connection
		ctx := context.Background()
		conn, err := bridges.clientBridge.Dial(ctx, "session-test-server")
		assert.NoError(t, err)
		defer conn.Close()

		// Get session ID
		sessionID := conn.(*MQTTNetBridgeConn).sessionID

		// Verify session exists and is active
		session, exists := bridges.serverBridge.sessionManager.GetSession(sessionID)
		assert.True(t, exists)
		assert.NotNil(t, session)
		assert.Equal(t, sessionID, session.ID)
		assert.Equal(t, BridgeSessionStateActive, session.State)

		cleanup()
	})

	// Test session suspension and resumption
	t.Run("Session Suspend and Resume", func(t *testing.T) {
		bridges, cleanup := setupTestSessionManager(t)

		// Establish connection
		ctx := context.Background()
		conn, err := bridges.clientBridge.Dial(ctx, "session-test-server")
		assert.NoError(t, err)
		sessionID := conn.(*MQTTNetBridgeConn).sessionID

		// check if session is active
		session, exists := bridges.clientBridge.sessionManager.GetSession(sessionID)
		assert.True(t, exists)
		assert.Equal(t, BridgeSessionStateActive, session.State)

		// Wait for session to be suspended
		time.Sleep(100 * time.Millisecond)

		// Verify session is active
		session, exists = bridges.serverBridge.sessionManager.GetSession(sessionID)
		assert.True(t, exists)
		assert.Equal(t, BridgeSessionStateActive, session.State)

		// Test suspension
		err = bridges.clientBridge.SuspendSession(sessionID)
		assert.NoError(t, err)

		time.Sleep(100 * time.Millisecond)

		// Verify connection is closed
		_, err = conn.Write([]byte("test"))
		assert.Error(t, err)

		// Resume session
		conn2, err := bridges.clientBridge.ResumeSession(ctx, "session-test-server", sessionID)
		assert.NoError(t, err)
		defer conn2.Close()

		// Verify session is active
		session, exists = bridges.serverBridge.sessionManager.GetSession(sessionID)
		assert.True(t, exists)
		assert.Equal(t, BridgeSessionStateActive, session.State)

		// Verify new connection works
		_, err = conn2.Write([]byte("test"))
		assert.NoError(t, err)

		cleanup()
	})

	// Test session cleanup
	t.Run("Session Cleanup", func(t *testing.T) {
		bridges, cleanup := setupTestSessionManager(t)
		defer cleanup()
		// Establish connection
		ctx := context.Background()
		conn, err := bridges.clientBridge.Dial(ctx, "session-test-server", WithSessionTimeout(2*time.Second))
		assert.NoError(t, err)
		sessionID := conn.(*MQTTNetBridgeConn).sessionID

		// wait for session to be cleaned up
		time.Sleep(3 * time.Second)

		// Verify session is cleaned up
		_, exists := bridges.serverBridge.sessionManager.GetSession(sessionID)
		assert.False(t, exists)

		// Verify we can't resume cleaned up session
		_, err = bridges.clientBridge.ResumeSession(ctx, "session-test-server", sessionID)
		assert.Error(t, err)

		// Verify we can't suspend cleaned up session
		err = bridges.clientBridge.SuspendSession(sessionID)
		assert.Error(t, err)
	})

	// Test multiple sessions
	t.Run("Multiple Sessions", func(t *testing.T) {
		bridges, cleanup := setupTestSessionManager(t)
		defer cleanup()

		// Establish first connection
		ctx := context.Background()
		conn1, err := bridges.clientBridge.Dial(ctx, "session-test-server")
		assert.NoError(t, err)
		sessionID1 := conn1.(*MQTTNetBridgeConn).sessionID

		// Establish second connection
		conn2, err := bridges.clientBridge.Dial(ctx, "session-test-server")
		assert.NoError(t, err)
		defer conn2.Close()
		sessionID2 := conn2.(*MQTTNetBridgeConn).sessionID

		// Verify sessions are different
		assert.NotEqual(t, sessionID1, sessionID2)

		// Suspend first session
		err = bridges.clientBridge.SuspendSession(sessionID1)
		assert.NoError(t, err)

		// Verify first session is suspended and second is active
		session1, _ := bridges.serverBridge.sessionManager.GetSession(sessionID1)
		session2, _ := bridges.serverBridge.sessionManager.GetSession(sessionID2)
		assert.Equal(t, BridgeSessionStateSuspended, session1.State)
		assert.Equal(t, BridgeSessionStateActive, session2.State)

		// Resume first session
		conn1Resumed, err := bridges.clientBridge.ResumeSession(ctx, "session-test-server", sessionID1)
		assert.NoError(t, err)
		defer conn1Resumed.Close()

		// Verify both sessions are active
		session1, _ = bridges.serverBridge.sessionManager.GetSession(sessionID1)
		session2, _ = bridges.serverBridge.sessionManager.GetSession(sessionID2)
		assert.Equal(t, BridgeSessionStateActive, session1.State)
		assert.Equal(t, BridgeSessionStateActive, session2.State)
	})

	// Test session cleanup on bridge close
	t.Run("Session Cleanup on Bridge Close", func(t *testing.T) {
		bridges, cleanup := setupTestSessionManager(t)
		defer cleanup()

		// Create a new bridge instance to test close behavior
		serverOpts := mqtt.NewClientOptions().
			AddBroker("tcp://localhost:1883").
			SetClientID("session-test-server-2")

		serverClient := mqtt.NewClient(serverOpts)
		if token := serverClient.Connect(); token.Wait() && token.Error() != nil {
			t.Fatalf("Failed to connect server to MQTT: %v", token.Error())
		}
		defer serverClient.Disconnect(250)

		testBridge := NewMQTTNetBridge(serverClient, "session-test-server-2",
			WithRootTopic("/test"),
			WithLogger(bridges.serverBridge.logger),
			WithQoS(1),
		)

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
		conn1, err := bridges.clientBridge.Dial(ctx, "session-test-server-2")
		assert.NoError(t, err)
		defer conn1.Close()
		sessionID1 := conn1.(*MQTTNetBridgeConn).sessionID

		conn2, err := bridges.clientBridge.Dial(ctx, "session-test-server-2")
		assert.NoError(t, err)
		defer conn2.Close()
		sessionID2 := conn2.(*MQTTNetBridgeConn).sessionID

		// Manually suspend one session to verify it remains suspended
		err = bridges.clientBridge.SuspendSession(sessionID1)
		assert.NoError(t, err)

		// Small delay to allow suspension to complete
		time.Sleep(100 * time.Millisecond)

		// Verify initial session states
		session1, exists := testBridge.sessionManager.GetSession(sessionID1)
		assert.True(t, exists)
		assert.Equal(t, BridgeSessionStateSuspended, session1.State)

		session2, exists := testBridge.sessionManager.GetSession(sessionID2)
		assert.True(t, exists)
		assert.Equal(t, BridgeSessionStateActive, session2.State)

		// Close the bridge
		err = testBridge.Close()
		assert.NoError(t, err)

		// Small delay to allow for session updates
		time.Sleep(100 * time.Millisecond)

		// Verify all sessions are now suspended
		sessions := testBridge.sessionManager.GetAllSessions()
		for id, session := range sessions {
			if id == sessionID1 {
				// Already suspended session should remain suspended
				assert.Equal(t, BridgeSessionStateSuspended, session.State)
			} else {
				// All other sessions should now be suspended
				assert.Equal(t, BridgeSessionStateSuspended, session.State)
			}
			// Verify connections are closed
			assert.Nil(t, session.Connection)
		}
	})
}

func TestSessionErrors(t *testing.T) {
	bridges, cleanup := setupTestSessionManager(t)
	defer cleanup()

	// Start server goroutine to accept connections
	serverErr := make(chan error, 1)
	go func() {
		for {
			conn, err := bridges.serverBridge.Accept()
			if err != nil {
				serverErr <- err
				return
			}
			go echoHandler(t, conn)
		}
	}()

	t.Run("Session Not Found", func(t *testing.T) {
		err := bridges.clientBridge.SuspendSession("non-existent-session")
		assert.Error(t, err)
		assert.IsType(t, &SessionError{}, err)
		assert.Equal(t, "session not found", err.(*SessionError).Message)
	})

	t.Run("Already Active Session", func(t *testing.T) {
		// Establish first connection
		ctx := context.Background()
		conn, err := bridges.clientBridge.Dial(ctx, "session-test-server")
		assert.NoError(t, err)
		defer conn.Close()
		sessionID := conn.(*MQTTNetBridgeConn).sessionID

		// Try to resume the already active session
		_, err = bridges.clientBridge.ResumeSession(ctx, "session-test-server", sessionID)
		assert.Error(t, err)
		assert.IsType(t, &SessionError{}, err)
		assert.Equal(t, "session is already active", err.(*SessionError).Message)

		// Try to create a new connection with same session ID
		_, err = bridges.clientBridge.Dial(ctx, "session-test-server", WithSessionID(sessionID))
		assert.Error(t, err)
		assert.IsType(t, &SessionError{}, err)
		assert.Equal(t, "session is already active", err.(*SessionError).Message)
	})

	t.Run("Invalid State Transitions", func(t *testing.T) {
		// Establish connection
		ctx := context.Background()
		conn, err := bridges.clientBridge.Dial(ctx, "session-test-server")
		assert.NoError(t, err)
		sessionID := conn.(*MQTTNetBridgeConn).sessionID

		// Try to resume active session
		_, err = bridges.clientBridge.ResumeSession(ctx, "session-test-server", sessionID)
		assert.Error(t, err)

		// Suspend session
		err = bridges.clientBridge.SuspendSession(sessionID)
		assert.NoError(t, err)

		// Try to suspend already suspended session
		err = bridges.clientBridge.SuspendSession(sessionID)
		assert.Error(t, err)
	})
}

func TestSessionDisconnect(t *testing.T) {
	bridges, cleanup := setupTestSessionManager(t)
	defer cleanup()

	// Start server goroutine to accept connections
	serverErr := make(chan error, 1)
	go func() {
		for {
			conn, err := bridges.serverBridge.Accept()
			if err != nil {
				serverErr <- err
				return
			}
			go echoHandler(t, conn)
		}
	}()

	t.Run("Session Disconnect", func(t *testing.T) {
		// Establish connection
		ctx := context.Background()
		conn, err := bridges.clientBridge.Dial(ctx, "session-test-server", WithSessionTimeout(2*time.Second))
		assert.NoError(t, err)
		sessionID := conn.(*MQTTNetBridgeConn).sessionID

		// Disconnect session
		err = bridges.clientBridge.DisconnectSession(sessionID)
		assert.NoError(t, err)

		// Verify session is closed on client bridge
		_, exists := bridges.clientBridge.sessionManager.GetSession(sessionID)
		assert.False(t, exists)

		// Wait for session to be closed on server bridge
		time.Sleep(100 * time.Millisecond)

		// Verify session is Suspended on server bridge
		session, exists := bridges.serverBridge.sessionManager.GetSession(sessionID)
		assert.True(t, exists)
		assert.Equal(t, BridgeSessionStateSuspended, session.State)

		// Try to resume disconnected session before timeout
		_, err = bridges.clientBridge.ResumeSession(ctx, "session-test-server", sessionID)
		assert.NoError(t, err)

		// Try to resume disconnected session after timeout
		time.Sleep(3 * time.Second)
		_, err = bridges.clientBridge.ResumeSession(ctx, "session-test-server", sessionID)
		assert.Error(t, err)
	})

	t.Run("Quick Disconnect and Resume", func(t *testing.T) {
		// Establish connection
		ctx := context.Background()
		conn, err := bridges.clientBridge.Dial(ctx, "session-test-server")
		assert.NoError(t, err)
		sessionID := conn.(*MQTTNetBridgeConn).sessionID

		// Close connection (simulating disconnect)
		conn.Close()

		// Try to resume quickly
		time.Sleep(100 * time.Millisecond)
		conn2, err := bridges.clientBridge.ResumeSession(ctx, "session-test-server", sessionID)
		assert.NoError(t, err)
		defer conn2.Close()

		// Verify session is active
		session, exists := bridges.serverBridge.sessionManager.GetSession(sessionID)
		assert.True(t, exists)
		assert.Equal(t, BridgeSessionStateActive, session.State)
	})

	t.Run("Resume Session Without an Active Connection", func(t *testing.T) {
		// Establish connection
		ctx := context.Background()
		_, err := bridges.clientBridge.Dial(ctx, "session-test-server", WithSessionID("test-session"))
		assert.Error(t, err)
		assert.IsType(t, ErrSessionActive, err)

		// Verify session is active
		_, exists := bridges.serverBridge.sessionManager.GetSession("test-session")
		assert.False(t, exists)
	})
}

func echoHandler(t *testing.T, conn net.Conn) {
	defer conn.Close()
	buf := make([]byte, 1024)
	for {
		_, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				t.Logf("Read error: %v", err)
			}
			return
		}
	}
}
