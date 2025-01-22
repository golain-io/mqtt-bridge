package bridge

import (
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestMQTTBridgeEchoServer(t *testing.T) {
	// Setup logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// MQTT client options for server
	serverOpts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-server")

	// Create and connect server MQTT client
	serverClient := mqtt.NewClient(serverOpts)
	if token := serverClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect server to MQTT: %v", token.Error())
	}
	defer serverClient.Disconnect(250)

	// Create bridge listener
	serverBridgeID := "test-server"
	rootTopic := "/vedant"
	listener := NewMQTTNetBridge(serverClient, serverBridgeID,
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(2),
	)
	listener.AddHook(NewEchoHook(logger), nil)
	defer listener.Close()

	// Channel to signal server is ready
	serverReady := make(chan struct{})
	serverErr := make(chan error, 1)

	// Start echo server in goroutine
	go func() {
		defer close(serverReady)
		conn, err := listener.Accept()
		if err != nil {
			serverErr <- fmt.Errorf("accept error: %v", err)
			return
		}

		serverReady <- struct{}{}

		go func() {
			defer conn.Close()
			handleTestConnection(t, conn)
		}()
	}()

	// MQTT client options for client
	clientOpts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-client")

	// Create and connect client MQTT client
	clientClient := mqtt.NewClient(clientOpts)
	if token := clientClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect client to MQTT: %v", token.Error())
	}
	defer clientClient.Disconnect(250)

	// Create client bridge
	clientBridgeID := "test-client"
	clientBridge := NewMQTTNetBridge(clientClient, clientBridgeID, WithRootTopic(rootTopic))
	defer clientBridge.Close()

	// Connect to the server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := clientBridge.Dial(ctx, serverBridgeID)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	// Increase timeout for server readiness
	select {
	case <-serverReady:
	case err := <-serverErr:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for server to be ready")
	}

	// Add small delay to ensure MQTT subscriptions are fully established
	time.Sleep(500 * time.Millisecond)

	// Test echo functionality
	testCases := []string{
		"Hello, World!",
		"Testing 1,2,3",
		"Special chars: !@#$%^&*()",
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Echo_%s", tc), func(t *testing.T) {
			// Increase operation deadline
			deadline := time.Now().Add(5 * time.Second)
			err = conn.SetWriteDeadline(deadline)
			assert.NoError(t, err)

			// Write test message
			_, err := conn.Write([]byte(tc))
			assert.NoError(t, err)

			// Set read deadline
			err = conn.SetReadDeadline(deadline)
			assert.NoError(t, err)

			// Read response with retry
			buf := make([]byte, 1024)
			var n int
			for retries := 3; retries > 0; retries-- {
				n, err = conn.Read(buf)
				if err == nil {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}
			assert.NoError(t, err)
			assert.Equal(t, tc, string(buf[:n]))
		})
	}
}

func handleTestConnection(t *testing.T, conn io.ReadWriteCloser) {
	// Echo incoming messages
	buf := make([]byte, 1024)
	for {
		// Set read deadline for each iteration
		if tc, ok := conn.(interface{ SetReadDeadline(time.Time) error }); ok {
			tc.SetReadDeadline(time.Now().Add(10 * time.Second))
		}

		n, err := conn.Read(buf)
		if err != nil {
			if err != io.EOF {
				t.Logf("Read error: %v", err)
			}
			return
		}

		// Don't echo control messages
		msgStr := string(buf[:n])
		if strings.HasPrefix(msgStr, disconnectMsg+":") ||
			strings.HasPrefix(msgStr, suspendMsg+":") ||
			strings.HasPrefix(msgStr, resumeMsg+":") {
			t.Logf("Server received control message: %s", msgStr)
			continue
		}

		// Log received data
		t.Logf("Server received: %s", string(buf[:n]))

		// Set write deadline for response
		if tc, ok := conn.(interface{ SetWriteDeadline(time.Time) error }); ok {
			tc.SetWriteDeadline(time.Now().Add(10 * time.Second))
		}

		// Echo back immediately without delay
		_, err = conn.Write(buf[:n])
		if err != nil {
			t.Logf("Write error: %v", err)
			return
		}

		// Log echo response
		t.Logf("Server echoed: %s", string(buf[:n]))
	}
}

// Hook from example/hooks/echo_hook.go
type EchoHook struct {
	logger    *zap.Logger
	isRunning atomic.Bool
	id        string
}

// NewEchoHook creates a new LoggingHook instance
func NewEchoHook(logger *zap.Logger) *EchoHook {
	return &EchoHook{
		logger: logger,
		id:     "echo_hook",
	}
}

// OnMessageReceived logs the received message
func (h *EchoHook) OnMessageReceived(msg []byte) []byte {
	if !h.isRunning.Load() {
		return msg
	}

	h.logger.Info("message received echo",
		zap.ByteString("message", msg),
		zap.String("hook_id", h.id))
	return msg
}

// Provides indicates whether this hook provides the specified functionality
func (h *EchoHook) Provides(b byte) bool {
	return b == OnMessageReceived
}

// Init initializes the hook with the provided configuration
func (h *EchoHook) Init(config any) error {
	if config != nil {
		// You could add configuration handling here
		// For example, if config contains log level or other settings
	}
	h.isRunning.Store(true)
	return nil
}

// Stop gracefully stops the hook
func (h *EchoHook) Stop() error {
	h.isRunning.Store(false)
	return nil
}

// ID returns the unique identifier for this hook
func (h *EchoHook) ID() string {
	return h.id
}

func TestMQTTBridgeSessionManagement(t *testing.T) {
	// Setup logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// MQTT client options for server
	serverOpts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-server")

	// Create and connect server MQTT client
	serverClient := mqtt.NewClient(serverOpts)
	if token := serverClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect server to MQTT: %v", token.Error())
	}
	defer serverClient.Disconnect(250)

	// Create bridge listener
	serverBridgeID := "test-server"
	rootTopic := "/test"
	listener := NewMQTTNetBridge(serverClient, serverBridgeID,
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(1),
	)
	defer listener.Close()

	// Start server goroutine
	serverErr := make(chan error, 1)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				serverErr <- fmt.Errorf("accept error: %v", err)
				return
			}

			// Handle each connection in a goroutine
			go func(conn net.Conn) {
				defer conn.Close()
				buf := make([]byte, 1024)
				for {
					n, err := conn.Read(buf)
					if err != nil {
						if err != io.EOF {
							t.Logf("Server read error: %v", err)
						}
						return
					}
					// Echo back the data
					_, err = conn.Write(buf[:n])
					if err != nil {
						t.Logf("Server write error: %v", err)
						return
					}
				}
			}(conn)
		}
	}()

	// Create client MQTT client
	clientOpts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-client")

	clientClient := mqtt.NewClient(clientOpts)
	if token := clientClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect client to MQTT: %v", token.Error())
	}
	defer clientClient.Disconnect(250)

	// Create client bridge
	clientBridge := NewMQTTNetBridge(clientClient, "test-client",
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(1),
	)
	defer clientBridge.Close()

	// Test cases
	t.Run("Session Suspend and Resume", func(t *testing.T) {
		// 1. Establish initial connection
		conn1, err := clientBridge.Dial(context.Background(), serverBridgeID)
		if err != nil {
			t.Fatalf("Failed to establish initial connection: %v", err)
		}

		// Get session ID from connection
		sessionID := conn1.(*MQTTNetBridgeConn).sessionID

		// Write some data and verify echo
		testData := []byte("hello")
		_, err = conn1.Write(testData)
		if err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}

		// Read echo response
		buf := make([]byte, 1024)
		deadline := time.Now().Add(5 * time.Second)
		conn1.SetReadDeadline(deadline)
		n, err := conn1.Read(buf)
		if err != nil {
			t.Fatalf("Failed to read echo response: %v", err)
		}
		if string(buf[:n]) != string(testData) {
			t.Fatalf("Echo mismatch. Got %s, want %s", string(buf[:n]), string(testData))
		}

		// 2. Suspend the session
		err = clientBridge.SuspendSession(sessionID)
		if err != nil {
			t.Fatalf("Failed to suspend session: %v", err)
		}

		// Verify connection is closed
		_, err = conn1.Read(buf)
		if err == nil {
			t.Fatal("Expected connection to be closed after suspension")
		}

		// 4. Resume the session
		conn2, err := clientBridge.ResumeSession(context.Background(), serverBridgeID, sessionID)
		if err != nil {
			t.Fatalf("Failed to resume session: %v", err)
		}

		// Verify we can write to resumed connection and get echo
		_, err = conn2.Write(testData)
		if err != nil {
			t.Fatalf("Failed to write to resumed connection: %v", err)
		}

		// Read echo response on resumed connection
		deadline = time.Now().Add(5 * time.Second)
		conn2.SetReadDeadline(deadline)
		n, err = conn2.Read(buf)
		if err != nil {
			t.Fatalf("Failed to read echo response on resumed connection: %v", err)
		}
		if string(buf[:n]) != string(testData) {
			t.Fatalf("Echo mismatch on resumed connection. Got %s, want %s", string(buf[:n]), string(testData))
		}

		// Clean up
		conn2.Close()
	})

	t.Run("Session Cleanup", func(t *testing.T) {
		// 1. Establish connection
		conn, err := clientBridge.Dial(context.Background(), serverBridgeID)
		if err != nil {
			t.Fatalf("Failed to establish connection: %v", err)
		}

		sessionID := conn.(*MQTTNetBridgeConn).sessionID

		// 2. Suspend session
		err = clientBridge.SuspendSession(sessionID)
		if err != nil {
			t.Fatalf("Failed to suspend session: %v", err)
		}
		time.Sleep(3 * time.Second) // Wait for cleanup
		// 3. Force cleanup on the server with short timeout
		listener.CleanupStaleSessions(1 * time.Millisecond)

		// 4. Verify session is cleaned up
		listener.sessionsMu.RLock()
		_, exists := listener.sessions[sessionID]
		listener.sessionsMu.RUnlock()
		if exists {
			t.Fatal("Session should have been cleaned up")
		}

		// 5. Verify we can't resume cleaned up session from the client bridge
		_, err = clientBridge.ResumeSession(context.Background(), serverBridgeID, sessionID)
		if err == nil {
			t.Fatal("Expected error when resuming cleaned up session")
		}
	})

	t.Run("Multiple Session Operations", func(t *testing.T) {
		// 1. Create multiple connections
		conn1, err := clientBridge.Dial(context.Background(), serverBridgeID)
		if err != nil {
			t.Fatalf("Failed to establish first connection: %v", err)
		}
		sessionID1 := conn1.(*MQTTNetBridgeConn).sessionID

		conn2, err := clientBridge.Dial(context.Background(), serverBridgeID)
		if err != nil {
			t.Fatalf("Failed to establish second connection: %v", err)
		}
		sessionID2 := conn2.(*MQTTNetBridgeConn).sessionID

		// Verify sessions are different
		if sessionID1 == sessionID2 {
			t.Fatal("Expected different session IDs for different connections")
		}

		// 2. Suspend first session
		err = clientBridge.SuspendSession(sessionID1)
		if err != nil {
			t.Fatalf("Failed to suspend first session: %v", err)
		}

		// 3. Verify second session still works
		testData := []byte("test")
		_, err = conn2.Write(testData)
		if err != nil {
			t.Fatalf("Failed to write to second connection: %v", err)
		}

		// 4. Resume first session
		conn1Resumed, err := clientBridge.ResumeSession(context.Background(), serverBridgeID, sessionID1)
		if err != nil {
			t.Fatalf("Failed to resume first session: %v", err)
		}

		// 5. Verify both connections work
		_, err = conn1Resumed.Write(testData)
		if err != nil {
			t.Fatalf("Failed to write to resumed connection: %v", err)
		}

		_, err = conn2.Write(testData)
		if err != nil {
			t.Fatalf("Failed to write to second connection: %v", err)
		}

		// Clean up
		conn1Resumed.Close()
		conn2.Close()
	})
}

func TestMQTTBridgeSessionExchange(t *testing.T) {
	// Setup logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// Create echo server bridge
	serverOpts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-server")

	serverClient := mqtt.NewClient(serverOpts)
	if token := serverClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect server to MQTT: %v", token.Error())
	}
	defer serverClient.Disconnect(250)

	// Create echo server bridge
	serverBridgeID := "test-server"
	rootTopic := "/test"
	serverBridge := NewMQTTNetBridge(serverClient, serverBridgeID,
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(1),
	)
	serverBridge.AddHook(NewEchoHook(logger), nil)
	defer serverBridge.Close()

	// Start echo server
	serverErr := make(chan error, 1)
	go func() {
		for {
			conn, err := serverBridge.Accept()
			if err != nil {
				serverErr <- fmt.Errorf("accept error: %v", err)
				return
			}
			go handleTestConnection(t, conn)
		}
	}()

	// Create first client bridge
	client1Opts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-client1")

	client1 := mqtt.NewClient(client1Opts)
	if token := client1.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect client1 to MQTT: %v", token.Error())
	}
	defer client1.Disconnect(250)

	bridge1 := NewMQTTNetBridge(client1, "test-client1",
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(1),
	)
	defer bridge1.Close()

	// Create second client bridge
	client2Opts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-client2")

	client2 := mqtt.NewClient(client2Opts)
	if token := client2.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect client2 to MQTT: %v", token.Error())
	}
	defer client2.Disconnect(250)

	bridge2 := NewMQTTNetBridge(client2, "test-client2",
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(1),
	)
	defer bridge2.Close()

	// Test session exchange
	t.Run("Session Exchange", func(t *testing.T) {
		// 1. Create initial connection from bridge1
		conn1, err := bridge1.Dial(context.Background(), serverBridgeID)
		if err != nil {
			t.Fatalf("Failed to establish initial connection: %v", err)
		}

		// Get session ID
		sessionID := conn1.(*MQTTNetBridgeConn).sessionID

		t.Logf("Session ID: %s", sessionID)

		// 2. Verify echo works on bridge1
		testData := []byte("hello from bridge1")
		deadline := time.Now().Add(5 * time.Second)
		conn1.SetDeadline(deadline)

		_, err = conn1.Write(testData)
		if err != nil {
			t.Fatalf("Failed to write data on bridge1: %v", err)
		}

		buf := make([]byte, 1024)
		n, err := conn1.Read(buf)
		if err != nil {
			t.Fatalf("Failed to read echo on bridge1: %v", err)
		}
		if string(buf[:n]) != string(testData) {
			t.Fatalf("Echo mismatch on bridge1. Got %s, want %s", string(buf[:n]), string(testData))
		}

		// 3. Suspend session on bridge1
		err = bridge1.SuspendSession(sessionID)
		if err != nil {
			t.Fatalf("Failed to suspend session on bridge1: %v", err)
		}

		// 4. Resume session on bridge2
		conn2, err := bridge2.ResumeSession(context.Background(), serverBridgeID, sessionID)
		if err != nil {
			t.Fatalf("Failed to resume session on bridge2: %v", err)
		}

		// 5. Verify echo works on bridge2 with resumed session
		testData = []byte("hello from bridge2")
		deadline = time.Now().Add(5 * time.Second)
		conn2.SetDeadline(deadline)

		_, err = conn2.Write(testData)
		if err != nil {
			t.Fatalf("Failed to write data on bridge2: %v", err)
		}

		n, err = conn2.Read(buf)
		if err != nil {
			t.Fatalf("Failed to read echo on bridge2: %v", err)
		}
		if string(buf[:n]) != string(testData) {
			t.Fatalf("Echo mismatch on bridge2. Got %s, want %s", string(buf[:n]), string(testData))
		}

		// 6. Verify original connection is closed
		_, err = conn1.Write([]byte("test"))
		if err == nil {
			t.Fatal("Expected original connection to be closed")
		}

		// Clean up
		conn2.Close()
	})
}

func TestMQTTBridgeDisconnectCleanup(t *testing.T) {
	// Setup logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// MQTT client options for server
	serverOpts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-server")

	// Create and connect server MQTT client
	serverClient := mqtt.NewClient(serverOpts)
	if token := serverClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect server to MQTT: %v", token.Error())
	}
	defer serverClient.Disconnect(250)

	// Create bridge listener
	serverBridgeID := "test-server"
	rootTopic := "/test-disconnect"
	listener := NewMQTTNetBridge(serverClient, serverBridgeID,
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(1),
	)
	defer listener.Close()

	// Start server goroutine
	serverErr := make(chan error, 1)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				serverErr <- fmt.Errorf("accept error: %v", err)
				return
			}
			go handleTestConnection(t, conn)
		}
	}()

	// Create client MQTT client
	clientOpts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-client")

	clientClient := mqtt.NewClient(clientOpts)
	if token := clientClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect client to MQTT: %v", token.Error())
	}
	defer clientClient.Disconnect(250)

	// Create client bridge
	clientBridge := NewMQTTNetBridge(clientClient, "test-client",
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(1),
	)
	defer clientBridge.Close()

	t.Run("Disconnect Cleanup", func(t *testing.T) {
		// 1. Establish initial connection
		conn1, err := clientBridge.Dial(context.Background(), serverBridgeID)
		if err != nil {
			t.Fatalf("Failed to establish initial connection: %v", err)
		}
		sessionID := conn1.(*MQTTNetBridgeConn).sessionID

		// 2. Verify connection works
		testData := []byte("test data")
		_, err = conn1.Write(testData)
		if err != nil {
			t.Fatalf("Failed to write data: %v", err)
		}

		buf := make([]byte, 1024)
		deadline := time.Now().Add(5 * time.Second)
		conn1.SetReadDeadline(deadline)
		n, err := conn1.Read(buf)
		if err != nil {
			t.Fatalf("Failed to read echo response: %v", err)
		}
		if string(buf[:n]) != string(testData) {
			t.Fatalf("Echo mismatch. Got %s, want %s", string(buf[:n]), string(testData))
		}

		// 3. Close client connection abruptly (simulating disconnect)
		conn1.Close()

		// 4. Verify session is suspended after disconnect
		time.Sleep(100 * time.Millisecond) // Small delay to allow disconnect message processing
		listener.sessionsMu.RLock()
		session, exists := listener.sessions[sessionID]
		listener.sessionsMu.RUnlock()

		if !exists {
			t.Fatal("Session should still exist after disconnect")
		}
		if session.State != BridgeSessionStateSuspended {
			t.Fatalf("Session should be suspended, got state: %v", session.State)
		}

		// 5. Wait for cleanup delay and verify session is cleaned up
		time.Sleep(2 * time.Second)

		listener.sessionsMu.RLock()
		_, exists = listener.sessions[sessionID]
		listener.sessionsMu.RUnlock()

		if exists {
			t.Fatal("Session should have been cleaned up after delay")
		}

		// 6. Verify we cannot resume the cleaned up session
		_, err = clientBridge.ResumeSession(context.Background(), serverBridgeID, sessionID)
		if err == nil {
			t.Fatal("Expected error when resuming cleaned up session")
		}
	})

	t.Run("Disconnect Then Quick Resume", func(t *testing.T) {
		// 1. Establish initial connection
		conn1, err := clientBridge.Dial(context.Background(), serverBridgeID)
		if err != nil {
			t.Fatalf("Failed to establish initial connection: %v", err)
		}
		sessionID := conn1.(*MQTTNetBridgeConn).sessionID

		// 2. Close client connection abruptly
		conn1.Close()

		// 3. Quick resume before timeout
		time.Sleep(500 * time.Millisecond) // Increased delay for disconnect processing
		conn2, err := clientBridge.ResumeSession(context.Background(), serverBridgeID, sessionID)
		if err != nil {
			t.Fatalf("Failed to resume session before timeout: %v", err)
		}

		// 4. Verify resumed connection works
		testData := []byte("test after resume")
		_, err = conn2.Write(testData)
		if err != nil {
			t.Fatalf("Failed to write data after resume: %v", err)
		}

		buf := make([]byte, 1024)
		deadline := time.Now().Add(5 * time.Second)
		conn2.SetReadDeadline(deadline)
		n, err := conn2.Read(buf)
		if err != nil {
			t.Fatalf("Failed to read echo response after resume: %v", err)
		}
		if string(buf[:n]) != string(testData) {
			t.Fatalf("Echo mismatch after resume. Got %s, want %s", string(buf[:n]), string(testData))
		}

		// Clean up
		conn2.Close()
	})
}
