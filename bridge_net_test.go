package bridge

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestMQTTBridgeEchoServer(t *testing.T) {
	// Setup logger
	logger, _ := zap.NewProduction()
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
	rootTopic := "/8d94_b0aa/CC:47:40:05:A6:B7"
	listener := NewMQTTNetBridge(serverClient, serverBridgeID,
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(2),
	)
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

	clientLogger, _ := zap.NewDevelopment()

	// Create client bridge
	clientBridgeID := "test-client"
	clientBridge := NewMQTTNetBridge(clientClient, clientBridgeID, WithRootTopic(rootTopic), WithLogger(clientLogger))
	defer clientBridge.Close()

	// Connect to the server
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Add small delay to ensure server is ready
	time.Sleep(100 * time.Millisecond)

	conn, err := clientBridge.Dial(ctx, serverBridgeID)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	// Wait for server readiness with increased timeout
	select {
	case <-serverReady:
		t.Log("Server ready")
	case err := <-serverErr:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("Timeout waiting for server to be ready")
	}

	// Add delay to ensure MQTT subscriptions are fully established
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

func TestMQTTBridgeUnsubscribe(t *testing.T) {
	// Setup logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// Create MQTT clients
	serverClient := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-server-unsub"))

	if token := serverClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect server to MQTT: %v", token.Error())
	}
	defer serverClient.Disconnect(250)

	// Create bridge listener
	serverBridgeID := "test-server-unsub"
	rootTopic := "/vedant/unsub"
	listener := NewMQTTNetBridge(serverClient, serverBridgeID,
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(2),
	)
	defer listener.Close()

	// Create client MQTT client
	clientClient := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-client-unsub"))

	if token := clientClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect client to MQTT: %v", token.Error())
	}
	defer clientClient.Disconnect(250)

	// Create client bridge
	clientBridgeID := "test-client-unsub"
	clientBridge := NewMQTTNetBridge(clientClient, clientBridgeID,
		WithRootTopic(rootTopic),
		WithLogger(logger))
	defer clientBridge.Close()

	// Start server goroutine
	serverConn := make(chan io.ReadWriteCloser, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			t.Errorf("Accept error: %v", err)
			return
		}
		serverConn <- conn
	}()

	// Connect client to server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientConn, err := clientBridge.Dial(ctx, serverBridgeID)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}

	// Wait for server connection
	var conn io.ReadWriteCloser
	select {
	case conn = <-serverConn:
		t.Log("Server accepted connection")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for server connection")
	}

	// Allow time for subscriptions to be established
	time.Sleep(500 * time.Millisecond)

	// Test sending data before closing
	testMsg := "test message"
	_, err = clientConn.Write([]byte(testMsg))
	assert.NoError(t, err)

	// Read response with timeout
	buf := make([]byte, 1024)
	if tc, ok := conn.(interface{ SetReadDeadline(time.Time) error }); ok {
		tc.SetReadDeadline(time.Now().Add(2 * time.Second))
	}
	n, err := conn.Read(buf)
	assert.NoError(t, err)
	assert.Equal(t, testMsg, string(buf[:n]))

	// Close client connection
	clientConn.Close()

	// Wait a bit for the connection to fully close
	time.Sleep(1 * time.Second)

	// Try to send data after closing
	_, err = clientConn.Write([]byte("should not work"))
	assert.Error(t, err, "Write should fail after connection is closed")

	// Try to read data after closing - should timeout or error
	if tc, ok := conn.(interface{ SetReadDeadline(time.Time) error }); ok {
		tc.SetReadDeadline(time.Now().Add(2 * time.Second))
	}
	_, err = conn.Read(buf)
	assert.Error(t, err, "Read should fail after connection is closed")

	// Close server connection
	conn.Close()
}

func TestMQTTBridgeProxy(t *testing.T) {
	// Setup logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// Create a temporary Unix socket path with unique name
	sockPath := "/tmp/test-proxy.sock"

	if err := os.Remove(sockPath); err != nil && !os.IsNotExist(err) {
		logger.Error("Failed to clean up existing socket", zap.String("address", sockPath), zap.Error(err))
	}

	// Ensure socket file is cleaned up after test
	defer os.Remove(sockPath)

	// Create a backend TCP server that will be proxied
	backendServer, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("Failed to create backend server: %v", err)
	}
	defer backendServer.Close()

	// Channel to signal backend server is ready
	backendReady := make(chan struct{})

	// Start backend server
	backendDone := make(chan struct{})
	go func() {
		defer close(backendDone)
		// Signal that we're ready to accept connections
		close(backendReady)

		for {
			conn, err := backendServer.Accept()
			if err != nil {
				if !strings.Contains(err.Error(), "use of closed network connection") {
					t.Errorf("Backend accept error: %v", err)
				}
				return
			}

			t.Log("Backend server accepted connection")
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 1024)
				for {
					n, err := c.Read(buf)
					if err != nil {
						if err != io.EOF && !strings.Contains(err.Error(), "use of closed network connection") {
							t.Logf("Backend read error: %v", err)
						}
						return
					}

					t.Logf("Backend received: %s", string(buf[:n]))

					// Echo the received data back
					_, err = c.Write(buf[:n])
					if err != nil {
						if !strings.Contains(err.Error(), "use of closed network connection") {
							t.Logf("Backend write error: %v", err)
						}
						return
					}
					t.Logf("Backend echoed: %s", string(buf[:n]))
				}
			}(conn)
		}
	}()

	// Wait for backend server to be ready
	select {
	case <-backendReady:
		t.Log("Backend server ready")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for backend server to be ready")
	}

	// Create MQTT clients
	serverClient := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-server-proxy"))

	if token := serverClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect server to MQTT: %v", token.Error())
	}
	defer serverClient.Disconnect(250)

	// Create bridge listener with proxy configuration
	serverBridgeID := "test-server-proxy"
	rootTopic := "/vedant/proxy"
	listener := NewMQTTNetBridge(serverClient, serverBridgeID,
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(2),
		WithCleanUpInterval(50*time.Second),
		WithProxyAddr("unix", sockPath),
	)
	defer listener.Close()

	// Start accepting connections in the background
	acceptDone := make(chan struct{})
	acceptErr := make(chan error, 1)
	go func() {
		defer close(acceptDone)
		for {
			conn, err := listener.Accept()
			if err != nil {
				if !strings.Contains(err.Error(), "listener closed") &&
					!strings.Contains(err.Error(), "context canceled") {
					acceptErr <- err
					t.Errorf("Accept error: %v", err)
				}
				return
			}
			t.Log("Bridge accepted connection")
			// Keep the connection open until the test finishes
			defer conn.Close()
		}
	}()

	// Create client MQTT client
	clientClient := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-client-proxy"))

	if token := clientClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect client to MQTT: %v", token.Error())
	}
	defer clientClient.Disconnect(250)

	// Create client bridge
	clientBridgeID := "test-client-proxy"
	clientBridge := NewMQTTNetBridge(clientClient, clientBridgeID,
		WithRootTopic(rootTopic),
		WithLogger(logger))
	defer clientBridge.Close()

	// Connect client to server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Add small delay to ensure server is ready
	time.Sleep(100 * time.Millisecond)

	clientConn, err := clientBridge.Dial(ctx, serverBridgeID, WithSessionTimeout(30*time.Second))
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer clientConn.Close()

	// Test proxy functionality with multiple messages
	testCases := []string{
		"Hello through proxy!",
		"Testing proxy 1,2,3",
		"Special proxy chars: !@#$%^&*()",
	}

	for _, tc := range testCases {
		t.Run(fmt.Sprintf("Proxy_%s", tc), func(t *testing.T) {
			// Write test message
			t.Logf("Writing message: %s", tc)
			_, err := clientConn.Write([]byte(tc))
			assert.NoError(t, err)

			// Read response with retry
			buf := make([]byte, 1024)
			var n int
			for retries := 3; retries > 0; retries-- {
				n, err = clientConn.Read(buf)
				if err == nil {
					break
				}
				t.Logf("Read attempt failed (retries left: %d): %v", retries-1, err)
				time.Sleep(100 * time.Millisecond)
			}
			assert.NoError(t, err)
			response := string(buf[:n])
			t.Logf("Received response: %s", response)
			assert.Equal(t, tc, response)
		})

		// Add delay between test cases
		time.Sleep(100 * time.Millisecond)
	}

	// Clean up in reverse order
	clientConn.Close()
	clientBridge.Close()
	listener.Close()
	backendServer.Close()

	// Wait for backend server to finish
	select {
	case <-backendDone:
		t.Log("Backend server closed")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for backend server to close")
	}

	// Wait for accept goroutine to finish
	select {
	case <-acceptDone:
		t.Log("Accept loop closed")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for accept loop to close")
	}

	// Check if there were any unexpected accept errors
	select {
	case err := <-acceptErr:
		t.Errorf("Unexpected accept error: %v", err)
	default:
		// No errors, test passed
	}
}

// TestCloseUnblocksRead verifies that a blocked Read() operation unblocks immediately
// when Close() is called, as required by the net.Conn interface contract.
func TestCloseUnblocksRead(t *testing.T) {
	// Setup logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// Create MQTT clients
	serverClient := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-close-read-server"))

	if token := serverClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect server to MQTT: %v", token.Error())
	}
	defer serverClient.Disconnect(250)

	// Create bridge listener
	serverBridgeID := "test-close-read-server"
	rootTopic := "/vedant/close-read"
	listener := NewMQTTNetBridge(serverClient, serverBridgeID,
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(2),
	)
	defer listener.Close()

	// Create client MQTT client
	clientClient := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-close-read-client"))

	if token := clientClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect client to MQTT: %v", token.Error())
	}
	defer clientClient.Disconnect(250)

	// Create client bridge
	clientBridgeID := "test-close-read-client"
	clientBridge := NewMQTTNetBridge(clientClient, clientBridgeID,
		WithRootTopic(rootTopic),
		WithLogger(logger))
	defer clientBridge.Close()

	// Start server goroutine
	serverConn := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			t.Errorf("Accept error: %v", err)
			return
		}
		serverConn <- conn
	}()

	// Connect client to server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientConn, err := clientBridge.Dial(ctx, serverBridgeID)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}

	// Wait for server connection
	var conn net.Conn
	select {
	case conn = <-serverConn:
		t.Log("Server accepted connection")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for server connection")
	}
	defer conn.Close()

	// Allow time for subscriptions to be established
	time.Sleep(500 * time.Millisecond)

	// Start a goroutine that blocks on Read()
	readDone := make(chan struct{})
	readErr := make(chan error, 1)
	go func() {
		defer close(readDone)
		buf := make([]byte, 1024)
		_, err := clientConn.Read(buf)
		readErr <- err
	}()

	// Give Read() a moment to block
	time.Sleep(100 * time.Millisecond)

	// Close the connection - this should unblock the Read()
	closeStart := time.Now()
	err = clientConn.Close()
	assert.NoError(t, err)

	// Wait for Read() to unblock (should happen immediately)
	select {
	case err := <-readErr:
		closeDuration := time.Since(closeStart)
		// Read should have unblocked within 500ms (much faster than any timeout)
		assert.Less(t, closeDuration, 500*time.Millisecond,
			"Read() should unblock immediately after Close()")
		// Should return net.ErrClosed or io.EOF
		assert.True(t, err == net.ErrClosed || err == io.EOF,
			"Read() should return net.ErrClosed or io.EOF, got: %v", err)
		t.Logf("Read() unblocked after %v with error: %v", closeDuration, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Read() did not unblock within 2 seconds after Close()")
	}

	// Wait for goroutine to finish
	select {
	case <-readDone:
		// Success
	case <-time.After(1 * time.Second):
		t.Error("Read goroutine did not finish")
	}
}

// TestCloseUnblocksWrite verifies that a blocked Write() operation unblocks immediately
// when Close() is called, as required by the net.Conn interface contract.
func TestCloseUnblocksWrite(t *testing.T) {
	// Setup logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// Create MQTT clients
	serverClient := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-close-write-server"))

	if token := serverClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect server to MQTT: %v", token.Error())
	}
	defer serverClient.Disconnect(250)

	// Create bridge listener
	serverBridgeID := "test-close-write-server"
	rootTopic := "/vedant/close-write"
	listener := NewMQTTNetBridge(serverClient, serverBridgeID,
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(2),
	)
	defer listener.Close()

	// Create client MQTT client
	clientClient := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-close-write-client"))

	if token := clientClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect client to MQTT: %v", token.Error())
	}
	defer clientClient.Disconnect(250)

	// Create client bridge
	clientBridgeID := "test-close-write-client"
	clientBridge := NewMQTTNetBridge(clientClient, clientBridgeID,
		WithRootTopic(rootTopic),
		WithLogger(logger))
	defer clientBridge.Close()

	// Start server goroutine
	serverConn := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			t.Errorf("Accept error: %v", err)
			return
		}
		serverConn <- conn
	}()

	// Connect client to server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientConn, err := clientBridge.Dial(ctx, serverBridgeID)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}

	// Wait for server connection
	var conn net.Conn
	select {
	case conn = <-serverConn:
		t.Log("Server accepted connection")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for server connection")
	}
	defer conn.Close()

	// Allow time for subscriptions to be established
	time.Sleep(500 * time.Millisecond)

	// Start a goroutine that blocks on Write()
	// We'll write a large message to potentially block
	writeDone := make(chan struct{})
	writeErr := make(chan error, 1)
	go func() {
		defer close(writeDone)
		largeData := make([]byte, 1024*1024) // 1MB to potentially cause blocking
		_, err := clientConn.Write(largeData)
		writeErr <- err
	}()

	// Give Write() a moment to potentially block
	time.Sleep(100 * time.Millisecond)

	// Close the connection - this should unblock the Write()
	closeStart := time.Now()
	err = clientConn.Close()
	assert.NoError(t, err)

	// Wait for Write() to unblock (should happen immediately)
	select {
	case err := <-writeErr:
		closeDuration := time.Since(closeStart)
		// Write should have unblocked within 500ms (much faster than any timeout)
		assert.Less(t, closeDuration, 500*time.Millisecond,
			"Write() should unblock immediately after Close()")
		// Should return net.ErrClosed
		assert.Equal(t, net.ErrClosed, err,
			"Write() should return net.ErrClosed, got: %v", err)
		t.Logf("Write() unblocked after %v with error: %v", closeDuration, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Write() did not unblock within 2 seconds after Close()")
	}

	// Wait for goroutine to finish
	select {
	case <-writeDone:
		// Success
	case <-time.After(1 * time.Second):
		t.Error("Write goroutine did not finish")
	}
}

// TestCloseIdempotent verifies that multiple calls to Close() are safe and idempotent.
func TestCloseIdempotent(t *testing.T) {
	// Setup logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// Create MQTT clients
	serverClient := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-close-idempotent-server"))

	if token := serverClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect server to MQTT: %v", token.Error())
	}
	defer serverClient.Disconnect(250)

	// Create bridge listener
	serverBridgeID := "test-close-idempotent-server"
	rootTopic := "/vedant/close-idempotent"
	listener := NewMQTTNetBridge(serverClient, serverBridgeID,
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(2),
	)
	defer listener.Close()

	// Create client MQTT client
	clientClient := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("bridge-test-close-idempotent-client"))

	if token := clientClient.Connect(); token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect client to MQTT: %v", token.Error())
	}
	defer clientClient.Disconnect(250)

	// Create client bridge
	clientBridgeID := "test-close-idempotent-client"
	clientBridge := NewMQTTNetBridge(clientClient, clientBridgeID,
		WithRootTopic(rootTopic),
		WithLogger(logger))
	defer clientBridge.Close()

	// Start server goroutine
	serverConn := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		serverConn <- conn
	}()

	// Connect client to server
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientConn, err := clientBridge.Dial(ctx, serverBridgeID)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}

	// Wait for server connection
	select {
	case <-serverConn:
		t.Log("Server accepted connection")
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for server connection")
	}

	// Allow time for subscriptions to be established
	time.Sleep(500 * time.Millisecond)

	// Call Close() multiple times concurrently
	closeCount := 10
	closeErrs := make(chan error, closeCount)
	for i := 0; i < closeCount; i++ {
		go func() {
			closeErrs <- clientConn.Close()
		}()
	}

	// Collect all results
	var errors []error
	for i := 0; i < closeCount; i++ {
		select {
		case err := <-closeErrs:
			errors = append(errors, err)
		case <-time.After(2 * time.Second):
			t.Fatal("Close() did not return within 2 seconds")
		}
	}

	// All Close() calls should succeed (return nil)
	for i, err := range errors {
		assert.NoError(t, err, "Close() call %d should succeed", i+1)
	}

	// Verify connection is actually closed
	_, err = clientConn.Write([]byte("test"))
	assert.Error(t, err, "Write should fail after Close()")
	assert.Equal(t, net.ErrClosed, err, "Write should return net.ErrClosed")
}
