package bridge

import (
	"context"
	"fmt"
	"io"
	"strings"
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
