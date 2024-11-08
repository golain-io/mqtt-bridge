package bridge

import (
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"

	echo "github.com/golain-io/mqtt-bridge/example"
)

func setupTestBridge(t *testing.T) (*MQTTBridge, echo.EchoServiceServer) {
	// Create MQTT client
	opts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("test-bridge")

	mqttClient := mqtt.NewClient(opts)
	token := mqttClient.Connect()
	if token.Wait() && token.Error() != nil {
		t.Fatalf("Failed to connect to MQTT broker: %v", token.Error())
	}

	// Create logger
	logger, _ := zap.NewDevelopment()

	// Create bridge
	bridge := NewMQTTBridge(mqttClient, logger, 30*time.Second)

	// Create and register echo service
	echoServer := echo.NewEchoServer()
	bridge.RegisterService(&echo.EchoService_ServiceDesc, echoServer)

	return bridge, echoServer
}

func TestEchoUnary(t *testing.T) {
	bridge, _ := setupTestBridge(t)
	defer bridge.mqttClient.Disconnect(0)

	// Test implementation goes here
	// You'll need to implement the client side to send messages
	// and verify responses
}
