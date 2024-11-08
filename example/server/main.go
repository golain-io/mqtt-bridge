package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	bridge "github.com/golain-io/mqtt-bridge"
	echo "github.com/golain-io/mqtt-bridge/example"
	"go.uber.org/zap"
	"google.golang.org/grpc/reflection"
)

func main() {
	// Create logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// Create MQTT client
	opts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("echo-server")

	mqttClient := mqtt.NewClient(opts)
	token := mqttClient.Connect()
	if token.Wait() && token.Error() != nil {
		logger.Fatal("Failed to connect to MQTT broker", zap.Error(token.Error()))
	}
	defer mqttClient.Disconnect(0)

	// Create bridge
	bridge := bridge.NewMQTTBridge(mqttClient, logger, 30*time.Second)

	// Create and register echo service
	echoServer := echo.NewEchoServer()
	bridge.RegisterService(&echo.EchoService_ServiceDesc, echoServer)
	reflection.Register(bridge)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down server...")
}
