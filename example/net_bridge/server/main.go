package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	bridge "github.com/golain-io/mqtt-bridge"
	echo "github.com/golain-io/mqtt-bridge/example"
)

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// Create MQTT client
	opts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("echo-net-server")

	mqttClient := mqtt.NewClient(opts)
	token := mqttClient.Connect()
	if token.Wait() && token.Error() != nil {
		log.Fatal("Failed to connect to MQTT broker:", token.Error())
	}
	defer mqttClient.Disconnect(0)

	// Create network bridge
	netBridge := bridge.NewMQTTNetBridge(mqttClient, logger, "echo-service")

	// Create gRPC server with timeout options
	grpcServer := grpc.NewServer()

	// Create gRPC server using the network bridge
	echoServer := echo.NewEchoServer()
	echo.RegisterEchoServiceServer(grpcServer, echoServer)
	reflection.Register(grpcServer)

	go grpcServer.Serve(netBridge)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Info("Shutting down server...")
	netBridge.Close()
}
