package main

import (
	"log/slog"
	"os"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"google.golang.org/protobuf/proto"

	bridge "github.com/golain-io/mqtt-bridge"
	echo "github.com/golain-io/mqtt-bridge/example"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// Create MQTT client
	opts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("echo-client")

	mqttClient := mqtt.NewClient(opts)
	token := mqttClient.Connect()
	if token.Wait() && token.Error() != nil {
		logger.Error("Failed to connect to MQTT broker", slog.Any("error", token.Error()))
		os.Exit(1)
	}
	defer mqttClient.Disconnect(0)

	// Create session ID and stream ID
	sessionID := "test-session-1"

	// Subscribe to response topic
	responseTopic := bridge.BuildTopicPath("echo", "EchoService", "Echo", sessionID, "up")
	token = mqttClient.Subscribe(responseTopic, 1, func(client mqtt.Client, msg mqtt.Message) {
		frame, err := bridge.UnmarshalFrame(msg.Payload())
		if err != nil {
			logger.Error("Failed to unmarshal response", slog.Any("error", err))
			return
		}

		resp := &echo.EchoResponse{}
		if err := proto.Unmarshal(frame.Data, resp); err != nil {
			logger.Error("Failed to unmarshal echo response", slog.Any("error", err))
			return
		}

		logger.Info("Received response",
			slog.String("message", resp.Message),
			slog.Int("sequence", int(resp.Sequence)))
	})

	if token.Wait() && token.Error() != nil {
		logger.Error("Failed to subscribe", slog.Any("error", token.Error()))
		os.Exit(1)
	}

	// Send request
	req := &echo.EchoRequest{Message: "Hello, MQTT-gRPC Bridge!"}
	reqData, err := proto.Marshal(req)
	if err != nil {
		logger.Error("Failed to marshal request", slog.Any("error", err))
		os.Exit(1)
	}

	frame, err := bridge.NewFrame(bridge.MessageTypeData, 1, reqData)
	if err != nil {
		logger.Error("Failed to create frame", slog.Any("error", err))
		os.Exit(1)
	}

	requestTopic := bridge.BuildTopicPath("echo", "EchoService", "Echo", sessionID, "down")
	frameData, err := frame.Marshal()
	if err != nil {
		logger.Error("Failed to marshal frame", slog.Any("error", err))
		os.Exit(1)
	}
	token = mqttClient.Publish(requestTopic, 1, false, frameData)
	if token.Wait() && token.Error() != nil {
		logger.Error("Failed to publish request", slog.Any("error", token.Error()))
		os.Exit(1)
	}

	// Wait a bit for response
	time.Sleep(2 * time.Second)
}
