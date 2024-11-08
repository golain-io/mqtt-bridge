package main

import (
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"

	bridge "github.com/golain-io/mqtt-bridge"
	echo "github.com/golain-io/mqtt-bridge/example"
)

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// Create MQTT client
	opts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("echo-client")

	mqttClient := mqtt.NewClient(opts)
	token := mqttClient.Connect()
	if token.Wait() && token.Error() != nil {
		logger.Fatal("Failed to connect to MQTT broker", zap.Error(token.Error()))
	}
	defer mqttClient.Disconnect(0)

	// Create session ID and stream ID
	sessionID := "test-session-1"

	// Subscribe to response topic
	responseTopic := bridge.BuildTopicPath("echo", "EchoService", "Echo", sessionID, "up")
	token = mqttClient.Subscribe(responseTopic, 1, func(client mqtt.Client, msg mqtt.Message) {
		frame, err := bridge.UnmarshalFrame(msg.Payload())
		if err != nil {
			logger.Error("Failed to unmarshal response", zap.Error(err))
			return
		}

		resp := &echo.EchoResponse{}
		if err := proto.Unmarshal(frame.Data, resp); err != nil {
			logger.Error("Failed to unmarshal echo response", zap.Error(err))
			return
		}

		logger.Info("Received response",
			zap.String("message", resp.Message),
			zap.Int32("sequence", resp.Sequence))
	})

	if token.Wait() && token.Error() != nil {
		logger.Fatal("Failed to subscribe", zap.Error(token.Error()))
	}

	// Send request
	req := &echo.EchoRequest{Message: "Hello, MQTT-gRPC Bridge!"}
	reqData, err := proto.Marshal(req)
	if err != nil {
		logger.Fatal("Failed to marshal request", zap.Error(err))
	}

	frame, err := bridge.NewFrame(bridge.MessageTypeData, 1, reqData)
	if err != nil {
		logger.Fatal("Failed to create frame", zap.Error(err))
	}

	requestTopic := bridge.BuildTopicPath("echo", "EchoService", "Echo", sessionID, "down")
	token = mqttClient.Publish(requestTopic, 1, false, frame.Marshal())
	if token.Wait() && token.Error() != nil {
		logger.Fatal("Failed to publish request", zap.Error(token.Error()))
	}

	// Wait a bit for response
	time.Sleep(2 * time.Second)
}
