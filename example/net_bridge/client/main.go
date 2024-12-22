package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"

	bridge "github.com/golain-io/mqtt-bridge"
	echo "github.com/golain-io/mqtt-bridge/example"
)

func main() {
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// Create MQTT client with unique client ID
	opts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("echo-net-client-" + uuid.New().String())

	mqttClient := mqtt.NewClient(opts)
	token := mqttClient.Connect()
	if token.Wait() && token.Error() != nil {
		log.Fatal("Failed to connect to MQTT broker:", token.Error())
	}
	defer mqttClient.Disconnect(0)

	// Create network bridge with unique client ID
	netBridge := bridge.NewMQTTNetBridge(mqttClient, "echo-client", bridge.WithLogger(logger))

	/**
	You could also use the deprecated grpc.Dial() function with the bridge client
	This is supported as well, but since grpc-go is moving to the NewClient() +
	Resolver style clients, we recommend using that approach (shown below).

	Here is the Dial() version:

	conn, err := grpc.Dial(
		"echo-service",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return netBridge.Dial(ctx, addr)
		}),
	)

	NOTE: with the dial approach, you don't need the `mqtt://` scheme prefix.
	*/

	// since we are using an "mqtt" scheme, we need to register the bridge as a resolver builder
	// so that grpc is aware of it
	resolver.Register(netBridge)

	// Create gRPC client with appropriate timeouts and retry policy
	ctx := context.Background()
	conn, err := grpc.NewClient(
		"mqtt://echo-service",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// alternatively, you can register the bridge as a resolver builder
		// only for this client connection here:
		// grpc.WithResolvers(netBridge),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return netBridge.Dial(ctx, addr)
		}),
	)
	if err != nil {
		log.Fatal("Failed to dial server:", err)
	}
	conn.Connect()
	defer conn.Close()

	// Create echo client
	client := echo.NewEchoServiceClient(conn)

	// Make echo call with timeout
	callCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	resp, err := client.Echo(callCtx, &echo.EchoRequest{Message: "Hello from client!"})
	if err != nil {
		log.Fatal("Echo call failed:", err)
	}

	fmt.Println("Response:", resp.Message)

	// streaming echo
	stream, err := client.EchoStream(callCtx, &echo.EchoRequest{Message: "Hello from client!"})
	if err != nil {
		log.Fatal("StreamEcho call failed:", err)
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		fmt.Println("Response:", resp.Message)
	}

	// bidirectional streaming echo
	stream, err = client.EchoBidiStream(callCtx)
	if err != nil {
		log.Fatal("StreamEcho call failed:", err)
	}

	go func() {
		for i := 0; i < 10; i++ {
			stream.SendMsg(&echo.EchoRequest{Message: fmt.Sprintf("Hello from client! %d", i)})
			time.Sleep(100 * time.Millisecond)
		}
	}()

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		fmt.Println("Response:", resp.Message)
	}

	time.Sleep(1 * time.Second)

	// close the stream
	stream.CloseSend()
	// wait for the stream to close
	<-stream.Context().Done()
}
