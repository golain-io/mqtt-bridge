package bridge

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"

	echo "github.com/golain-io/mqtt-bridge/example"
)

const benchBrokerURL = "tcp://localhost:1883"

func benchMQTTClient(b *testing.B, clientID string) mqtt.Client {
	b.Helper()
	opts := mqtt.NewClientOptions().
		AddBroker(benchBrokerURL).
		SetClientID(clientID)
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.WaitTimeout(5*time.Second) && token.Error() != nil {
		b.Skipf("MQTT broker unavailable at %s: %v", benchBrokerURL, token.Error())
	}
	return client
}

func setupBenchEchoServer(b *testing.B, rootTopic, serverID string) *MQTTNetBridge {
	b.Helper()
	serverClient := benchMQTTClient(b, serverID+"-mqtt")
	listener := NewMQTTNetBridge(serverClient, serverID,
		WithRootTopic(rootTopic),
		WithQoS(2),
	)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go benchEchoConn(conn)
		}
	}()
	time.Sleep(200 * time.Millisecond)
	b.Cleanup(func() {
		listener.Close()
		serverClient.Disconnect(250)
	})
	return listener
}

func benchEchoConn(conn io.ReadWriteCloser) {
	defer conn.Close()
	buf := make([]byte, 64*1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return
		}
		if _, err := conn.Write(buf[:n]); err != nil {
			return
		}
	}
}

func setupBenchClientConn(b *testing.B, rootTopic, clientID, serverID string) net.Conn {
	b.Helper()
	clientClient := benchMQTTClient(b, clientID+"-mqtt")
	bridge := NewMQTTNetBridge(clientClient, clientID, WithRootTopic(rootTopic), WithQoS(2))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := bridge.Dial(ctx, serverID)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	b.Cleanup(func() {
		conn.Close()
		bridge.Close()
		clientClient.Disconnect(250)
	})
	time.Sleep(200 * time.Millisecond)
	return conn
}

func BenchmarkNetBridgeDial(b *testing.B) {
	rootTopic := "/bench/dial"
	serverID := "bench-dial-server"
	setupBenchEchoServer(b, rootTopic, serverID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clientID := fmt.Sprintf("bench-dial-client-%d", i)
		clientClient := benchMQTTClient(b, clientID+"-mqtt")
		bridge := NewMQTTNetBridge(clientClient, clientID, WithRootTopic(rootTopic), WithQoS(2))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		conn, err := bridge.Dial(ctx, serverID)
		cancel()
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		conn.Close()
		bridge.Close()
		clientClient.Disconnect(250)
	}
}

func BenchmarkNetBridgeEchoRoundTrip(b *testing.B) {
	sizes := []int{64, 1024, 10 * 1024, 50 * 1024}
	for _, size := range sizes {
		b.Run(fmt.Sprintf("payload-%dB", size), func(b *testing.B) {
			rootTopic := fmt.Sprintf("/bench/echo/%d", size)
			serverID := fmt.Sprintf("bench-echo-server-%d", size)
			setupBenchEchoServer(b, rootTopic, serverID)
			conn := setupBenchClientConn(b, rootTopic, fmt.Sprintf("bench-echo-client-%d", size), serverID)
			payload := bytes.Repeat([]byte("x"), size)
			buf := make([]byte, size)

			b.SetBytes(int64(size * 2))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := conn.Write(payload); err != nil {
					b.Fatalf("write: %v", err)
				}
				if _, err := io.ReadFull(conn, buf); err != nil {
					b.Fatalf("read: %v", err)
				}
			}
		})
	}
}

func BenchmarkNetBridgeUnaryGRPC(b *testing.B) {
	logger := zap.NewNop()
	rootTopic := "/bench/grpc"
	serverID := "bench-grpc-server"

	serverClient := benchMQTTClient(b, serverID+"-mqtt")
	netBridge := NewMQTTNetBridge(serverClient, serverID,
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(2),
	)
	grpcServer := grpc.NewServer()
	echo.RegisterEchoServiceServer(grpcServer, echo.NewEchoServer())
	go grpcServer.Serve(netBridge)
	b.Cleanup(func() {
		grpcServer.Stop()
		netBridge.Close()
		serverClient.Disconnect(250)
	})
	time.Sleep(300 * time.Millisecond)

	clientClient := benchMQTTClient(b, "bench-grpc-client-mqtt")
	clientBridge := NewMQTTNetBridge(clientClient, "bench-grpc-client",
		WithRootTopic(rootTopic),
		WithLogger(logger),
		WithQoS(2),
	)
	resolver.Register(clientBridge)
	conn, err := grpc.NewClient(
		"mqtt://"+serverID,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
			return clientBridge.Dial(ctx, serverID)
		}),
	)
	if err != nil {
		b.Fatalf("grpc client: %v", err)
	}
	b.Cleanup(func() {
		conn.Close()
		clientBridge.Close()
		clientClient.Disconnect(250)
	})
	client := echo.NewEchoServiceClient(conn)
	req := &echo.EchoRequest{Message: "bench"}
	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.Echo(ctx, req); err != nil {
			b.Fatalf("echo: %v", err)
		}
	}
}
