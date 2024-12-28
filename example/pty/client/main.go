package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	bridge "github.com/golain-io/mqtt-bridge"
	"github.com/google/uuid"
)

func main() {
	// Setup MQTT client
	client := mqtt.NewClient(mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("client-" + uuid.New().String()))

	if ok := client.Connect().Wait(); !ok {
		log.Fatal("MQTT connect failed")
	}
	defer client.Disconnect(0)

	// Setup bridge connection
	b := bridge.NewMQTTNetBridge(client, "test-client", bridge.WithRootTopic("/root/test"))
	defer b.Close()

	conn, err := b.Dial(context.Background(), "test-server")
	if err != nil {
		log.Fatal("Dial failed:", err)
	}
	defer conn.Close()

	errCh := make(chan error, 2)

	// Start goroutine to relay data from connection to stdout
	go relay(conn, os.Stdout, errCh, "read")

	// Start goroutine to relay data from stdin to connection
	go relay(os.Stdin, conn, errCh, "write")

	if err := <-errCh; err != nil {
		log.Fatal(err)
	}
}

func relay(r io.Reader, w io.Writer, errCh chan error, op string) {
	buf := make([]byte, 1024)
	for {
		n, err := r.Read(buf)
		if err != nil {
			errCh <- fmt.Errorf("%s error: %w", op, err)
			return
		}
		if _, err := w.Write(buf[:n]); err != nil {
			errCh <- fmt.Errorf("%s write error: %w", op, err)
			return
		}
	}
}
