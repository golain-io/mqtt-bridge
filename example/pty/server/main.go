package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"

	"github.com/creack/pty"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	bridge "github.com/golain-io/mqtt-bridge"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/term"
)

// shellSession handles an interactive shell session
func shellSession(ctx context.Context, cmdReader io.ReadWriter) error {
	cmd := exec.Command("/bin/bash")
	cmd.Env = os.Environ()
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid:  true,
		Setctty: true,
	}

	ptmx, err := pty.Start(cmd)
	if err != nil {
		return fmt.Errorf("start pty: %w", err)
	}
	defer ptmx.Close()

	// Handle PTY window size changes
	if err := handlePTYResize(ptmx); err != nil {
		return fmt.Errorf("setup pty resize: %w", err)
	}

	// Set up raw terminal mode
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return fmt.Errorf("set raw mode: %w", err)
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)

	// Handle I/O between PTY and connection
	errCh := make(chan error, 2)
	go copyIO(ptmx, cmdReader, errCh)
	go copyIO(cmdReader, ptmx, errCh)

	// Wait for either context cancellation or command completion
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	case err := <-waitCmdAsync(cmd):
		return err
	}
}

// handlePTYResize sets up window size handling for the PTY
func handlePTYResize(ptmx *os.File) error {
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGWINCH)

	go func() {
		for range ch {
			if err := pty.InheritSize(os.Stdin, ptmx); err != nil {
				// Log error but continue
				zap.L().Error("resize pty", zap.Error(err))
			}
		}
	}()

	// Initial resize
	ch <- syscall.SIGWINCH
	return nil
}

// copyIO copies data between reader and writer
func copyIO(dst io.Writer, src io.Reader, errCh chan<- error) {
	_, err := io.Copy(dst, src)
	errCh <- err
}

// waitCmdAsync waits for command completion asynchronously
func waitCmdAsync(cmd *exec.Cmd) <-chan error {
	ch := make(chan error, 1)
	go func() {
		ch <- cmd.Wait()
	}()
	return ch
}

// server represents the SSH server instance
type server struct {
	logger      *zap.Logger
	bridge      *bridge.MQTTNetBridge
	mqttClient  mqtt.Client
	activeConns sync.Map
}

// newServer creates and initializes a new server instance
func newServer(logger *zap.Logger) (*server, error) {
	opts := mqtt.NewClientOptions().
		AddBroker("tcp://localhost:1883").
		SetClientID("ssh-server-" + uuid.New().String())

	mqttClient := mqtt.NewClient(opts)
	if token := mqttClient.Connect(); token.Wait() && token.Error() != nil {
		return nil, fmt.Errorf("connect to MQTT broker: %w", token.Error())
	}

	b := bridge.NewMQTTNetBridge(mqttClient, "test-server", bridge.WithLogger(logger), bridge.WithRootTopic("/root/test"))

	return &server{
		logger:      logger,
		bridge:      b,
		mqttClient:  mqttClient,
		activeConns: sync.Map{},
	}, nil
}

// run starts the server and handles connections
func (s *server) run(ctx context.Context) error {
	defer s.cleanup()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			conn, err := s.bridge.Accept()
			if err != nil {
				s.logger.Error("accept connection", zap.Error(err))
				continue
			}

			connID := uuid.New()
			s.activeConns.Store(connID, conn)

			go s.handleConnection(ctx, conn, connID)
		}
	}
}

// handleConnection processes a single client connection
func (s *server) handleConnection(ctx context.Context, conn io.ReadWriter, id uuid.UUID) {
	defer s.activeConns.Delete(id)

	if err := shellSession(ctx, conn); err != nil {
		s.logger.Error("shell session error",
			zap.Error(err),
			zap.String("connection_id", id.String()),
		)
	}
}

// cleanup performs necessary cleanup when server shuts down
func (s *server) cleanup() {
	s.activeConns.Range(func(key, value interface{}) bool {
		if closer, ok := value.(io.Closer); ok {
			closer.Close()
		}
		s.activeConns.Delete(key)
		return true
	})

	s.bridge.Close()
	s.mqttClient.Disconnect(250)
}

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	srv, err := newServer(logger)
	if err != nil {
		logger.Fatal("initialize server", zap.Error(err))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		logger.Info("shutting down server...")
		cancel()
	}()

	if err := srv.run(ctx); err != nil && err != context.Canceled {
		logger.Fatal("server error", zap.Error(err))
	}
}
