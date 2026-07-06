package bridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"

	"log/slog"
)

// listens for connections on a unix socket and proxies them to the bridge
func (b *MQTTNetBridge) ListenOnUnixSocket(path string, addr string) error {
	// Remove existing socket file if it exists
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove existing socket: %v", err)
	}

	// listen for connections on the socket
	listener, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	defer listener.Close()

	// accept connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}

		// proxy the connection to the bridge
		bConn, err := b.Dial(b.ctx, addr)
		if err != nil {
			return err
		}
		b.proxyConn(conn, bConn)
	}
}

func (b *MQTTNetBridge) proxyConn(conn net.Conn, bConn net.Conn) {
	errChan := make(chan error, 2)
	done := make(chan struct{})

	// Copy from client to bridge
	go func() {
		_, err := io.Copy(bConn, conn)
		if err != nil && err != io.EOF && !isClosedConnError(err) {
			b.logger.Error("Error copying data from client to bridge", slog.Any("error", err))
		}
		errChan <- err
	}()

	// Copy from bridge to client
	go func() {
		_, err := io.Copy(conn, bConn)
		if err != nil && err != io.EOF && !isClosedConnError(err) {
			b.logger.Error("Error copying data from bridge to client", slog.Any("error", err))
		}
		errChan <- err
	}()

	// Wait for either copy operation to finish
	go func() {
		var proxyErr error
		for i := 0; i < 2; i++ {
			if err := <-errChan; err != nil && err != io.EOF && !isClosedConnError(err) {
				proxyErr = errors.Join(proxyErr, err)
			}
		}
		if proxyErr != nil {
			b.logger.Error("Proxy connection error",
				slog.Any("error", proxyErr))
		}
		close(done)
	}()

	// Wait for completion or context cancellation
	select {
	case <-done:
		b.logger.Debug("Proxy connection completed")
	case <-b.ctx.Done():
		b.logger.Debug("Proxy connection cancelled")
	}

	// Ensure both connections are closed
	conn.Close()
	bConn.Close()
}

// isClosedConnError returns true if the error is related to using a closed connection
func isClosedConnError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, net.ErrClosed) ||
		errors.Is(err, context.Canceled) ||
		errors.Is(err, syscall.EPIPE)
}
