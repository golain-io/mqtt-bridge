package bridge

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"
)

// MQTTNetBridgeConn implements net.Conn over MQTT.
type MQTTNetBridgeConn struct {
	ctx    context.Context
	cancel context.CancelFunc

	bridge     *MQTTNetBridge
	localAddr  net.Addr
	remoteAddr net.Addr
	sessionID  string

	// Read buffer management
	readBuf      chan []byte
	readMu       sync.Mutex
	deadline     time.Time
	readBufClose sync.Once // Protects against double-close of readBuf

	// Write management
	writeMu   sync.Mutex
	wDeadline time.Time

	// Connection state
	closed  bool
	closeMu sync.RWMutex

	// Add topics
	upTopic   string
	downTopic string
	connected bool
	connMu    sync.RWMutex
	role      string // "client" or "server"

	// Lifecycle message handling
	respChan chan struct {
		payload []byte
		topic   string
	}
}

// SessionID returns the MQTT session ID for this connection.
func (c *MQTTNetBridgeConn) SessionID() string {
	return c.sessionID
}

func (c *MQTTNetBridgeConn) Read(b []byte) (n int, err error) {
	c.connMu.RLock()
	if !c.connected {
		c.connMu.RUnlock()
		return 0, fmt.Errorf("connection not established")
	}
	c.connMu.RUnlock()

	c.readMu.Lock()
	deadline := c.deadline
	c.readMu.Unlock()

	var timer *time.Timer
	var timeout <-chan time.Time

	if !deadline.IsZero() {
		if time.Now().After(deadline) {
			return 0, os.ErrDeadlineExceeded
		}
		timer = time.NewTimer(time.Until(deadline))
		timeout = timer.C
		defer timer.Stop()
	}

	select {
	case data, ok := <-c.readBuf:
		if !ok {
			return 0, io.EOF
		}
		n = copy(b, data)
		return n, nil
	case <-timeout:
		return 0, os.ErrDeadlineExceeded
	case <-c.ctx.Done():
		return 0, net.ErrClosed
	case <-c.bridge.ctx.Done():
		return 0, c.bridge.ctx.Err()
	}
}

func (c *MQTTNetBridgeConn) Write(b []byte) (n int, err error) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	// Check context before proceeding (unblocks immediately if Close() was called)
	select {
	case <-c.ctx.Done():
		return 0, net.ErrClosed
	default:
	}

	// Check closed flag with proper synchronization
	c.closeMu.RLock()
	closed := c.closed
	c.closeMu.RUnlock()
	if closed {
		return 0, net.ErrClosed
	}

	// Determine which topic to use based on role
	topic := c.upTopic
	if c.role == "server" {
		topic = c.downTopic // Server writes to down topic
	}

	c.bridge.logger.Debug("Writing data",
		zap.String("sessionID", c.sessionID),
		zap.Int("bytes", len(b)),
		zap.String("topic", topic))

	done := make(chan struct{})
	var publishErr error
	go func() {
		defer close(done)
		token := c.bridge.mqttClient.Publish(topic, c.bridge.qos, false, b)
		if !token.WaitTimeout(5 * time.Second) {
			publishErr = fmt.Errorf("write timeout")
		} else if token.Error() != nil {
			publishErr = token.Error()
		}
	}()

	for {
		select {
		case <-c.ctx.Done():
			return 0, net.ErrClosed
		case <-done:
			if c.ctx.Err() != nil {
				return 0, net.ErrClosed
			}
			c.closeMu.RLock()
			closed := c.closed
			c.closeMu.RUnlock()
			if closed {
				return 0, net.ErrClosed
			}
			if publishErr != nil {
				select {
				case <-c.ctx.Done():
					return 0, net.ErrClosed
				case <-time.After(200 * time.Millisecond):
					if c.ctx.Err() != nil {
						return 0, net.ErrClosed
					}
					c.closeMu.RLock()
					if c.closed {
						c.closeMu.RUnlock()
						return 0, net.ErrClosed
					}
					c.closeMu.RUnlock()
					return 0, publishErr
				}
			}
			return len(b), nil
		}
	}
}

func (c *MQTTNetBridgeConn) Close() error {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return nil
	}
	c.closed = true
	c.closeMu.Unlock()

	alreadyClosed := false
	select {
	case <-c.ctx.Done():
		alreadyClosed = true
	default:
	}

	shouldDisconnect := false
	if !alreadyClosed {
		shouldDisconnect = c.bridge.sessionManager.IsSessionActive(c.sessionID)
	}

	c.cancel()
	c.readBufClose.Do(func() {
		close(c.readBuf)
	})

	if alreadyClosed {
		return nil
	}

	c.bridge.mqttClient.Unsubscribe(c.downTopic)
	c.bridge.mqttClient.Unsubscribe(c.upTopic)

	sessionID := c.sessionID
	bridge := c.bridge
	if !shouldDisconnect {
		return nil
	}

	bridge.connCleanup.Add(1)
	go func() {
		defer bridge.connCleanup.Done()

		if err := bridge.DisconnectSession(sessionID); err != nil {
			bridge.logger.Error("Failed to disconnect session during close",
				zap.String("sessionID", sessionID),
				zap.Error(err))
		}
	}()

	return nil
}

func (c *MQTTNetBridgeConn) LocalAddr() net.Addr {
	return c.localAddr
}

func (c *MQTTNetBridgeConn) RemoteAddr() net.Addr {
	return c.remoteAddr
}

func (c *MQTTNetBridgeConn) SetDeadline(t time.Time) error {
	if err := c.SetReadDeadline(t); err != nil {
		return err
	}
	return c.SetWriteDeadline(t)
}

func (c *MQTTNetBridgeConn) SetReadDeadline(t time.Time) error {
	c.readMu.Lock()
	defer c.readMu.Unlock()

	c.deadline = t
	return nil
}

func (c *MQTTNetBridgeConn) SetWriteDeadline(t time.Time) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	c.wDeadline = t
	return nil
}
