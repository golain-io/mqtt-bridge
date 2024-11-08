package bridge

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"fmt"
	"reflect"
	"strings"

	"context"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"go.uber.org/zap"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	PM "github.com/golain-io/mqtt-bridge/proto"
)

const (
	topicPrefix = "grpc"
	topicUp     = "up"
	topicDown   = "down"
)

type MQTTBridge struct {
	mqttClient mqtt.Client
	logger     *zap.Logger

	// For service registration
	servicesRW      map[string]*serviceInfo
	servicesRWMutex sync.RWMutex

	// Active sessions
	sessions  map[string]*Session
	sessionMu sync.RWMutex

	// Timeout for session
	timeout time.Duration

	// Add fields for tracking service info
	serviceInfo map[string]grpc.ServiceInfo
}

type serviceInfo struct {
	// Contains the implementation for the methods in this service.
	serviceImpl any
	methods     map[string]*grpc.MethodDesc
	streams     map[string]*grpc.StreamDesc
	mdata       any
}

// SessionEvent represents different session lifecycle events
type SessionEvent int

const (
	SessionEventCreated SessionEvent = iota
	SessionEventActivated
	SessionEventStreamStarted
	SessionEventStreamEnded
	SessionEventClosing
	SessionEventClosed
	SessionEventTimeout
)

// SessionEventHandler is called when session state changes occur
type SessionEventHandler func(session *Session, event SessionEvent)

// Session represents an active MQTT-GRPC bridge session
type Session struct {
	ID          string
	ServiceName string
	LastActive  time.Time
	State       SessionState

	// Active streams for this session
	streams      map[string]*StreamContext
	streamsMutex sync.RWMutex

	// Stream cancellation
	cancelCtx  context.Context
	cancelFunc context.CancelFunc

	// Event handling
	eventHandler SessionEventHandler

	// Sequence number management
	sequenceNum uint64 // Atomic counter for message sequence numbers
}

// StreamContext holds the context for an active stream
type StreamContext struct {
	StreamID   string // Usually method name + unique identifier
	Method     string
	Created    time.Time
	LastActive time.Time
	State      StreamState
	Cancel     context.CancelFunc // For cancelling individual streams
	Context    context.Context
	Extra      interface{} // For storing the serverStream
}

type StreamState int

const (
	StreamStateActive StreamState = iota
	StreamStateClosing
	StreamStateClosed
)

type SessionState int

const (
	SessionStateActive SessionState = iota
	SessionStateClosing
	SessionStateClosed
)

// Ensure we implement the required interfaces
var (
	_ grpc.ServiceRegistrar          = (*MQTTBridge)(nil)
	_ reflection.ServiceInfoProvider = (*MQTTBridge)(nil)
)

// BuildTopicPath creates an MQTT topic path for a given service and method
func BuildTopicPath(pkg, service, method, sessionID, direction string) string {
	return fmt.Sprintf("%s/%s/%s/%s/%s/%s",
		topicPrefix, pkg, service, method, sessionID, direction)
}

// ParseTopicPath extracts components from an MQTT topic path
func ParseTopicPath(topic string) (pkg, service, method, sessionID, direction string, err error) {
	parts := strings.Split(topic, "/")
	if len(parts) < 6 || parts[0] != topicPrefix {
		return "", "", "", "", "", fmt.Errorf("invalid topic format: %s", topic)
	}

	// Calculate package path length (total parts - 5 required parts)
	pkgParts := len(parts) - 5

	// Reconstruct package path
	pkg = strings.Join(parts[1:1+pkgParts], ".")

	// Get remaining parts
	service = parts[1+pkgParts]
	method = parts[2+pkgParts]
	sessionID = parts[3+pkgParts]
	direction = parts[4+pkgParts]

	return pkg, service, method, sessionID, direction, nil
}

// RegisterService implements grpc.ServiceRegistrar
func (b *MQTTBridge) RegisterService(desc *grpc.ServiceDesc, impl interface{}) {
	b.servicesRWMutex.Lock()
	defer b.servicesRWMutex.Unlock()

	svcInfo := grpc.ServiceInfo{
		Methods:  make([]grpc.MethodInfo, 0),
		Metadata: desc.Metadata,
	}

	// Validate implementation
	ht := reflect.TypeOf(desc.HandlerType).Elem()
	st := reflect.TypeOf(impl)
	if !st.Implements(ht) {
		panic(fmt.Sprintf("service implementation does not implement %v", ht))
	}

	serviceName := desc.ServiceName
	if b.servicesRW == nil {
		b.servicesRW = make(map[string]*serviceInfo)
	}
	if b.serviceInfo == nil {
		b.serviceInfo = make(map[string]grpc.ServiceInfo)
	}

	// Create new serviceInfo
	info := &serviceInfo{
		serviceImpl: impl,
		methods:     make(map[string]*grpc.MethodDesc),
		streams:     make(map[string]*grpc.StreamDesc),
		mdata:       desc.Metadata,
	}

	// Register methods
	for _, method := range desc.Methods {
		info.methods[method.MethodName] = &method
		svcInfo.Methods = append(svcInfo.Methods, grpc.MethodInfo{
			Name:           method.MethodName,
			IsClientStream: false,
			IsServerStream: false,
		})
	}

	// Register streams
	for _, stream := range desc.Streams {
		info.streams[stream.StreamName] = &stream
		svcInfo.Methods = append(svcInfo.Methods, grpc.MethodInfo{
			Name:           stream.StreamName,
			IsClientStream: true,
			IsServerStream: true,
		})
	}

	b.servicesRW[serviceName] = info
	b.serviceInfo[serviceName] = svcInfo

	// Subscribe to service methods after registration
	err := b.subscribeToService(serviceName, info)
	if err != nil {
		b.logger.Error("Failed to subscribe to service methods", zap.Error(err), zap.String("service", serviceName))
	}
}

// GetServiceInfo implements grpc.ServiceInfoProvider
func (b *MQTTBridge) GetServiceInfo() map[string]grpc.ServiceInfo {
	b.servicesRWMutex.RLock()
	defer b.servicesRWMutex.RUnlock()

	// Return a copy to prevent concurrent map access
	info := make(map[string]grpc.ServiceInfo, len(b.serviceInfo))
	for k, v := range b.serviceInfo {
		info[k] = v
	}
	return info
}

// Subscribe to service methods after registration
func (b *MQTTBridge) subscribeToService(serviceName string, info *serviceInfo) error {
	// Extract package and service name (format: pkg.subpkg.ServiceName)
	parts := strings.Split(serviceName, ".")
	if len(parts) < 2 {
		return fmt.Errorf("invalid service name format: %s", serviceName)
	}

	// Last part is the service name, everything before is the package
	svc := parts[len(parts)-1]
	pkg := strings.Join(parts[:len(parts)-1], ".")

	// Subscribe to unary methods
	for methodName := range info.methods {
		topic := BuildTopicPath(pkg, svc, methodName, "+", topicDown)
		token := b.mqttClient.Subscribe(topic, 1, b.handleMessage)
		if token.Wait() && token.Error() != nil {
			return fmt.Errorf("failed to subscribe to %s: %v", topic, token.Error())
		}
		b.logger.Info("Subscribed to method",
			zap.String("topic", topic),
			zap.String("service", serviceName),
			zap.String("method", methodName))
	}

	// Subscribe to streaming methods
	for streamName := range info.streams {
		topic := BuildTopicPath(pkg, svc, streamName, "+", topicDown)
		token := b.mqttClient.Subscribe(topic, 1, b.handleMessage)
		if token.Wait() && token.Error() != nil {
			return fmt.Errorf("failed to subscribe to %s: %v", topic, token.Error())
		}
		b.logger.Info("Subscribed to stream",
			zap.String("topic", topic),
			zap.String("service", serviceName),
			zap.String("stream", streamName))
	}

	return nil
}

// Handle incoming MQTT messages
func (b *MQTTBridge) handleMessage(client mqtt.Client, msg mqtt.Message) {
	pkg, service, method, sessionID, direction, err := ParseTopicPath(msg.Topic())
	if err != nil {
		b.logger.Error("Failed to parse topic", zap.Error(err), zap.String("topic", msg.Topic()))
		return
	}

	// Ignore messages in the 'up' direction (they are responses)
	if direction == topicUp {
		return
	}

	serviceName := fmt.Sprintf("%s.%s", pkg, service)

	// Get service info
	b.servicesRWMutex.RLock()
	svcInfo, exists := b.servicesRW[serviceName]
	b.servicesRWMutex.RUnlock()

	if !exists {
		b.logger.Error("Service not found", zap.String("service", serviceName))
		b.sendError(pkg, service, method, sessionID, status.Error(codes.NotFound, "service not found"))
		return
	}

	// Create or get session
	session := b.getOrCreateSession(sessionID, serviceName)

	// Parse the frame
	frame, err := UnmarshalFrame(msg.Payload())
	if err != nil {
		b.logger.Error("Failed to unmarshal frame", zap.Error(err))
		b.sendError(pkg, service, method, sessionID, status.Error(codes.Internal, "failed to unmarshal frame"))
		return
	}

	// Handle based on message type
	switch frame.Header.Type {
	case MessageTypeData:
		if methodDesc, ok := svcInfo.methods[method]; ok {
			go b.handleUnaryCall(pkg, service, methodDesc, session, frame)
		} else if streamDesc, ok := svcInfo.streams[method]; ok {
			go b.handleStreamMessage(pkg, service, streamDesc, session, frame)
		} else {
			b.sendError(pkg, service, method, sessionID, status.Error(codes.NotFound, "method not found"))
		}

	case MessageTypeStreamInit:
		if streamDesc, ok := svcInfo.streams[method]; ok {
			go b.handleStreamInit(pkg, service, streamDesc, session, frame)
		} else {
			b.sendError(pkg, service, method, sessionID, status.Error(codes.NotFound, "stream not found"))
		}

	case MessageTypeStreamEnd:
		if streamDesc, ok := svcInfo.streams[method]; ok {
			go b.handleStreamEnd(pkg, service, streamDesc, session, frame)
		}
	}
}

// Send a response message
func (b *MQTTBridge) sendResponse(pkg, service, method, sessionID string, frame Frame) error {
	topic := BuildTopicPath(pkg, service, method, sessionID, topicUp)

	data := frame.Marshal()
	token := b.mqttClient.Publish(topic, 1, false, data)
	token.Wait()
	return token.Error()
}

// Send an error message
func (b *MQTTBridge) sendError(pkg, service, method, sessionID string, err error) {
	st, ok := status.FromError(err)
	if !ok {
		st = status.New(codes.Unknown, err.Error())
	}

	statusProto := st.Proto()
	statusData, err := proto.Marshal(statusProto)
	if err != nil {
		b.logger.Error("Failed to marshal status", zap.Error(err))
		return
	}

	frame, err := NewFrame(MessageTypeError, 0, statusData)
	if err != nil {
		b.logger.Error("Failed to create error frame", zap.Error(err))
		return
	}

	if err := b.sendResponse(pkg, service, method, sessionID, *frame); err != nil {
		b.logger.Error("Failed to send error response", zap.Error(err))
	}
}

// NewSession creates a new session with proper initialization
func NewSession(id, serviceName string, eventHandler SessionEventHandler) *Session {
	ctx, cancel := context.WithCancel(context.Background())
	session := &Session{
		ID:           id,
		ServiceName:  serviceName,
		LastActive:   time.Now(),
		State:        SessionStateActive,
		streams:      make(map[string]*StreamContext),
		cancelCtx:    ctx,
		cancelFunc:   cancel,
		eventHandler: eventHandler,
		sequenceNum:  MinSequenceNum, // Initialize sequence number
	}

	if eventHandler != nil {
		eventHandler(session, SessionEventCreated)
	}

	return session
}

// AddStream adds a new stream to the session
func (s *Session) AddStream(methodName string) (*StreamContext, error) {
	s.streamsMutex.Lock()
	defer s.streamsMutex.Unlock()

	if s.State != SessionStateActive {
		return nil, fmt.Errorf("session is not active")
	}

	streamID := fmt.Sprintf("%s-%s", methodName, uuid.New().String())
	ctx, cancel := context.WithCancel(s.cancelCtx)

	streamCtx := &StreamContext{
		StreamID:   streamID,
		Method:     methodName,
		Created:    time.Now(),
		LastActive: time.Now(),
		State:      StreamStateActive,
		Cancel:     cancel,
		Context:    ctx,
	}

	s.streams[streamID] = streamCtx

	if s.eventHandler != nil {
		s.eventHandler(s, SessionEventStreamStarted)
	}

	return streamCtx, nil
}

// CloseStream gracefully closes a stream
func (s *Session) CloseStream(streamID string) error {
	s.streamsMutex.Lock()
	defer s.streamsMutex.Unlock()

	stream, exists := s.streams[streamID]
	if !exists {
		return fmt.Errorf("stream not found: %s", streamID)
	}

	if stream.State != StreamStateActive {
		return nil // Already closing or closed
	}

	stream.State = StreamStateClosing
	stream.Cancel() // Cancel the stream context
	delete(s.streams, streamID)
	stream.State = StreamStateClosed

	if s.eventHandler != nil {
		s.eventHandler(s, SessionEventStreamEnded)
	}

	return nil
}

// UpdateActivity updates the last active timestamp for the session and stream
func (s *Session) UpdateActivity(streamID string) {
	s.LastActive = time.Now()

	if streamID != "" {
		s.streamsMutex.Lock()
		if stream, exists := s.streams[streamID]; exists {
			stream.LastActive = time.Now()
		}
		s.streamsMutex.Unlock()
	}
}

// Close closes all streams and the session itself
func (s *Session) Close() {
	s.streamsMutex.Lock()
	s.State = SessionStateClosing

	// Close all active streams
	for id, stream := range s.streams {
		if stream.State == StreamStateActive {
			stream.State = StreamStateClosing
			stream.Cancel()
			delete(s.streams, id)
			stream.State = StreamStateClosed
		}
	}
	s.streamsMutex.Unlock()

	// Cancel the session context
	s.cancelFunc()
	s.State = SessionStateClosed

	if s.eventHandler != nil {
		s.eventHandler(s, SessionEventClosed)
	}
}

// Now let's update the bridge's session management
func (b *MQTTBridge) getOrCreateSession(sessionID, serviceName string) *Session {
	b.sessionMu.Lock()
	defer b.sessionMu.Unlock()

	// Check for existing session
	if session, exists := b.sessions[sessionID]; exists {
		session.UpdateActivity("")
		return session
	}

	// Create new session with event handler
	session := NewSession(sessionID, serviceName, func(s *Session, event SessionEvent) {
		b.handleSessionEvent(s, event)
	})

	if b.sessions == nil {
		b.sessions = make(map[string]*Session)
	}
	b.sessions[sessionID] = session

	// Start session cleanup goroutine
	go b.monitorSession(sessionID)

	return session
}

// handleSessionEvent processes session lifecycle events
func (b *MQTTBridge) handleSessionEvent(session *Session, event SessionEvent) {
	switch event {
	case SessionEventCreated:
		b.logger.Info("Session created",
			zap.String("session_id", session.ID),
			zap.String("service", session.ServiceName))

	case SessionEventStreamStarted:
		b.logger.Info("Stream started",
			zap.String("session_id", session.ID))

	case SessionEventStreamEnded:
		b.logger.Info("Stream ended",
			zap.String("session_id", session.ID))

	case SessionEventTimeout:
		b.logger.Warn("Session timed out",
			zap.String("session_id", session.ID))

	case SessionEventClosed:
		b.logger.Info("Session closed",
			zap.String("session_id", session.ID))
	}
}

// monitorSession watches for session timeout
func (b *MQTTBridge) monitorSession(sessionID string) {
	ticker := time.NewTicker(b.timeout / 2)
	defer ticker.Stop()

	for range ticker.C {
		b.sessionMu.RLock()
		session, exists := b.sessions[sessionID]
		if !exists {
			b.sessionMu.RUnlock()
			return
		}

		if time.Since(session.LastActive) > b.timeout {
			b.sessionMu.RUnlock()
			b.closeSession(sessionID)
			return
		}
		b.sessionMu.RUnlock()
	}
}

// closeSession handles cleanup of an expired or closed session
func (b *MQTTBridge) closeSession(sessionID string) {
	b.sessionMu.Lock()
	defer b.sessionMu.Unlock()

	session, exists := b.sessions[sessionID]
	if !exists {
		return
	}

	session.Close() // This will trigger the appropriate events
	delete(b.sessions, sessionID)
}

// handleUnaryCall processes a unary RPC call
func (b *MQTTBridge) handleUnaryCall(pkg, service string, methodDesc *grpc.MethodDesc, session *Session, frame Frame) {
	ctx := context.Background()
	serviceImpl := b.servicesRW[fmt.Sprintf("%s.%s", pkg, service)].serviceImpl

	// Call the handler
	resp, err := methodDesc.Handler(serviceImpl, ctx, func(req interface{}) error {
		return proto.Unmarshal(frame.Data, req.(proto.Message))
	}, nil)

	if err != nil {
		b.sendError(pkg, service, methodDesc.MethodName, session.ID, err)
		return
	}

	// Marshal the response
	respData, err := proto.Marshal(resp.(proto.Message))
	if err != nil {
		b.sendError(pkg, service, methodDesc.MethodName, session.ID,
			status.Errorf(codes.Internal, "failed to marshal response: %v", err))
		return
	}

	// Create response frame
	responseFrame, err := NewFrame(MessageTypeData,
		atomic.AddUint64(&session.sequenceNum, 1),
		respData)
	if err != nil {
		b.sendError(pkg, service, methodDesc.MethodName, session.ID, err)
		return
	}

	// Send response
	if err := b.sendResponse(pkg, service, methodDesc.MethodName, session.ID, *responseFrame); err != nil {
		b.logger.Error("Failed to send response",
			zap.Error(err),
			zap.String("session", session.ID))
	}
}

// handleStreamInit initializes a new stream
func (b *MQTTBridge) handleStreamInit(pkg, service string, streamDesc *grpc.StreamDesc, session *Session, frame Frame) {
	// Create new stream context
	streamCtx, err := session.AddStream(streamDesc.StreamName)
	if err != nil {
		b.sendError(pkg, service, streamDesc.StreamName, session.ID, status.Error(codes.Internal, "failed to create stream"))
		return
	}

	// Create server stream
	stream := newServerStream(streamCtx.Context, b, pkg, service, streamDesc.StreamName, session, streamCtx.StreamID)

	// Store the server stream in the context
	streamCtx.Extra = stream

	// Start stream handler in goroutine
	go func() {
		defer session.CloseStream(streamCtx.StreamID)

		// Get the service implementation
		b.servicesRWMutex.RLock()
		svcInfo := b.servicesRW[fmt.Sprintf("%s.%s", pkg, service)]
		b.servicesRWMutex.RUnlock()

		err := streamDesc.Handler(svcInfo.serviceImpl, stream)
		if err != nil {
			b.sendError(pkg, service, streamDesc.StreamName, session.ID, err)
		}
	}()

	// Send stream init acknowledgment
	ackFrame, err := NewFrame(MessageTypeStreamInit,
		atomic.AddUint64(&session.sequenceNum, 1),
		nil,
		WithStreamID(streamCtx.StreamID))
	if err != nil {
		b.logger.Error("Failed to create stream init ack frame",
			zap.Error(err),
			zap.String("session", session.ID))
		return
	}

	if err := b.sendResponse(pkg, service, streamDesc.StreamName, session.ID, *ackFrame); err != nil {
		b.logger.Error("Failed to send stream init ack",
			zap.Error(err),
			zap.String("session", session.ID))
	}
}

// handleStreamMessage processes messages for an existing stream
func (b *MQTTBridge) handleStreamMessage(pkg, service string, streamDesc *grpc.StreamDesc, session *Session, frame Frame) {
	// Find the active stream
	session.streamsMutex.RLock()
	streamCtx, exists := session.streams[frame.Header.StreamID]
	session.streamsMutex.RUnlock()

	if !exists {
		b.sendError(pkg, service, streamDesc.StreamName, session.ID,
			status.Error(codes.NotFound, "stream not found"))
		return
	}

	session.UpdateActivity(streamCtx.StreamID)

	// Get the server stream
	stream, ok := streamCtx.Extra.(*serverStream)
	if !ok {
		b.logger.Error("Stream context does not contain serverStream",
			zap.String("session", session.ID),
			zap.String("stream", streamCtx.StreamID))
		return
	}

	// Route the message to the stream's receive buffer
	select {
	case stream.recvBuf <- &frame:
		// Message successfully queued
	default:
		// Buffer is full, log warning and drop message
		b.logger.Warn("Stream receive buffer full, dropping message",
			zap.String("session", session.ID),
			zap.String("stream", streamCtx.StreamID))
	}
}

// handleStreamEnd closes an existing stream
func (b *MQTTBridge) handleStreamEnd(pkg, service string, streamDesc *grpc.StreamDesc, session *Session, frame Frame) {
	err := session.CloseStream(frame.Header.StreamID)
	if err != nil {
		b.logger.Error("Failed to close stream",
			zap.Error(err),
			zap.String("session", session.ID),
			zap.String("stream", frame.Header.StreamID))
	}
}

// serverStream implements grpc.ServerStream
type serverStream struct {
	ctx      context.Context
	bridge   *MQTTBridge
	pkg      string
	service  string
	method   string
	session  *Session
	streamID string

	// Message receiving
	recvMu  sync.Mutex
	recvBuf chan *Frame

	// Metadata handling
	header   metadata.MD
	trailer  metadata.MD
	headerMu sync.Mutex
}

func newServerStream(ctx context.Context, bridge *MQTTBridge, pkg, service, method string, session *Session, streamID string) *serverStream {
	return &serverStream{
		ctx:      ctx,
		bridge:   bridge,
		pkg:      pkg,
		service:  service,
		method:   method,
		session:  session,
		streamID: streamID,
		recvBuf:  make(chan *Frame, 100), // Buffered channel for receiving messages
		header:   metadata.MD{},
		trailer:  metadata.MD{},
	}
}

func (s *serverStream) SetHeader(md metadata.MD) error {
	s.headerMu.Lock()
	defer s.headerMu.Unlock()

	if s.header != nil {
		return status.Error(codes.Internal, "header already sent")
	}

	s.header = metadata.Join(s.header, md)
	return nil
}

func (s *serverStream) SendHeader(md metadata.MD) error {
	s.headerMu.Lock()
	defer s.headerMu.Unlock()

	if s.header != nil {
		return status.Error(codes.Internal, "header already sent")
	}

	// Convert metadata to our protobuf format
	mf := &PM.MetadataFrame{
		Metadata: make(map[string]*PM.MetadataValues),
	}

	for k, vs := range md {
		mf.Metadata[k] = &PM.MetadataValues{
			Values: vs,
		}
	}

	// Marshal metadata
	headerData, err := proto.Marshal(mf)
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal header: %v", err)
	}

	frame, err := NewFrame(MessageTypeHeader,
		atomic.AddUint64(&s.session.sequenceNum, 1),
		headerData,
		WithStreamID(s.streamID))
	if err != nil {
		return status.Errorf(codes.Internal, "failed to create header frame: %v", err)
	}

	if err := s.bridge.sendResponse(s.pkg, s.service, s.method, s.session.ID, *frame); err != nil {
		return status.Errorf(codes.Internal, "failed to send header: %v", err)
	}

	s.header = md
	return nil
}

func (s *serverStream) SetTrailer(md metadata.MD) {
	s.headerMu.Lock()
	defer s.headerMu.Unlock()
	s.trailer = metadata.Join(s.trailer, md)
}

func (s *serverStream) Context() context.Context {
	return s.ctx
}

func (s *serverStream) SendMsg(m interface{}) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}

	data, err := proto.Marshal(m.(proto.Message))
	if err != nil {
		return status.Errorf(codes.Internal, "failed to marshal message: %v", err)
	}

	frames := FrameMessage(data, atomic.AddUint64(&s.session.sequenceNum, 1), MessageTypeData)
	for _, frame := range frames {
		frame.Header.StreamID = s.streamID
		if err := s.bridge.sendResponse(s.pkg, s.service, s.method, s.session.ID, frame); err != nil {
			return status.Errorf(codes.Internal, "failed to send message: %v", err)
		}
	}

	return nil
}

func (s *serverStream) RecvMsg(m interface{}) error {
	if err := s.ctx.Err(); err != nil {
		return err
	}

	s.recvMu.Lock()
	defer s.recvMu.Unlock()

	select {
	case frame := <-s.recvBuf:
		if frame == nil {
			return io.EOF
		}

		// Handle error frames
		if frame.Header.Type == MessageTypeError {
			st := &spb.Status{}
			if err := proto.Unmarshal(frame.Data, st); err != nil {
				return status.Error(codes.Internal, "failed to unmarshal error status")
			}
			return status.FromProto(st).Err()
		}

		// Unmarshal the message
		if err := proto.Unmarshal(frame.Data, m.(proto.Message)); err != nil {
			return status.Errorf(codes.Internal, "failed to unmarshal message: %v", err)
		}
		return nil

	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func NewMQTTBridge(mqttClient mqtt.Client, logger *zap.Logger, timeout time.Duration) *MQTTBridge {
	return &MQTTBridge{
		mqttClient:  mqttClient,
		logger:      logger,
		servicesRW:  make(map[string]*serviceInfo),
		sessions:    make(map[string]*Session),
		serviceInfo: make(map[string]grpc.ServiceInfo),
		timeout:     timeout,
	}
}
