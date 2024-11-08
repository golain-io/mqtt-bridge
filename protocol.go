package bridge

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"
)

var (
	MaxFragmentSize = 10 * 1024 // 10KB per fragment
	HeaderSize      = 16        // Fixed header size

	// Pool for frame marshaling
	frameBufferPool = sync.Pool{
		New: func() interface{} {
			return new(bytes.Buffer)
		},
	}

	// Pool for header marshaling
	headerBufferPool = sync.Pool{
		New: func() interface{} {
			// Pre-allocate with reasonable capacity for header
			return bytes.NewBuffer(make([]byte, 0, MinHeaderSize+256)) // 256 bytes extra for StreamID
		},
	}

	// Pool for large message assembly
	largeBufferPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer(make([]byte, 0, MaxFragmentSize))
		},
	}
)

func SetMaxFragmentSize(size int) {
	MaxFragmentSize = size
}

type MessageType uint8

const (
	MessageTypeData MessageType = iota
	MessageTypeStreamInit
	MessageTypeStreamEnd
	MessageTypeHeader
	MessageTypeError
)

type Header struct {
	Type           MessageType // 1 byte
	SequenceNumber uint64      // 8 bytes
	FragmentID     uint16      // 2 bytes
	FragmentTotal  uint16      // 2 bytes
	FragmentSeq    uint16      // 2 bytes
	IsLastFragment bool        // 1 byte
	StreamID       string      // Variable length - we'll need to adjust marshaling/unmarshaling
}

const (
	MinHeaderSize          = 16        // Fixed header size without StreamID
	MaxStreamIDSize        = 256       // Maximum length for StreamID
	MinSequenceNum         = 1         // Minimum sequence number
	MaxSequenceNum  uint64 = 1<<64 - 1 // Maximum sequence number (uint64 max)
)

// validateStreamID checks if a StreamID is valid
func validateStreamID(streamID string) error {
	// Empty StreamID is allowed (for unary calls)
	if streamID == "" {
		return nil
	}

	if len(streamID) > MaxStreamIDSize {
		return fmt.Errorf("stream ID exceeds maximum length of %d bytes", MaxStreamIDSize)
	}
	return nil
}

// NewFrame creates a new Frame with validated parameters
func NewFrame(msgType MessageType, seqNum uint64, data []byte, opts ...FrameOption) (*Frame, error) {
	if seqNum < MinSequenceNum || seqNum > MaxSequenceNum {
		return nil, fmt.Errorf("sequence number out of range [%d, %d]", MinSequenceNum, MaxSequenceNum)
	}

	frame := &Frame{
		Header: &Header{
			Type:           msgType,
			SequenceNumber: seqNum,
			FragmentID:     0,
			FragmentTotal:  1,
			FragmentSeq:    0,
			IsLastFragment: true,
			StreamID:       "", // Will be set by options if needed
		},
		Data: data,
	}

	// Apply options
	for _, opt := range opts {
		if err := opt(frame); err != nil {
			return nil, fmt.Errorf("failed to apply frame option: %w", err)
		}
	}

	return frame, nil
}

// FrameOption defines options for frame creation
type FrameOption func(*Frame) error

// WithStreamID sets the StreamID for a frame
func WithStreamID(streamID string) FrameOption {
	return func(f *Frame) error {
		if err := validateStreamID(streamID); err != nil {
			return err
		}
		f.Header.StreamID = streamID
		return nil
	}
}

// WithFragmentation sets fragmentation parameters for a frame
func WithFragmentation(fragmentID uint16, total uint16, seq uint16, isLast bool) FrameOption {
	return func(f *Frame) error {
		if total == 0 {
			return errors.New("fragment total cannot be zero")
		}
		if seq >= total {
			return errors.New("fragment sequence cannot be greater than or equal to total")
		}
		f.Header.FragmentID = fragmentID
		f.Header.FragmentTotal = total
		f.Header.FragmentSeq = seq
		f.Header.IsLastFragment = isLast
		return nil
	}
}

func (h *Header) marshal() []byte {
	// Validate StreamID before marshaling
	if err := validateStreamID(h.StreamID); err != nil {
		panic(fmt.Sprintf("invalid stream ID in header: %v", err))
	}

	buf := headerBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer headerBufferPool.Put(buf)

	// Write fixed fields
	buf.WriteByte(byte(h.Type))
	binary.Write(buf, binary.BigEndian, h.SequenceNumber)
	binary.Write(buf, binary.BigEndian, h.FragmentID)
	binary.Write(buf, binary.BigEndian, h.FragmentTotal)
	binary.Write(buf, binary.BigEndian, h.FragmentSeq)
	if h.IsLastFragment {
		buf.WriteByte(1)
	} else {
		buf.WriteByte(0)
	}

	// Write StreamID length and value (even if empty)
	streamIDBytes := []byte(h.StreamID)
	binary.Write(buf, binary.BigEndian, uint16(len(streamIDBytes)))
	if len(streamIDBytes) > 0 {
		buf.Write(streamIDBytes)
	}

	// Make a copy since we're recycling the buffer
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result
}

func unmarshalHeader(data []byte) (*Header, error) {
	if len(data) < HeaderSize {
		return nil, errors.New("data too short for header")
	}

	pos := 0
	h := &Header{
		Type:           MessageType(data[pos]),
		SequenceNumber: binary.BigEndian.Uint64(data[pos+1:]),
		FragmentID:     binary.BigEndian.Uint16(data[pos+9:]),
		FragmentTotal:  binary.BigEndian.Uint16(data[pos+11:]),
		FragmentSeq:    binary.BigEndian.Uint16(data[pos+13:]),
		IsLastFragment: data[pos+15] == 1,
	}
	pos += HeaderSize

	// Read StreamID length
	if len(data) < pos+2 {
		return nil, errors.New("data too short for StreamID length")
	}
	streamIDLen := binary.BigEndian.Uint16(data[pos:])
	pos += 2

	// Read StreamID
	if len(data) < pos+int(streamIDLen) {
		return nil, errors.New("data too short for StreamID")
	}
	h.StreamID = string(data[pos : pos+int(streamIDLen)])

	return h, nil
}

// Message wraps the actual data with ordering information
type message struct {
	SequenceNumber uint64
	Data           []byte
}

func (m *message) marshal() []byte {
	buf := frameBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer frameBufferPool.Put(buf)

	// Pre-allocate expected size
	buf.Grow(8 + len(m.Data))

	// Write sequence number
	binary.Write(buf, binary.BigEndian, m.SequenceNumber)
	buf.Write(m.Data)

	// Make a copy since we're recycling the buffer
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result
}

func unmarshalMessage(data []byte) (*message, error) {
	if len(data) < 8 {
		return nil, &BridgeError{Op: "unmarshal", Err: errors.New("message too short")}
	}
	return &message{
		SequenceNumber: binary.BigEndian.Uint64(data[:8]),
		Data:           data[8:],
	}, nil
}

type fragmentBuffer struct {
	fragments     map[uint16][]byte
	totalSize     int
	fragmentTotal uint16
	lastUpdate    time.Time
}

type fragmentManager struct {
	mu      sync.Mutex
	buffers map[uint64]*fragmentBuffer // keyed by FragmentID
	timeout time.Duration
}

func newFragmentManager(timeout time.Duration) *fragmentManager {
	return &fragmentManager{
		buffers: make(map[uint64]*fragmentBuffer),
		timeout: timeout,
	}
}

func (fm *fragmentManager) addFragment(header *Header, data []byte) ([]byte, bool, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	// Clean up old fragments
	fm.cleanup()

	buffer, exists := fm.buffers[header.SequenceNumber]
	if !exists {
		buffer = &fragmentBuffer{
			fragments:     make(map[uint16][]byte),
			fragmentTotal: header.FragmentTotal,
			lastUpdate:    time.Now(),
		}
		fm.buffers[header.SequenceNumber] = buffer
	}

	// Validate fragment
	if header.FragmentSeq >= buffer.fragmentTotal {
		return nil, false, errors.New("invalid fragment sequence")
	}

	// Store fragment
	buffer.fragments[header.FragmentSeq] = data
	buffer.totalSize += len(data)
	buffer.lastUpdate = time.Now()

	// Check if we have all fragments
	if len(buffer.fragments) == int(buffer.fragmentTotal) {
		buf := largeBufferPool.Get().(*bytes.Buffer)
		buf.Reset()
		defer largeBufferPool.Put(buf)

		buf.Grow(buffer.totalSize)

		for i := uint16(0); i < buffer.fragmentTotal; i++ {
			buf.Write(buffer.fragments[i])
		}

		// Clean up
		delete(fm.buffers, header.SequenceNumber)

		// Make a copy since we're recycling the buffer
		result := make([]byte, buf.Len())
		copy(result, buf.Bytes())
		return result, true, nil
	}

	return nil, false, nil
}

func (fm *fragmentManager) cleanup() {
	now := time.Now()
	for seq, buffer := range fm.buffers {
		if now.Sub(buffer.lastUpdate) > fm.timeout {
			delete(fm.buffers, seq)
		}
	}
}

// Frame represents a complete protocol frame including header and payload
type Frame struct {
	Header *Header
	Data   []byte
}

// FrameMessage splits a large message into frames
func FrameMessage(data []byte, seqNum uint64, msgType MessageType) []Frame {
	if len(data) <= MaxFragmentSize {
		frame, _ := NewFrame(msgType, seqNum, data)
		return []Frame{*frame}
	}

	fragmentID := uint16(0)
	remaining := data
	fragmentTotal := uint16((len(data) + MaxFragmentSize - 1) / MaxFragmentSize)
	frames := make([]Frame, 0, fragmentTotal)
	for len(remaining) > 0 {
		size := MaxFragmentSize
		if len(remaining) < size {
			size = len(remaining)
		}

		frame, _ := NewFrame(msgType, seqNum, remaining[:size],
			WithFragmentation(fragmentID, fragmentTotal, fragmentID, len(remaining) <= size))

		frames = append(frames, *frame)
		remaining = remaining[size:]
		fragmentID++
	}

	return frames
}

// MarshalFrame converts a Frame into a single byte slice ready for transmission
func (f *Frame) Marshal() []byte {
	buf := frameBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer frameBufferPool.Put(buf)

	// Marshal header first to get its size
	headerBytes := f.Header.marshal()

	// Pre-allocate the full size
	totalSize := len(headerBytes) + len(f.Data)
	buf.Grow(totalSize)

	// Write header and data
	buf.Write(headerBytes)
	buf.Write(f.Data)

	// Make a copy since we're recycling the buffer
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return result
}

// Add a new helper for creating large messages
func NewLargeMessage(size int) *bytes.Buffer {
	buf := largeBufferPool.Get().(*bytes.Buffer)
	buf.Reset()
	buf.Grow(size)
	return buf
}

// Add a helper to return large message buffers to the pool
func ReleaseLargeMessage(buf *bytes.Buffer) {
	largeBufferPool.Put(buf)
}

// UnmarshalFrame converts a byte slice into a Frame
func UnmarshalFrame(data []byte) (Frame, error) {
	if len(data) < MinHeaderSize {
		return Frame{}, errors.New("data too short for frame")
	}

	// Read StreamID length from the fixed header position
	streamIDLen := binary.BigEndian.Uint16(data[MinHeaderSize : MinHeaderSize+2])

	// Validate StreamID length
	if streamIDLen > MaxStreamIDSize {
		return Frame{}, fmt.Errorf("stream ID length exceeds maximum of %d bytes", MaxStreamIDSize)
	}

	// Calculate total header size including StreamID
	totalHeaderSize := MinHeaderSize + 2 + int(streamIDLen)

	if len(data) < totalHeaderSize {
		return Frame{}, errors.New("data too short for frame with StreamID")
	}

	// Unmarshal header
	header, err := unmarshalHeader(data[:totalHeaderSize])
	if err != nil {
		return Frame{}, fmt.Errorf("failed to unmarshal header: %w", err)
	}

	// Extract payload
	payload := data[totalHeaderSize:]

	return Frame{
		Header: header,
		Data:   payload,
	}, nil
}

// Helper function to calculate the total header size for a given StreamID
func calculateHeaderSize(streamID string) int {
	return MinHeaderSize + 2 + len(streamID) // MinHeaderSize + uint16 length + StreamID bytes
}
