package bridge

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"
)

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
	MaxFragmentSize        = 10 * 1024 // 10KB per fragment
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

// appendHeader appends the wire encoding of h to dst and returns the extended
// slice. Layout: Type(1) | SequenceNumber(8) | FragmentID(2) | FragmentTotal(2)
// | FragmentSeq(2) | IsLastFragment(1) | len(StreamID)(2) | StreamID.
func appendHeader(dst []byte, h *Header) []byte {
	if err := validateStreamID(h.StreamID); err != nil {
		panic(fmt.Sprintf("invalid stream ID in header: %v", err))
	}

	var fixed [MinHeaderSize + 2]byte
	fixed[0] = byte(h.Type)
	binary.BigEndian.PutUint64(fixed[1:], h.SequenceNumber)
	binary.BigEndian.PutUint16(fixed[9:], h.FragmentID)
	binary.BigEndian.PutUint16(fixed[11:], h.FragmentTotal)
	binary.BigEndian.PutUint16(fixed[13:], h.FragmentSeq)
	if h.IsLastFragment {
		fixed[15] = 1
	}
	binary.BigEndian.PutUint16(fixed[16:], uint16(len(h.StreamID)))

	dst = append(dst, fixed[:]...)
	dst = append(dst, h.StreamID...)
	return dst
}

func (h *Header) marshal() []byte {
	return appendHeader(make([]byte, 0, calculateHeaderSize(h.StreamID)), h)
}

func unmarshalHeader(data []byte) (*Header, error) {
	if len(data) < MinHeaderSize {
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
	pos += MinHeaderSize

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
	buf := make([]byte, 8+len(m.Data))
	binary.BigEndian.PutUint64(buf, m.SequenceNumber)
	copy(buf[8:], m.Data)
	return buf
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
	mu          sync.Mutex
	buffers     map[uint64]*fragmentBuffer // keyed by FragmentID
	timeout     time.Duration
	lastCleanup time.Time
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

	now := time.Now()

	// ponytail: cleanup is throttled to at most once per timeout window instead
	// of scanning every map on every fragment. Ceiling: stale buffers may linger
	// up to ~2x timeout; upgrade path = a per-buffer timer.
	if now.Sub(fm.lastCleanup) > fm.timeout {
		fm.cleanupExpired(now)
		fm.lastCleanup = now
	}

	buffer, exists := fm.buffers[header.SequenceNumber]
	if !exists {
		buffer = &fragmentBuffer{
			fragments:     make(map[uint16][]byte),
			fragmentTotal: header.FragmentTotal,
			lastUpdate:    now,
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
	buffer.lastUpdate = now

	// Check if we have all fragments
	if len(buffer.fragments) == int(buffer.fragmentTotal) {
		tmp := getBuf(buffer.totalSize)[:0]
		for i := uint16(0); i < buffer.fragmentTotal; i++ {
			tmp = append(tmp, buffer.fragments[i]...)
		}

		delete(fm.buffers, header.SequenceNumber)

		// result escapes to the caller, so it must be owned memory.
		result := make([]byte, len(tmp))
		copy(result, tmp)
		putBuf(tmp)
		return result, true, nil
	}

	return nil, false, nil
}

func (fm *fragmentManager) cleanup() {
	fm.cleanupExpired(time.Now())
}

func (fm *fragmentManager) cleanupExpired(now time.Time) {
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

// appendTo appends the wire encoding of the frame (header + data) to dst.
func (f *Frame) appendTo(dst []byte) []byte {
	dst = appendHeader(dst, f.Header)
	dst = append(dst, f.Data...)
	return dst
}

// Marshal converts a Frame into a single byte slice ready for transmission.
// The returned slice is caller-owned (not pooled).
func (f *Frame) Marshal() []byte {
	return f.appendTo(make([]byte, 0, calculateHeaderSize(f.Header.StreamID)+len(f.Data)))
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
