package bridge

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func TestFragmentManager(t *testing.T) {
	t.Run("successfully reassemble fragments", func(t *testing.T) {
		fm := newFragmentManager(time.Second * 10)

		// Create test data
		data1 := []byte("Hello")
		data2 := []byte(" World")

		header1 := &Header{
			SequenceNumber: 1,
			FragmentSeq:    0,
			FragmentTotal:  2,
		}

		header2 := &Header{
			SequenceNumber: 1,
			FragmentSeq:    1,
			FragmentTotal:  2,
		}

		// Add first fragment
		complete, done, err := fm.addFragment(header1, data1)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if done {
			t.Error("shouldn't be done after first fragment")
		}
		if complete != nil {
			t.Error("shouldn't return data until all fragments received")
		}

		// Add second fragment
		complete, done, err = fm.addFragment(header2, data2)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !done {
			t.Error("should be done after second fragment")
		}

		expected := []byte("Hello World")
		if string(complete) != string(expected) {
			t.Errorf("got %s, want %s", string(complete), string(expected))
		}
	})

	t.Run("invalid fragment sequence", func(t *testing.T) {
		fm := newFragmentManager(time.Second * 10)

		header := &Header{
			SequenceNumber: 1,
			FragmentSeq:    2, // Invalid sequence (>= total)
			FragmentTotal:  2,
		}

		_, _, err := fm.addFragment(header, []byte("test"))
		if err == nil {
			t.Error("expected error for invalid fragment sequence")
		}
	})

	t.Run("cleanup expired fragments", func(t *testing.T) {
		timeout := time.Millisecond * 100
		fm := newFragmentManager(timeout)

		header := &Header{
			SequenceNumber: 1,
			FragmentSeq:    0,
			FragmentTotal:  2,
		}

		// Add one fragment
		fm.addFragment(header, []byte("test"))

		// Verify buffer exists
		if len(fm.buffers) != 1 {
			t.Error("buffer should exist before timeout")
		}

		// Wait for timeout
		time.Sleep(timeout * 2)

		// Force cleanup
		fm.cleanup()

		// Verify buffer was cleaned up
		if len(fm.buffers) != 0 {
			t.Error("buffer should be cleaned up after timeout")
		}
	})
}

func TestFrameMessage(t *testing.T) {
	tests := []struct {
		name           string
		input          []byte
		sequenceNum    uint64
		messageType    MessageType
		expectedFrames int
		lastFrameSize  int
	}{
		{
			name:           "empty message",
			input:          []byte{},
			sequenceNum:    1,
			messageType:    MessageTypeData,
			expectedFrames: 1,
			lastFrameSize:  0,
		},
		{
			name:           "small message",
			input:          []byte("Hello, World!"),
			sequenceNum:    2,
			messageType:    MessageTypeData,
			expectedFrames: 1,
			lastFrameSize:  13,
		},
		{
			name:           "exact fragment size",
			input:          bytes.Repeat([]byte("a"), MaxFragmentSize),
			sequenceNum:    3,
			messageType:    MessageTypeData,
			expectedFrames: 1,
			lastFrameSize:  MaxFragmentSize,
		},
		{
			name:           "multi fragment",
			input:          bytes.Repeat([]byte("b"), MaxFragmentSize*2+100),
			sequenceNum:    4,
			messageType:    MessageTypeData,
			expectedFrames: 3,
			lastFrameSize:  100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frames := FrameMessage(tt.input, tt.sequenceNum, tt.messageType)

			// Check number of frames
			if len(frames) != tt.expectedFrames {
				t.Errorf("got %d frames, want %d", len(frames), tt.expectedFrames)
			}

			// Verify sequence
			var totalData []byte
			for i, frame := range frames {
				// Check header fields
				if frame.Header.SequenceNumber != tt.sequenceNum {
					t.Errorf("frame %d: got sequence %d, want %d",
						i, frame.Header.SequenceNumber, tt.sequenceNum)
				}
				if frame.Header.FragmentTotal != uint16(tt.expectedFrames) {
					t.Errorf("frame %d: got total %d, want %d",
						i, frame.Header.FragmentTotal, tt.expectedFrames)
				}
				if frame.Header.FragmentSeq != uint16(i) {
					t.Errorf("frame %d: got seq %d, want %d",
						i, frame.Header.FragmentSeq, i)
				}
				if frame.Header.Type != tt.messageType {
					t.Errorf("frame %d: got type %d, want %d",
						i, frame.Header.Type, tt.messageType)
				}

				// Check last frame
				if i == len(frames)-1 {
					if !frame.Header.IsLastFragment {
						t.Error("last frame should have IsLastFragment set")
					}
					if len(frame.Data) != tt.lastFrameSize {
						t.Errorf("last frame: got size %d, want %d",
							len(frame.Data), tt.lastFrameSize)
					}
				}

				totalData = append(totalData, frame.Data...)
			}

			// Verify reassembled data matches input
			if !bytes.Equal(totalData, tt.input) {
				t.Error("reassembled data doesn't match input")
			}
		})
	}
}

func BenchmarkFrameMessage(b *testing.B) {
	sizes := []int{
		100,                  // Small message
		MaxFragmentSize,      // Exact fragment size
		MaxFragmentSize * 2,  // Multiple fragments
		MaxFragmentSize * 10, // Many fragments
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			data := bytes.Repeat([]byte("a"), size)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				FrameMessage(data, uint64(i), MessageTypeData)
			}
		})
	}
}
