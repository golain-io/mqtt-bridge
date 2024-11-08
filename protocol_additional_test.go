package bridge

import (
	"bytes"
	"testing"
)

func TestHeader(t *testing.T) {
	tests := []struct {
		name   string
		header Header
	}{
		{
			name: "basic header",
			header: Header{
				Type:           MessageTypeData,
				SequenceNumber: 1,
				FragmentID:     1,
				FragmentTotal:  1,
				FragmentSeq:    0,
				IsLastFragment: true,
			},
		},
		{
			name: "multi-fragment header",
			header: Header{
				Type:           MessageTypeStreamInit,
				SequenceNumber: 65535,
				FragmentID:     2,
				FragmentTotal:  3,
				FragmentSeq:    1,
				IsLastFragment: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := tt.header.marshal()
			decoded, err := unmarshalHeader(data)
			if err != nil {
				t.Fatalf("Failed to unmarshal header: %v", err)
			}

			if decoded.Type != tt.header.Type ||
				decoded.SequenceNumber != tt.header.SequenceNumber ||
				decoded.FragmentID != tt.header.FragmentID ||
				decoded.FragmentTotal != tt.header.FragmentTotal ||
				decoded.FragmentSeq != tt.header.FragmentSeq ||
				decoded.IsLastFragment != tt.header.IsLastFragment {
				t.Errorf("Unmarshaled header doesn't match original")
			}
		})
	}
}

func TestMessage(t *testing.T) {
	tests := []struct {
		name    string
		message message
	}{
		{
			name: "small message",
			message: message{
				SequenceNumber: 1,
				Data:           []byte("Hello"),
			},
		},
		{
			name: "large message",
			message: message{
				SequenceNumber: 999999,
				Data:           bytes.Repeat([]byte("Large Message "), 100),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := tt.message.marshal()
			decoded, err := unmarshalMessage(data)
			if err != nil {
				t.Fatalf("Failed to unmarshal message: %v", err)
			}

			if decoded.SequenceNumber != tt.message.SequenceNumber ||
				!bytes.Equal(decoded.Data, tt.message.Data) {
				t.Errorf("Unmarshaled message doesn't match original")
			}
		})
	}
}

func TestSetMaxFragmentSize(t *testing.T) {
	originalSize := MaxFragmentSize
	defer func() {
		MaxFragmentSize = originalSize
	}()

	newSize := 1024
	SetMaxFragmentSize(newSize)
	if MaxFragmentSize != newSize {
		t.Errorf("MaxFragmentSize = %d; want %d", MaxFragmentSize, newSize)
	}
}
