package bridge

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

func BenchmarkHeaderMarshal(b *testing.B) {
	header := &Header{
		Type:           MessageTypeData,
		SequenceNumber: 12345,
		FragmentID:     1,
		FragmentTotal:  4,
		FragmentSeq:    2,
		IsLastFragment: false,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		header.marshal()
	}
}

func BenchmarkHeaderUnmarshal(b *testing.B) {
	header := &Header{
		Type:           MessageTypeData,
		SequenceNumber: 12345,
		FragmentID:     1,
		FragmentTotal:  4,
		FragmentSeq:    2,
		IsLastFragment: false,
	}
	data := header.marshal()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		unmarshalHeader(data)
	}
}

func BenchmarkMessageMarshal(b *testing.B) {
	msg := &message{
		SequenceNumber: 12345,
		Data:           []byte("Hello, World!"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg.marshal()
	}
}

func BenchmarkMessageUnmarshal(b *testing.B) {
	msg := &message{
		SequenceNumber: 12345,
		Data:           []byte("Hello, World!"),
	}
	data := msg.marshal()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		unmarshalMessage(data)
	}
}

func BenchmarkHeaderMarshalWithPool(b *testing.B) {
	header := &Header{
		Type:           MessageTypeData,
		SequenceNumber: 12345,
		FragmentID:     1,
		FragmentTotal:  4,
		FragmentSeq:    2,
		IsLastFragment: false,
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			header.marshal()
		}
	})
}

func BenchmarkFrameMarshalWithPool(b *testing.B) {
	sizes := []int{
		100,
		1000,
		MaxFragmentSize,
		MaxFragmentSize * 2,
	}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size-%d", size), func(b *testing.B) {
			data := bytes.Repeat([]byte("a"), size)
			frames := FrameMessage(data, 1, MessageTypeData)

			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					for _, frame := range frames {
						frame.Marshal()
					}
				}
			})
		})
	}
}

func BenchmarkFragmentReassemblyWithPool(b *testing.B) {
	fm := newFragmentManager(time.Second * 30)
	data := bytes.Repeat([]byte("a"), MaxFragmentSize*3)
	frames := FrameMessage(data, 1, MessageTypeData)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for _, frame := range frames {
				fm.addFragment(frame.Header, frame.Data)
			}
		}
	})
}
