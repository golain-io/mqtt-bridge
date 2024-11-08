package echo

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type EchoServer struct {
	UnimplementedEchoServiceServer
	counter int32
}

func NewEchoServer() *EchoServer {
	return &EchoServer{}
}

// Unary call implementation
func (s *EchoServer) Echo(ctx context.Context, req *EchoRequest) (*EchoResponse, error) {
	if req.Message == "" {
		return nil, status.Error(codes.InvalidArgument, "empty message")
	}

	seq := atomic.AddInt32(&s.counter, 1)
	return &EchoResponse{
		Message:  fmt.Sprintf("Echo: %s", req.Message),
		Sequence: seq,
	}, nil
}

// Server streaming implementation
func (s *EchoServer) EchoStream(req *EchoRequest, stream EchoService_EchoStreamServer) error {
	if req.Message == "" {
		return status.Error(codes.InvalidArgument, "empty message")
	}

	// Send 5 responses
	for i := 0; i < 5; i++ {
		seq := atomic.AddInt32(&s.counter, 1)
		resp := &EchoResponse{
			Message:  fmt.Sprintf("Echo %d: %s", i+1, req.Message),
			Sequence: seq,
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

// Bidirectional streaming implementation
func (s *EchoServer) EchoBidiStream(stream EchoService_EchoBidiStreamServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		if req.Message == "" {
			return status.Error(codes.InvalidArgument, "empty message")
		}

		seq := atomic.AddInt32(&s.counter, 1)
		resp := &EchoResponse{
			Message:  fmt.Sprintf("Echo: %s", req.Message),
			Sequence: seq,
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}
