# MQTT-Bridge

MQTT-Bridge is a library that allows you to bridge protocols over MQTT, with a focus on gRPC. It provides two implementations:

- Network Bridge: A low-level implementation that allows existing gRPC clients and servers to communicate over MQTT without modification. The network bridge provides a `net.Listener` and `net.Conn` interface.
- gRPC Bridge: A higher-level implementation that works directly with MQTT messages while maintaining gRPC-style APIs. The gRPC bridge provides a `grpc.ServiceRegistrar` and `grpc.ServiceInfoProvider` interface.

In theory, the network bridge should work with anything that uses the `net.Listener` and `net.Conn` interfaces, such as HTTP servers and other networking libraries.


# Example Overview

This example demonstrates how to use mqtt-bridge to enable gRPC-style communication over MQTT. The project includes two different implementations showing how to bridge gRPC and MQTT communications.

## Overview

The example implements a simple Echo service with three types of RPCs:
- Unary calls (simple request-response)
- Server streaming (server sends multiple responses)
- Bidirectional streaming (both client and server can send multiple messages)

## Prerequisites

- Go 1.19 or later
- An MQTT broker (e.g., Mosquitto) running on localhost:1883
- Protocol buffer compiler (protoc)

## Service Definition

The Echo service is defined in the proto file:
```proto:example/echo.proto
startLine: 6
endLine: 15
```

## Implementation Options

### 1. Network Bridge Implementation

The network bridge provides a low-level network implementation that allows existing gRPC clients and servers to communicate over MQTT without modification.

#### Server Setup
```go:example/net_bridge/server/main.go
startLine: 34
endLine: 45
```

#### Client Setup
```go:example/net_bridge/client/main.go
startLine: 37
endLine: 55
```

To run:
```bash
# Start the server
go run example/net_bridge/server/main.go

# In another terminal, start the client
go run example/net_bridge/client/main.go
```

### 2. gRPC Bridge Implementation

The gRPC bridge provides a higher-level abstraction that works directly with MQTT messages while maintaining gRPC-style APIs.

#### Server Setup
```go:example/grpc_bridge/server/main.go
startLine: 33
endLine: 39
```

#### Client Setup
```go:example/grpc_bridge/client/main.go
startLine: 30
endLine: 51
```

To run:
```bash
# Start the server
go run example/grpc_bridge/server/main.go

# In another terminal, start the client
go run example/grpc_bridge/client/main.go
```

## Service Implementation

The Echo service implements three types of RPCs:

1. Unary Call - Simple request-response:
```go:example/echo_service.go
startLine: 23
endLine: 33
```

2. Server Streaming - Server sends multiple responses:
```go:example/echo_service.go
startLine: 36
endLine: 55
```

3. Bidirectional Streaming - Both sides can send messages:
```go:example/echo_service.go
startLine: 58
endLine: 82
```

## Key Features

- Seamless conversion between gRPC and MQTT communication
- Support for all gRPC communication patterns:
  - Unary calls
  - Server streaming
  - Client streaming
  - Bidirectional streaming
- Automatic message framing and protocol handling
- Integration with existing gRPC tooling
- Choice between network-level and message-level implementations

## Notes

- The network bridge implementation is ideal when you want to use existing gRPC code over MQTT
- The gRPC bridge implementation is better when you want to work directly with MQTT messages while maintaining gRPC-style APIs
- Both implementations support the full range of gRPC features
- Ensure your MQTT broker is properly configured and accessible before running the examples
- The gRPC bridge implementation has not been tested for streaming RPCs, only unary has been tested as of now.

## License

MIT License

## Copyright

Copyright 2024 Golain Systems Private Limited.
