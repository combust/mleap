# MLeap gRPC

This module provides the client side of the MLeap gRPC protocol. It contains
`GrpcClient`, an implementation of the `TransformService` trait that executes
models against a remote MLeap gRPC server, along with the type converters and
Akka stream helpers used to translate between MLeap types and their Protobuf
representations.

Use this module when you want to load, unload, transform, and stream data
against an MLeap model that is hosted by a separate `mleap-grpc-server`
process. The server side is provided by the `mleap-grpc-server` module.

See the `mleap-executor` README for a description of the `TransformService`
API that `GrpcClient` implements.
