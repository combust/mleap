# MLeap gRPC Server

This module provides the server side of the MLeap gRPC protocol. It exposes an
`MleapExecutor` over gRPC so that remote clients can load, unload, transform,
and stream data against MLeap models.

The server is backed by the `mleap-executor` module and is paired with the
`mleap-grpc` module, which provides the matching `GrpcClient`.

## Running the Server

The entry point is `ml.combust.mleap.grpc.server.Boot`. The server reads its
settings from Typesafe configuration, including the listen port and the
executor defaults. See `GrpcServerConfig.scala` for the available options.

See the `mleap-serving` README for how to start both the gRPC server and the
HTTP server from a single MLeap executor.
