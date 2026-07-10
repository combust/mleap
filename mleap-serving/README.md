# MLeap Serving

This module starts both the gRPC server and the HTTP server backed by a single
MLeap executor, so you can serve models over both protocols from one process.

The gRPC server is provided by `mleap-grpc-server` and the HTTP server is
provided by `mleap-spring-boot`. Both share the same `MleapExecutor`, so a model
loaded through one interface is available through the other.

The entry point is `ml.combust.mleap.serving.RunServer`. See that file for the
available configuration and startup details.
