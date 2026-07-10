# MLeap Executor Testkit

This module provides shared test utilities for verifying implementations of the
MLeap `TransformService` trait.

The main entry point is the `TransformServiceSpec` trait. Mix it into your own
spec and implement the required methods, and it will run a full suite of tests
that exercise loading, unloading, transforming, and streaming against your
`TransformService` implementation. `TestUtil` provides sample models and leap
frames used by the suite.

This testkit is used by `mleap-executor-tests` and `mleap-grpc-server` to
validate their executors. See the `mleap-executor` README for a description of
the `TransformService` API.
