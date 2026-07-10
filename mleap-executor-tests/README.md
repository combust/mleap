# MLeap Executor Tests

This module holds the integration tests for the `mleap-executor` module. It is
kept separate from `mleap-executor` to avoid a circular dependency, since the
tests rely on the shared `TransformServiceSpec` trait from
`mleap-executor-testkit`.

The tests cover the `MleapExecutor` and `LocalTransformService` implementations,
along with the file and HTTP repositories that back them.

Run them with:

```bash
sbt mleap-executor-tests/test
```

See the `mleap-executor` README for a description of the components under test.
