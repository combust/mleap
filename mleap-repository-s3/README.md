# MLeap Repository S3

This module provides an S3-backed `Repository` implementation for the MLeap
executor. It lets the executor download MLeap bundles directly from Amazon S3
using `s3://` URIs.

This support is early and lightly tested. See `S3Repository.scala` for the
current implementation.

## Configuration

Register the S3 repository with the executor by adding it to the executor
repository configuration. The provider is
`ml.combust.mleap.repository.s3.S3RepositoryProvider`.

```hocon
ml.combust.mleap.executor {
  repository {
    class = "ml.combust.mleap.executor.repository.MultiRepositoryProvider$"

    repositories = [{
      class = "ml.combust.mleap.executor.repository.FileRepositoryProvider$"
    }, {
      class = "ml.combust.mleap.executor.repository.HttpRepositoryProvider$"
    }, {
      class = "ml.combust.mleap.repository.s3.S3RepositoryProvider"
    }]
  }
}
```

Credentials and region are resolved using the standard AWS default credential
provider chain. See the `mleap-executor` README for more on repositories and
the `TransformService` API.
