# MLeap TensorFlow

This module provides TensorFlow support for MLeap, allowing TensorFlow graphs
to be executed as transformers inside an MLeap pipeline.

## Build

The `TENSORFLOW_PLATFORMS` environment variable controls which TensorFlow native
shared libraries are downloaded at build time. The possible values are
`windows-x86_64`, `linux-x86_64`, and `macosx-x86_64`, and multiple values can be
comma-separated. If it is not set, all three platform libraries are downloaded.

## Test

```bash
sbt mleap-tensorflow/test
```

## Usage

Add the MLeap TensorFlow dependency along with the TensorFlow native library for
your platform. Match the TensorFlow version to the one listed in the
compatibility matrix in the top-level README.

```scala
"ml.combust.mleap" %% "mleap-tensorflow" % mleapVersion
"org.tensorflow" % "tensorflow-core-native" % "1.0.0" classifier "linux-x86_64"
```
