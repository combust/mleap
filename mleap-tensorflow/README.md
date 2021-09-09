# Tensorflow
## Build
Environment variable `TENSORFLOW_PLATFORMS` can be used to reduce the number of tensorflow shared library downloaded
The possible values are `windows-x86_64,linux-x86_64,macosx-x86_64`, and can be multiple.
If it's not set, all three platform shared library will be downloaded.

## Test

```
sbt mleap-tensorflow/test 
```

## Usage
SBT
```scala
"ml.combust.mleap" %% "mleap-tensorflow" % mleapVersion
"org.tensorflow" % "tensorflow-core-api" %  "0.3.1" classifier "linux-x86_64"
```
