# Tensorflow

## Installation

Install Tensorflow JNI shared library, see [full instructions for more details](https://github.com/tensorflow/tensorflow/tree/v1.3.0/tensorflow/java)

Before beginning, make sure to delete the github entry for the protobuf library from `tensorflow/workspace.bzl`
as per [this issue](https://github.com/tensorflow/tensorflow/issues/12979).

```
# Clone the repo and checkout version 1.3.0 (as of MLeap 0.8.0)
git clone https://github.com/tensorflow/tensorflow.git
cd tensorflow
gco v1.3.0

# Build the Tensorflow JNI library
./configure
bazel build \
  //tensorflow/java:libtensorflow_jni
```

## Test

In order to use Tensorflow, tell Java where the JNI shared library as using the `-Djava.library.path` command line argument.

Make sure that everything is working by running the MLeap tests.

```
sbt mleap-tensorflow/test -Djava.library.path=<path-to-tensorflow-jni-folder>
```

## Usage

Whenever you wish to use a Tensorflow transformer with MLeap, you will have to make sure that the JVM has access to the share library build with the installation instructions. Use the `-Djava.library.path=<path>` command line argument to specify where the Tensorflow library may be found.
