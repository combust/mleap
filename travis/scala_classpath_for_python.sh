#! /bin/bash

# Compile classes used in python tests
sbt mleap-spark-extension/compile
sbt mleap-spark/compile

# Export the complete dependencyClasspath into an env. variable
export SCALA_CLASS_PATH=\
"$(sbt --error 'set showSuccess := false' mleap-spark-extension/printClassPath 2> /dev/null)"
