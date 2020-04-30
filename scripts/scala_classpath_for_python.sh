#! /bin/bash

# Source this file to load SCALA_CLASS_PATH in the environment

# Compile classes used in python tests
sbt mleap-spark-extension/compile
sbt mleap-spark/compile

# Export the complete dependencyClasspath into an env. variable
export SCALA_CLASS_PATH=\
"$(sbt --error 'export mleap-spark-extension/runtime:fullClasspath' 2> /dev/null)"
