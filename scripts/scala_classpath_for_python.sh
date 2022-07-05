#! /bin/bash
if [ -z ${SBT} ]; then
  export SBT=sbt
fi
# Source this file to load SCALA_CLASS_PATH in the environment

# Compile classes used in python tests
${SBT} mleap-spark-extension/compile
${SBT} mleap-spark/compile

# Export the complete dependencyClasspath into an env. variable
export SCALA_CLASS_PATH=\
"$( ${SBT} --error 'export mleap-spark-extension/runtime:fullClasspath' )"
