#! /bin/bash
if [ -z ${SBT} ]; then
  export SBT=sbt
fi
# Source this file to load SCALA_CLASS_PATH in the environment

# Compile classes used in python tests
${SBT} mleap-spark-extension/compile
${SBT} mleap-spark/compile

# Export the complete dependencyClasspath into an env. variable
# add `tail -n 1` because the fm-sbt-s3-resolver plugin will output something irrelative to STDOUT
# so only the last line output is the SCALA_CLASS_PATH.
export SCALA_CLASS_PATH=\
"$( ${SBT} --error 'export mleap-spark-extension/runtime:fullClasspath' 2> /dev/null | tail -n 1)"
