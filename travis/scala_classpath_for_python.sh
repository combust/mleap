#! /bin/bash

# Print the complete spark-extensions dependency classpath
sbt --error 'export mleap-spark-extension/runtime:fullClasspath' 2> /dev/null
