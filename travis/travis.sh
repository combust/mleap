#!/usr/bin/env bash

if [[ $TRAVIS_BRANCH == 'master' ]] && [ "$TRAVIS_PULL_REQUEST" = "false" ]; then
  source travis/extract.sh
  source travis/docker.sh
  sbt "+ test" "+ mleap-executor-tests/test" "+ mleap-benchmark/test" "mleap-xgboost-runtime/test" "mleap-xgboost-spark/test" "+ publishSigned" "mleap-xgboost-runtime/publishSigned" "mleap-xgboost-spark/publishSigned" "mleap-serving/docker:publish" "mleap-spring-boot/docker:publish"
else
  sbt "+ test" "+ mleap-executor-tests/test" "+ mleap-benchmark/test" "mleap-xgboost-runtime/test" "mleap-xgboost-spark/test"
fi
# copy resources again under mleap-spark-extension/target/scala-2.12/classes/ as pyspark tests need them
# (somehow building sbt with + ie. multi-version (builds first 2.11.8, then 2.12) removes the
#  reference.conf from under scala-2.11/classes/ even though it doesn't delete the .class files
#  in that folder)
# probably this is only enough to fix resources classpath for mleap-spark-extension
# (if classes/ folders of any other modules have required resources, need to do something more)
sbt mleap-spark-extension/compile
