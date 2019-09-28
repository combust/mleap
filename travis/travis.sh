#!/usr/bin/env bash

if [[ $TRAVIS_BRANCH == 'master' ]] && [ "$TRAVIS_PULL_REQUEST" = "false" ]; then
  source travis/extract.sh
  source travis/docker.sh
  sbt "+ mleap-spark-extension/writeRuntimeClasspathToFile" "+ test" "+ mleap-executor-tests/test" "+ mleap-benchmark/test" "mleap-xgboost-runtime/test" "mleap-xgboost-spark/test" "+ publishSigned" "mleap-xgboost-runtime/publishSigned" "mleap-xgboost-spark/publishSigned" "mleap-serving/docker:publish" "mleap-spring-boot/docker:publish"
else
  sbt "+ mleap-spark-extension/writeRuntimeClasspathToFile" "+ test" "+ mleap-executor-tests/test" "+ mleap-benchmark/test" "mleap-xgboost-runtime/test" "mleap-xgboost-spark/test"
fi
