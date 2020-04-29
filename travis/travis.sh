#!/usr/bin/env bash

if [[ $TRAVIS_BRANCH == 'master' ]] && [ "$TRAVIS_PULL_REQUEST" = "false" ]; then
  source travis/extract.sh
  source travis/docker.sh
  sbt -DsparkVersion=2.4.5 -DscalaVersion=2.11.12 "+ test" "+ mleap-executor-tests/test" "+ mleap-benchmark/test" "mleap-xgboost-runtime/test" "mleap-xgboost-spark/test" "+ publishSigned" "mleap-xgboost-runtime/publishSigned" "mleap-xgboost-spark/publishSigned" "mleap-serving/docker:publish" "mleap-spring-boot/docker:publish"
else
  sbt -DsparkVersion=2.4.5 -DscalaVersion=2.11.12 "+ test" "+ mleap-executor-tests/test" "+ mleap-benchmark/test" "mleap-xgboost-runtime/test" "mleap-xgboost-spark/test" &&
  sbt -DsparkVersion=3.0.0 -DscalaVersion=2.12.10 "+ test" "+ mleap-executor-tests/test" "+ mleap-benchmark/test"
fi
