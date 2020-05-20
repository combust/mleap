#!/usr/bin/env bash

if [[ $TRAVIS_BRANCH == 'master' ]] && [ "$TRAVIS_PULL_REQUEST" = "false" ]; then
  source travis/extract.sh
  source travis/docker.sh
  # TODO: Add back xgboost test when xgboost unit test fixed with spark 3.0
  sbt "test" "mleap-executor-tests/test" "mleap-benchmark/test" "publishSigned" "mleap-serving/docker:publish" "mleap-spring-boot/docker:publish"
else
  # TODO: Add back xgboost test when xgboost unit test fixed with spark 3.0
  sbt "test" "mleap-executor-tests/test" "mleap-benchmark/test"
fi
