#!/usr/bin/env bash

if [[ $TRAVIS_BRANCH == 'master' ]] && [ "$TRAVIS_PULL_REQUEST" = "false" ]; then
  source travis/extract.sh
  source travis/docker.sh
  sbt "+ test" "mleap-serving/test" "+ publishSigned" "mleap-serving/docker:publish"
else
  nosetests --nologcapture --exclude-dir=./python/mleap/pyspark --verbose
  sbt "+ test" "mleap-serving/test"
fi
