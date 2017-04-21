#!/usr/bin/env bash

if [[ $TRAVIS_BRANCH == 'master' ]] && [ "$TRAVIS_PULL_REQUEST" = "false" ]; then
  source travis/extract.sh
  sbt "+ test" "+ publishSigned"
else
  nosetests --nologcapture --exclude-dir=./python/mleap/pyspark --verbose
  sbt "+ test"
fi
