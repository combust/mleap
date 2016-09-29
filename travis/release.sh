#!/usr/bin/env bash

if [ "$TRAVIS_PULL_REQUEST" = "false" ]; then
  source travis/extract.sh
  git checkout $TRAVIS_BRANCH
  if [ $# -eq 0 ]; then
    sbt "release with-defaults"
  else
    sbt "release $@"
  fi
else
  echo "Can only build releases on the master branch, cannot trigger with a pull request"
  exit 1
fi
