#!/usr/bin/env bash

if [[ $TRAVIS_BRANCH == 'master' ]] && [ "$TRAVIS_PULL_REQUEST" = "false" ]; then
  source travis/extract.sh
  if [ $# -eq 0 ]; then
    sbt "release with-defaults"
  else
    sbt "release $@"
  fi
else
  echo "Can only build releases on the master branch, cannot trigger with a pull request"
  exit 1
fi
