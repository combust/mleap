#!/usr/bin/env bash

if [[ $TRAVIS_BRANCH == 'master' ]] && [ "$TRAVIS_PULL_REQUEST" = "false" ]; then
  sbt "+ test" "+ publishSigned"
else
  sbt "+ test"
fi
