#!/usr/bin/env bash

if [[ $TRAVIS_BRANCH == 'master' ]]; then
  sbt "+ test" "+ publishSigned"
else
  sbt "+ test"
fi
