#!/usr/bin/env bash

if [ $# -eq 0 ]; then
  sbt "release with-defaults"
else
  sbt "release $@"
fi
