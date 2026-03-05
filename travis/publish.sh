#!/usr/bin/env bash
sbt "+ publishSigned" \
      "mleap-serving/docker:publish" \
      "mleap-spring-boot/docker:publish"