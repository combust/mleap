#!/usr/bin/env bash

set -e

git clone https://github.com/WeichenXu123/xgboost.git --depth=1
cd xgboost
git fetch origin v151-dbg2:v151-dbg2
git checkout v151-dbg2

cd jvm-packages/xgboost4j-spark

sbt publishLocal
