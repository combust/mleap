#!/usr/bin/env bash

set -e

git clone https://github.com/WeichenXu123/xgboost.git --depth=1
cd xgboost
git fetch origin v151-dbg:v151-dbg
git checkout v151-dbg

cd jvm-packages/xgboost4j-spark

sbt publishLocal
