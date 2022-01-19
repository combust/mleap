#!/usr/bin/env bash

set -e

git clone https://github.com/WeichenXu123/xgboost.git@v151-dbg --depth=1
cd xgboost
git fetch origin xgb1.4.1-spark-3.2:xgb1.4.1-spark-3.2
git checkout xgb1.4.1-spark-3.2

cd jvm-packages/xgboost4j-spark

sbt publishLocal
