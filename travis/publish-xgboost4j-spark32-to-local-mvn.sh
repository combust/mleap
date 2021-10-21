#!/usr/bin/env bash

set -e

git clone https://github.com/WeichenXu123/xgboost.git@xgb1.4.1-spark-3.2 --depth=1
cd xgboost/jvm-packages/xgboost4j-spark

sbt publishM2
