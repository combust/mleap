#!/usr/bin/env bash

set -e

rm -rf mleap-databricks-runtime-fat-tests/src
mkdir -p mleap-databricks-runtime-fat-tests/src/test

cp -R mleap-core/src/test mleap-databricks-runtime-fat-tests/src
cp -R mleap-avro/src/test mleap-databricks-runtime-fat-tests/src
# cp -R mleap-executor-tests/src/test mleap-databricks-runtime-fat-tests/src
cp -R mleap-runtime/src/test mleap-databricks-runtime-fat-tests/src
cp -R mleap-spark/src/test mleap-databricks-runtime-fat-tests/src
cp -R mleap-spark-extension/src/test mleap-databricks-runtime-fat-tests/src
# cp -R mleap-spring-boot/src/test mleap-databricks-runtime-fat-tests/src
cp -R mleap-tensor/src/test mleap-databricks-runtime-fat-tests/src
cp -R mleap-tensorflow/src/test mleap-databricks-runtime-fat-tests/src
cp -R mleap-xgboost-runtime/src/test mleap-databricks-runtime-fat-tests/src
cp -R mleap-xgboost-spark/src/test mleap-databricks-runtime-fat-tests/src
cp -R mleap-spark-testkit/src/main/* mleap-databricks-runtime-fat-tests/src/test/
