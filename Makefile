SHELL := /bin/bash

.PHONY: test_executor test_benchmark test_xgboost_runtime test_xgboost_spark test_root_sbt_project py36_test py37_test

all: test

test_executor:
	sbt "+ mleap-executor-tests/test"

test_benchmark:
	sbt "+ mleap-benchmark/test"

test_xgboost_runtime:
	sbt "mleap-xgboost-runtime/test"

test_xgboost_spark:
	sbt "mleap-xgboost-spark/test"

test_root_sbt_project:
	sbt "+ test"

py36_test:
	source scripts/scala_classpath_for_python.sh && make -C python py36_test

py37_test:
	source scripts/scala_classpath_for_python.sh && make -C python py37_test

test: test_executor test_benchmark test_xgboost_runtime test_xgboost_spark test_root_sbt_project py36_test py37_test
	@echo "All tests run successfully"
