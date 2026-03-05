SHELL := /bin/bash
SBT ?= sbt
export SBT_OPTS = -Xmx4G

all: test

.PHONY: test_executor
test_executor:
	$(SBT) "+ mleap-executor-tests/test"

.PHONY: test_benchmark
test_benchmark:
	$(SBT) "+ mleap-benchmark/test"

.PHONY: test_root_sbt_project
test_root_sbt_project:
	$(SBT) "+ test"

.PHONY: test_xgboost_runtime
test_xgboost_runtime:
	$(SBT) "+ mleap-xgboost-runtime/test"

.PHONY: test_xgboost_spark
test_xgboost_spark:
	$(SBT) "+ mleap-xgboost-spark/test"

test_python_%: 
	source scripts/scala_classpath_for_python.sh && make -C python test_py$*
	@echo "Python tests for $@ completed successfully"

.PHONY: test_python
test_python:
	source scripts/scala_classpath_for_python.sh && make -C python test
	@echo "All python tests run successfully"

.PHONY: test
test: test_executor test_benchmark test_xgboost_runtime test_xgboost_spark test_root_sbt_project test_python
	@echo "All tests run successfully"
