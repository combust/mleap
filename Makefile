SHELL := /bin/bash
SBT ?= sbt

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

.PHONY: test_python37
test_python37:
	source scripts/scala_classpath_for_python.sh && make -C python py37_test

.PHONY: test_python38
test_python38:
	source scripts/scala_classpath_for_python.sh && make -C python py38_test

.PHONY: test_python
test_python: test_python37 test_python38
	@echo "All python tests run successfully"

.PHONY: test
test: test_executor test_benchmark test_xgboost_runtime test_xgboost_spark test_root_sbt_project test_python
	@echo "All tests run successfully"
