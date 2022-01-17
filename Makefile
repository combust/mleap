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
	$(SBT) "+ mleap-xgboost-spark/test" -verbose -debug

.PHONY: py36_test
py36_test:
	source scripts/scala_classpath_for_python.sh && make -C python py36_test

.PHONY: py37_test
py37_test:
	source scripts/scala_classpath_for_python.sh && make -C python py37_test

.PHONY: test
test: test_executor test_benchmark test_xgboost_runtime test_xgboost_spark test_root_sbt_project py36_test py37_test
	@echo "All tests run successfully"
