.PHONY: py36_test py37_test

.EXPORT_ALL_VARIABLES:
SCALA_CLASS_PATH ?= $(shell bash travis/scala_classpath_for_python.sh)

py36_test:
	make -C python py36_test

py37_test:
	make -C python py37_test
