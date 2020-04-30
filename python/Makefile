.PHONY: help env clean py36_test py37_test test build upload

.EXPORT_ALL_VARIABLES:
SCALA_CLASS_PATH ?= $(shell cd .. && bash travis/scala_classpath_for_python.sh)

help:
	@echo "  env         create a development environment using virtualenv"
	@echo "  clean       remove unwanted stuff"
	@echo "  test        run tests with code coverage"

env:
	tox -e venv

clean:
	rm -fr build
	rm -fr dist
	rm -fr .tox
	rm -fr venv
	find . -name '*.pyc' -exec rm -f {} \;
	find . -name '*.pyo' -exec rm -f {} \;
	find . -name '*~' -exec rm -f {} \;

py36_test:
	tox -e py36 -v

py37_test:
	tox -e py37 -v

test: py36_test py37_test
	@echo "All python tests completed"

build: clean
	python setup.py sdist
	python setup.py bdist_wheel

upload: clean
	python setup.py sdist upload
	python setup.py bdist_wheel upload
