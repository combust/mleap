SHELL := /bin/bash

ifndef SCALA_CLASS_PATH
$(error SCALA_CLASS_PATH for python tests is not set. Please check out the top-level Makefile)
endif

.PHONY: help env clean py36_test py37_test test build upload

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
