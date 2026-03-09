SHELL := /bin/bash

define check_scala_classpath
	@if [ -z "$$SCALA_CLASS_PATH" ]; then \
		echo "Error: SCALA_CLASS_PATH for python tests is not set. Please check out the top-level Makefile on how to source scala_classpath_for_python.sh"; \
		exit 1; \
	fi
endef

.PHONY: help env clean test build upload

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
	find . -name '__pycache__' | xargs -r rm -rf

test_%:
	$(check_scala_classpath)
	tox -e $*

test:
	$(check_scala_classpath)
	tox
	@echo "All python tests completed"

build: clean
	python setup.py sdist
	python setup.py bdist_wheel

upload: build
	twine upload dist/*
