SHELL=/bin/bash

.PHONY: install
install:
	pip install -U pip wheel
	pip install -r requirements.txt -r requirements.dev.txt
	# pip install -e .

.PHONY: compile-deps
compile-deps:
	pip-compile requirements.in -q
	pip-compile requirements.dev.in -q

.PHONY: build
build:
	rm dist gamma_io.egg-info -rf
	python -m build

.PHONY: test
test:
	pytest
	# python scripts/update-coverage.py