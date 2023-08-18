SHELL=/bin/bash

.PHONY: tag-push
tag-push:
	@VERSION=v$$(python -c 'from gamma.io import __version__; print(__version__)') && \
	git tag -a $$VERSION -m "Bump to version $$VERSION" && \
	git push --follow-tags

.PHONY: pdm-lock
pdm-lock:
	pdm lock -G:all -dG:all
