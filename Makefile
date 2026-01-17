SHELL = /usr/bin/env bash -xeuo pipefail

format: \
	fmt-python

fmt-python:
	uv run isort main.py upload_log.py src/
	uv run black main.py upload_log.py src/

execute:
	rm -f std.log
	uv run python main.py

.PHONY: \
	format \
	fmt-python
