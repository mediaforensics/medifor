SHELL := /bin/bash

.PHONY: default
default: get-deps genproto

.PHONY: get-deps
get-deps:
	pip install -r ../requirements.txt

.PHONY: genproto
genproto:
	python3 -m grpc_tools.protoc -I../proto --python_out=. --grpc_python_out=. ../proto/medifor/v1/*.proto
