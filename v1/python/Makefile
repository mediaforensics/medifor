SHELL := /bin/bash

.PHONY: default
default: get-deps build

.PHONY: get-deps
get-deps:
	pip install -r ../../requirements.txt

.PHONY: genproto
genproto:
	python3 -m grpc_tools.protoc -I../proto --python_out=. --grpc_python_out=. ../proto/*.proto
	python3 -m grpc_tools.protoc -I../proto --python_out=. ../proto/google/rpc/*.proto
	touch google/rpc/__init__.py # for Python2 compatibility.

