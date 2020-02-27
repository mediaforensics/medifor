SHELL := /bin/bash
PLATFORM := $(shell go env GOOS)
ARCH := $(shell go env GOARCH)
GOPATH := $(shell go env GOPATH)
BINARY_NAME := "example_analytic"
CURDIR_NAME := $(shell basename $(CURDIR))

default: get-deps build test

build-proto:
	mkdir -p mediforproto
	protoc -I../proto  -I../proto/google/rpc --go_out=plugins=grpc:mediforproto ../proto/*.proto

build: build-proto
	# build binary with generated stubs
	./scripts/build.sh


#licenseok:
#	# ensure license is present
#	GO111MODULE=on go build -o licenseok ./scripts/licenseok/main.go

validate:
	GO111MODULE=on go get golang.org/x/lint/golint
	GO111MODULE=on ./scripts/lint.sh

test:
	GO111MODULE=on ./scripts/test.sh

install:
	cp $(BINARY_NAME) $(GOPATH)/bin/$(BINARY_NAME)

start: build
	./$(BINARY_NAME)

clean:
	GO111MODULE=on go clean
	rm -rf release
	rm -f $(BINARY_NAME)
	rm -f $(CURDIR_NAME)
	rm -rf mediforproto

docker-build:
	docker build -t $(BINARY_NAME):latest .

docker-run:
	docker run -d --rm -p 50051:50051 --name $(BINARY_NAME) $(BINARY_NAME):latest

docker-stop:
	docker stop $(BINARY_NAME)

# disallow any parallelism (-j) for Make. This is necessary since some
# commands during the build process create temporary files that collide
# under parallel conditions.
.NOTPARALLEL:

.PHONY: build build-proto get-deps licenseok start clean docker-build docker-run