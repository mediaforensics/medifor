SHELL := /bin/bash
PLATFORM := $(shell go env GOOS)
ARCH := $(shell go env GOARCH)
GOPATH := $(shell go env GOPATH)
BINARY_NAME := "medifor-proto-fileutil"
CURDIR_NAME := $(shell basename $(CURDIR))

default: get-deps build test

get-deps:
	GO111MODULE=on go mod download

build:
	# check module formatting code
	GO111MODULE=on go fmt ./...
	# build binary with generated stubs
	GO111MODULE=on go build

#licenseok:
#	# ensure license is present
#	GO111MODULE=on go build -o licenseok ./scripts/licenseok/main.go

validate:
	GO111MODULE=on go get golang.org/x/lint/golint
	GO111MODULE=on ./scripts/lint.sh

test:
	GO111MODULE=on go test -race .

clean:
	GO111MODULE=on go clean

docker-build:
	docker build -t $(BINARY_NAME):latest .

# disallow any parallelism (-j) for Make. This is necessary since some
# commands during the build process create temporary files that collide
# under parallel conditions.
.NOTPARALLEL:

.PHONY: build get-deps licenseok start clean docker-build docker-run