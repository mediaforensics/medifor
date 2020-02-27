#!/bin/bash

cd $(dirname "$0")
protoc -I../proto --go_out=plugins=grpc:mediforproto ../proto/*.proto