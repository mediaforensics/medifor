#!/bin/bash

set -e

cd $(dirname "$0")

pdir='../proto'
mfdir="medifor/v1"

mkdir -p mediforproto/

protoc -I"${pdir}" -I"${pdir}/${mfdir}" --go_out=plugins=grpc:mediforproto "${pdir}/${mfdir}"/*.proto

mv mediforproto/github.com/mediaforensics/medifor/pkg/mediforproto//*.pb.go mediforproto/
rm -rf mediforproto/github.com/
