#!/bin/bash

cd $(dirname "$0")

pdir='../proto'
mfdir="medifor/v1"


protoc -I"${pdir}" -I"${pdir}/${mfdir}" --go_out=plugins=grpc:mediforproto "${pdir}/${mfdir}"/*.proto


mv "mediforproto/${mfdir}"/*.pb.go mediforproto/
rm -rf mediforproto/${mfdir%%/*}
