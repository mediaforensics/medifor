#!/bin/bash

cd $(dirname "$0")

pdir='../proto'
mfdir="medifor/v1"

mfprotofiles=(
  "${pdir}/${mfdir}/analytic.proto"
  "${pdir}/${mfdir}/fusion.proto"
  "${pdir}/${mfdir}/provenance.proto"
  "${pdir}/${mfdir}/kill.proto"
  "${pdir}/${mfdir}/streamingproxy.proto"
)

workflowfiles=(
  "${pdir}/${mfdir}/pipeline.proto"
)


protoc -I"${pdir}" -I"${pdir}/${mfdir}" --go_out=plugins=grpc:mediforproto "${mfprotofiles[@]}"
protoc -I"${pdir}" -I"${pdir}/${mfdir}" --go_out=plugins=grpc:workflowproto "${workflowfiles[@]}"


mv "mediforproto/${mfdir}"/*.pb.go mediforproto/
rm -rf mediforproto/${mfdir%%/*}

mv "workflowproto/${mfdir}"/*.pb.go workflowproto/
rm -rf workflowproto/${mfdir%%/*}
