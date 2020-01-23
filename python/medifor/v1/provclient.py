#!/bin/python

import grpc
import os.path
import sys
import uuid

import logging

from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc
from google.protobuf import json_format

from medifor.v1 import provenance_pb2, provenance_pb2_grpc
from medifor.v1.analyticservice import rewrite_uris, get_uris
from medifor.v1.medifortools import get_detection, get_detection_req, get_media_type
from medifor.v1.mediforclient import _map_src_targ


class ProvenanceClient(provenance_pb2_grpc.ProvenanceStub):
    """ Client for communicating with provenance analytics  """
    def __init__(self, host="localhost", port="50051",src='', targ='', osrc='', otarg=''):
        port = str(port)
        self.addr = "{!s}:{!s}".format(host, port)
        self.src = src
        self.targ = targ
        self.osrc = osrc
        self.otarg = otarg

        if bool(src) != bool(targ):
            raise ValueError('src->targ mapping specified, but one end is None: {}->{}'.format(src, targ))

        if bool(osrc) != bool(otarg):
            raise ValueError('osrc->otarg mapping specified, but one end is None: {}->{}'.format(osrc, otarg))

        channel = grpc.insecure_channel(self.addr)
        super(ProvenanceClient, self).__init__(channel)
        self.health_stub = health_pb2_grpc.HealthStub(channel)

    def map(self, fname):
        """Map filename to in-container name, using src and targ directories.

        Args:
            fname: The name of the file to map.

        Returns:
            A new filename, mapped to the target directory.
        """
        if not self.src:
            return fname
        return _map_src_targ(self.src, self.targ, fname)

    def unmap(self, fname):
        """Unmap input filename from in-container to on-host. Opposite of map."""
        if not self.src:
            return fname
        return _map_src_targ(self.targ, self.src, fname)

    def o_map(self, fname):
        """Unmap output filename from in-container to on-host. Opposite of o_unmap."""
        if not self.osrc:
            return fname
        return _map_src_targ(self.osrc, self.otarg, fname)

    def o_unmap(self, fname):
        """Map filename from in-container name to in-host name, using osrc and otarg.

        Args:
            fname: The name of the file to map.

        Returns:
            A new filename, mapped to the source directory.
        """
        if not self.osrc:
            return fname
        return _map_src_targ(self.otarg, self.osrc, fname)

    def health(self):
        return self.health_stub.Check(health_pb2.HealthCheckRequest())

    def send(self, req):
        if req.DESCRIPTOR.name == "ProvenanceFilteringRequest":
            return self.ProvenanceFiltering(req)
        elif req.DESCRIPTOR.name == "ProvenanceGraphRequest":
            return self.ProvenanceGraphBuilding(req)
        else:
            logging.error("Invalid Task")
            raise ValueError("{!s} is not a valid task type".format(req.DESCRIPTOR.name))

    def prov_filter(self, img, limit):
        """Function to call registered provenance filtering analytic"""
        img = self.map(img)
        req = provenance_pb2.ProvenanceFilteringRequest()
        mime, _ = get_media_type(img)
        req.image.uri = img
        req.image.type = mime
        req.request_id = str(uuid.uuid4())
        req.result_limit = limit
        return self.send(req)

    def prov_build_graph(self, matches, output_dir):
        raise NotImplementedError




