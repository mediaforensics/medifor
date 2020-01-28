from __future__ import print_function, division, unicode_literals, absolute_import

import contextlib
import json
import logging
import os
import requests
import select
import sys
import threading
import time
import traceback

from concurrent import futures

from medifor.v1 import provenance_pb2
from medifor.v1 import provenance_pb2_grpc
import grpc
from grpc_health.v1 import health
from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc

from google.protobuf import json_format

from flask import Flask, jsonify, request, Response

class EndpointAction(object):

    def __init__(self, action):
        self.action = action

    def __call__(self, *args):
        answer = self.action()
        return answer


class IndexSvc:
    def __init__(self, name, host="::", port=8080, debug=False):
        self.app = Flask(name)
        self.add_endpoint("/search", "search", self.search, methods=["POST"])
        self.host = host
        self.port = port
        self.id_map = None

    def run(self):
        print("Running on {!s}::{!s}".format(self.host, self.port))
        self.app.run(host=self.host, port=self.port)

    def set_map(self, map):
        self.id_map = map

    def search(self):
        data = request.json
        limit = data.get('limit', 0)
        if limit <= 0:
            limit = 30
        img = data['image']

        result = self.query_func(img, limit)


        return jsonify(result)


    def add_endpoint(self, endpoint=None, endpoint_name=None, handler=None, methods=None):
        self.app.add_url_rule(endpoint, endpoint_name, EndpointAction(handler), methods=methods)

    def RegisterQuery(self, f):
        self.query_func = f

def query_index(data, endpoints, limit=10):
    """Function to be passed into Provenance Filtering function to query the index shards"""
    logging.debug("Running Query function")
    json_query = {
        'limit': limit,
        'image': data
    }

    compiled_results = []
    for index in endpoints:
        r = requests.post(index, json=json_query)
        index_results = {
                "status":{
                     "code": r.status_code,
                },
                "value": r.json()
        }
        if r.status_code == requests.codes.ok:
            index_results["status"]["msg"] = r.reason

        compiled_results.append(index_results)

    return compiled_results

class _ProvenanceServicer(provenance_pb2_grpc.ProvenanceServicer):
    """The class registered with gRPC, handles endpoints."""

    def __init__(self, svc):
        """Create a servicer using the given Service object as implementation."""
        self.svc = svc

    def ProvenanceFiltering(self, req, ctx):
        return self.svc._CallEndpoint(self.svc.PROVENANCE_FILTERING, req, provenance_pb2.FilteringResult(), ctx)

    def ProvenanceGraphBuilding(self, req, ctx):
        return self.svc._CallEndpoint(self.svc.PROVENANCE_GRAPH, req, provenance_pb2.ProvenanceGraph(), ctx)

class ProvenanceService:
    """Actual implementation of the service, with function registration."""

    PROVENANCE_FILTERING = 'ProvenanceFiltering'
    PROVENANCE_GRAPH = 'ProvenanceGraphBuilding'
    # Add to _ALLOWED_IMPLS if you add things here.

    _ALLOWED_IMPLS = frozenset([PROVENANCE_FILTERING, PROVENANCE_GRAPH])

    def __init__(self):
        self._impls = {}
        self._health_servicer = health.HealthServicer()

    def Start(self, analytic_port=50051, max_workers=10, concurrency_safe=False):
        self.concurrency_safe = concurrency_safe
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers),
                             options=(('grpc.so_reuseport', 0),))
        provenance_pb2_grpc.add_ProvenanceServicer_to_server(_ProvenanceServicer(self), server)
        health_pb2_grpc.add_HealthServicer_to_server(self._health_servicer, server)
        if not server.add_insecure_port('[::]:{:d}'.format(analytic_port)):
            raise RuntimeError("can't bind to port {}: already in use".format(analytic_port))
        server.start()
        self._health_servicer.set('', health_pb2.HealthCheckResponse.SERVING)
        print("Analytic server started on port {} with PID {}".format(analytic_port, os.getpid()), file=sys.stderr)
        return server

    def Run(self, analytic_port=50051, max_workers=10, concurrency_safe=False):
        server = self.Start(analytic_port=analytic_port, max_workers=max_workers, concurrency_safe=concurrency_safe)

        try:
            while True:
                time.sleep(3600 * 24)
        except KeyboardInterrupt:
            server.stop(0)
            logging.info("Server stopped")
            return 0
        except Exception as e:
            server.stop(0)
            logging.error("Caught exception: %s", e)
            return -1

    def RegisterProvenanceFiltering(self, f):
        return self._RegisterImpl(self.PROVENANCE_FILTERING, f)

    def RegisterProvenanceGraphBuilding(self, f):
        return self._RegisterImpl(self.PROVENANCE_GRAPH, f)

    def _RegisterImpl(self, type_name, f):
        if type_name not in self._ALLOWED_IMPLS:
            raise ValueError("unknown implementation type {} specified".format(type_name))
        if type_name in self._impls:
            raise ValueError("implementation for {} already present".format(type_name))
        self._impls[type_name] = f
        return self

    def _CallEndpoint(self, ep_type, req, resp, ctx):
        """Implements calling endpoints and handling various exceptions that can come back.

        Args:
            ep_type: The name of the manipulation, e.g., "image". Should be in ALLOWED_IMPLS.
            req: The request proto to send.
            resp: The response proto to fill in.
            ctx: The context, used mainly for aborting with error codes.

        Returns:
            An appropriate response object for the endpoint type specified.
        """
        ep_func = self._impls.get(ep_type)
        if not ep_func:
            ctx.abort(grpc.StatusCode.UNIMPLEMENTED, "Endpoint {!r} not implemented".format(ep_type))

        try:
            if ep_type == self.PROVENANCE_FILTERING:
                ep_func(req, resp, query_index)
            else:
                ep_func(req, resp)
        except ValueError as e:
            logging.exception('invalid input')
            ctx.abort(grpc.StatusCode.INVALID_ARGUMENT, "Endpoint {!r} invalid input: {}".format(ep_type, e))
        except NotImplementedError as e:
            logging.warn('unimplemented endpoint {}'.format(ep_type))
            ctx.abort(grpc.StatusCode.UNIMPLEMENTED, "Endpoint {!r} not implemented: {}".format(ep_type, e))
        except Exception:
            logging.exception('unknown error')
            ctx.abort(grpc.StatusCode.UNKNOWN, "Error processing endpoint {!r}: {}".format(ep_type, traceback.format_exc()))
        return resp


class HTTPContextAbortedError(IOError):
    def __init__(self, code, details):
        self.code = code
        self.details = details
        super(HTTPContextAbortedError, self).__init__("Context aborted with code: {!s}.  Message: {!s}".format(code, details))


class HTTPContext:
    def abort(self, code, details):
        raise HTTPContextAbortedError(code, details)


class ProvenanceServiceHTTP(ProvenanceService):
    """Service implementation using basic REST connection to be used when libraries preclude the use of grpc."""
    def __init__(self, port=8765):
        self.port = port
        self.app = Flask(name)

        self.add_endpoint("/api/graph", "api-graph", self.graph, methods=["POST"])
        self.add_endpoint("/api/filter", "api-filter", self.filter, methods=["POST"])

        super(ProvenanceServiceHTTP, self).__init__()

    def add_endpoint(self, endpoint=None, endpoint_name=None, handler=None, methods=None):
        if not handler:
            handler = lambda: raise NotImplementedError()
        self.app.add_url_rule(endpoint, endpoint_name, lambda *args: handler(), methods=methods)

    def graph(self):
        ctx = HTTPContext()
        data = request.json
        req = json_format.ParseDict(data, provenance_pb2.ProvenanceGraphRequest())
        resp = provenance_pb2.ProvenanceGraph()
        self._CallEndpoint(ProvenanceService.PROVENANCE_GRAPH, req, resp, ctx)
        return jsonify(json_format.MessageToDict(resp))

    def filter(self):
        ctx = HTTPContext()
        data = request.json
        req = json_format.ParseDict(data, provenance_pb2.FilterRequest())
        resp = provenance_pb2.FilteringResult()
        self._CallEndpoint(ProvenanceService.PROVENANCE_FILTERING, req, resp, ctx)
        return jsonify(json_format.MessageToDict(resp))

    def Run(self):
        """Run the HTTP service."""
        print "Running HTTP provenance service on port {}".format(self.port)
        return self.app.run(host='0.0.0.0', port=self.port)
