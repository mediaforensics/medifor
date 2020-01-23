from __future__ import print_function, division, unicode_literals, absolute_import

import logging
import os
import sys
import time

from concurrent import futures

import medifor.v1.analytic_pb2 as analytic_pb2
import medifor.v1.fusion_pb2_grpc as fusion_pb2_grpc
import grpc
from grpc_health.v1 import health
from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc


class _FuserServicer(fusion_pb2_grpc.FuserServicer):
    """The class registered with gRPC, handles endpoints."""

    def __init__(self, svc):
        """Create a servicer using the given Service object as implementation."""
        self.svc = svc

    def FuseImageManipulation(self, req, ctx):
        return self.svc._CallEndpoint(self.svc.IMAGE_MANIPULATION, req, analytic_pb2.ImageManipulation(), ctx)

    def FuseVideoManipulation(self, req, ctx):
        return self.svc._CallEndpoint(self.svc.VIDEO_MANIPULATION, req, analytic_pb2.VideoManipulation(), ctx)

    def FuseImageSplice(self, req, ctx):
        return self.svc._CallEndpoint(self.svc.IMAGE_SPLICE, req, analytic_pb2.ImageSplice(), ctx)

    def FuseImageCameraMatch(self, req, ctx):
        return self.svc._CallEndpoint(self.svc.IMAGE_CAMERA_MATCH, req, analytic_pb2.ImageCameraMatch(), ctx)

    def FuseVideoCameraMatch(self, req, ctx):
        return self.svc._CallEndpoint(self.svc.VIDEO_CAMERA_MATCH, req, analytic_pb2.VideoCameraMatch(), ctx)

class FusionService:
    """Actual implementation of the service, with function registration."""

    IMAGE_MANIPULATION = 'FuseImageManipulation'
    VIDEO_MANIPULATION = 'FuseVideoManipulation'
    IMAGE_SPLICE = "FuseImageSplice"
    IMAGE_CAMERA_MATCH = "FuseImageCameraMatch"
    VIDEO_CAMERA_MATCH = "FuseVideoCameraMatch"
    # Add to _ALLOWED_IMPLS if you add things here.

    _ALLOWED_IMPLS = frozenset([IMAGE_MANIPULATION, VIDEO_MANIPULATION, IMAGE_SPLICE, IMAGE_CAMERA_MATCH, VIDEO_CAMERA_MATCH])

    def __init__(self):
        self._impls = {}
        self._health_servicer = health.HealthServicer()

    def Start(self, analytic_port=50051, max_workers=10):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers),
                             options=(('grpc.so_reuseport', 0),))
        fusion_pb2_grpc.add_FuserServicer_to_server(_FuserServicer(self), server)
        health_pb2_grpc.add_HealthServicer_to_server(self._health_servicer, server)
        if not server.add_insecure_port('[::]:{:d}'.format(analytic_port)):
            raise RuntimeError("can't bind to port {}: already in use".format(analytic_port))
        server.start()
        self._health_servicer.set('', health_pb2.HealthCheckResponse.SERVING)
        print("Fusion server started on port {} with PID {}".format(analytic_port, os.getpid()), file=sys.stderr)
        return server

    def Run(self, analytic_port=50051, max_workers=10):
        server = self.Start(analytic_port, max_workers)

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

    def RegisterImageManipulation(self, f):
        return self._RegisterImpl(self.IMAGE_MANIPULATION, f)

    def RegisterVideoManipulation(self, f):
        return self._RegisterImpl(self.VIDEO_MANIPULATION, f)

    def RegisterImageSplice(self, f):
        return self._RegisterImpl(self.IMAGE_SPLICE, f)

    def RegisterImageCameraMatch(self, f):
        return self._RegisterImpl(self.IMAGE_CAMERA_MATCH, f)

    def RegisterVideoCameraMatch(self, f):
        return self._RegisterImpl(self.VIDEO_CAMERA_MATCH, f)

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
            ep_func(req, resp)
        except ValueError as e:
            logging.exception('invalid input')
            ctx.abort(grpc.StatusCode.INVALID_ARGUMENT, "Endpoint {!r} invalid input: {}".format(ep_type, e))
        except NotImplementedError as e:
            logging.warn('unimplemented endpoint {}'.format(ep_type))
            ctx.abort(grpc.StatusCode.UNIMPLEMENTED, "Endpoint {!r} not implemented: {}".format(ep_type, e))
        except Exception as e:
            logging.exception('unknown error')
            ctx.abort(grpc.StatusCode.UNKNOWN, "Error processing endpoint {!r}: {}".format(ep_type, e))
        return resp
