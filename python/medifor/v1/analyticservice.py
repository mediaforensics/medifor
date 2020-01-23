from __future__ import print_function, division, unicode_literals, absolute_import

import base64
import contextlib
import json
import logging
import os
import select
import sys
import threading
import time
import traceback

from concurrent import futures

import medifor.v1.analytic_pb2 as analytic_pb2
import medifor.v1.analytic_pb2_grpc as analytic_pb2_grpc
import medifor.v1.streamingproxy_pb2_grpc as streamingproxy_pb2_grpc
import medifor.v1.streamingproxy_pb2 as streamingproxy_pb2

import grpc
from grpc_health.v1 import health
from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc

from google.protobuf import json_format

def OptOutVideoLocalization(resp):
    """Opt out of all video localization types for a given video. Modifies resp."""
    resp.opt_out.extend([analytic_pb2.VIDEO_OPT_OUT_TEMPORAL,
                         analytic_pb2.VIDEO_OPT_OUT_SPATIAL])


def OptOutVideoAll(resp):
    """Opt out of all video processing for a given video. Modifies resp."""
    OptOutVideoLocalization(resp)
    resp.opt_out.append(analytic_pb2.VIDEO_OPT_OUT_DETECTION)

def recv_into(stream, tmp_dir):
    curr_name = None
    curr_file = None
    det = None
    name_map = {}
    for c in stream:

        if c.HasField('detection'):
            det = c.detection
            continue

        if c.file_chunk.name != curr_name:
            if curr_file: curr_file.close()

            curr_name = c.file_chunk.name
            if not curr_name:
                raise ValueError("file chunk has no name.")
            local_name = get_local_name(curr_name, tmp_dir)
            name_map[curr_name] = local_name
            curr_file = open(local_name, "wb")

        curr_file.write(c.file_chunk.value)

    if curr_file: curr_file.close()

    if not det: raise ValueError("no detection in stream")
    rewrite_uris(det, name_map)

    return det

def get_local_name(name, tmp_dir):
    url_safe_bytes = base64.urlsafe_b64encode(name.encode("utf-8"))
    url_safe_name = str(url_safe_bytes, "utf-8")

    return os.path.join(tmp_dir, url_safe_name)


def walk_proto(proto, func, args=[]):
    """Walks through a proto and applies a specified function with args."""
    if not proto:
        return

    desc = getattr(proto, 'DESCRIPTOR', None)
    if not desc:
        return

    result = func(proto, *args)

    for fd in proto.DESCRIPTOR.fields:
        value = getattr(proto, fd.name, None)
        if fd.label == fd.LABEL_REPEATED:
            for v in value:
                for x in walk_proto(v, func, args):
                    yield x
        else:
            for x in walk_proto(value, func, args):
                yield x


def rewrite_uris(proto, name_map):
    """Walks through the proto to rewrite uris using the name_map"""
    def rewrite(proto, name_map):
        if proto.DESCRIPTOR.full_name == 'mediforproto.Resource' and proto.uri != "":
            proto.uri = name_map.get(proto.uri, proto.uri)
            return

    walk_proto(proto, rewrite, name_map)

def get_uris(proto):
    print(proto)
    def gen_uri(p):
        print("Called for {!s}".format(p))
        if p.DESCRIPTOR.full_name == 'mediforproto.Resource' and p.uri != "":
            print("Yielding the following: {!s}".format(p.uri))
            yield p.uri
            return

    for x in walk_proto(proto, gen_uri):
        yield x


class _AnalyticServicer(analytic_pb2_grpc.AnalyticServicer):
    """The class registered with gRPC, handles endpoints."""

    def __init__(self, svc):
        """Create a servicer using the given Service object as implementation."""
        self.svc = svc

    def DetectImageManipulation(self, req, ctx):
        return self.svc._CallEndpoint(self.svc.IMAGE_MANIPULATION, req, analytic_pb2.ImageManipulation(), ctx)

    def DetectVideoManipulation(self, req, ctx):
        return self.svc._CallEndpoint(self.svc.VIDEO_MANIPULATION, req, analytic_pb2.VideoManipulation(), ctx)

    def DetectImageSplice(self, req, ctx):
        return self.svc._CallEndpoint(self.svc.IMAGE_SPLICE, req, analytic_pb2.ImageSplice(), ctx)

    def DetectImageCameraMatch(self, req, ctx):
        return self.svc._CallEndpoint(self.svc.IMAGE_CAMERA_MATCH, req, analytic_pb2.ImageCameraMatch(), ctx)

    def DetectVideoCameraMatch(self, req, ctx):
        return self.svc._CallEndpoint(self.svc.VIDEO_CAMERA_MATCH, req, analytic_pb2.VideoCameraMatch(), ctx)

    def DetectImageCameras(self, req, ctx):
        return self.svc._CallEndpoint(self.svc.IMAGE_CAMERAS, req, analytic_pb2.ImageCameras(), ctx)

class _StreamingProxyServicer(streamingproxy_pb2_grpc.StreamingProxyServicer):
    """The class registered with grpc that handles streaming requests from the client"""

    def __init__(self, svc):
        self.svc = svc

    def DetectStream(self, stream, ctx):
        """Takes a detection stream and runs the appropraite function on it"""
        # TODO use python temp directory library
        tmp_dir = os.path.join(self.svc.tmp_dir, "medifor-streamingproxy")
        try:
            os.mkdir(tmp_dir)
        except FileExistsError:
            pass

        det = recv_into(stream, tmp_dir)
        det = self.svc.detect(det, ctx)

        # send_from(det)
        # Get all output files
        if det.HasField("img_manip"):
            resp = det.img_manip
        elif det.HasField("vid_manip"):
            resp = det.vid_manip
        elif det.HasField("img_splice"):
            resp = det.img_splice
        elif det.HasField("img_cam_match"):
            resp = det.img_cam_match
        else:
            raise ValueError("No valid response in detection")



        yield streamingproxy_pb2.DetectionChunk(detection=det)


        for fname in get_uris(resp):
            try:
                s = os.stat(fname)
                f = open(fname, 'rb')
            except OSError as e:
                logging.warning("Error opening file, skipping: %s", e)
                continue

            chunk_size = 1024*1024
            for offset in range(0, s.st_size, chunk_size):
                buf = f.read(1024*1024)
                yield streamingproxy_pb2.DetectionChunk(file_chunk=streamingproxy_pb2.FileChunk(
                    name=fname,
                    value=buf,
                    total_bytes=s.st_size
                ))


class AnalyticService:
    """Actual implementation of the service, with function registration."""

    IMAGE_MANIPULATION = 'ImageManipulation'
    VIDEO_MANIPULATION = 'VideoManipulation'
    IMAGE_SPLICE = "ImageSplice"
    IMAGE_CAMERA_MATCH = "ImageCameraMatch"
    VIDEO_CAMERA_MATCH = "VideoCameraMatch"
    IMAGE_CAMERAS = "ImageCameras"
    # Add to _ALLOWED_IMPLS if you add things here.

    _ALLOWED_IMPLS = frozenset([IMAGE_MANIPULATION, VIDEO_MANIPULATION,
                                IMAGE_SPLICE, IMAGE_CAMERA_MATCH, VIDEO_CAMERA_MATCH, IMAGE_CAMERAS])

    def __init__(self, tmp_dir="/tmp"):
        self._impls = {}
        self._health_servicer = health.HealthServicer()
        self.tmp_dir = tmp_dir

    def Start(self, analytic_port=50051, max_workers=10, concurrency_safe=False):
        self.concurrency_safe = concurrency_safe
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers),
                             options=(('grpc.so_reuseport', 0),))
        analytic_pb2_grpc.add_AnalyticServicer_to_server(_AnalyticServicer(self), server)
        streamingproxy_pb2_grpc.add_StreamingProxyServicer_to_server(_StreamingProxyServicer(self), server)
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

    def detect(self, det, ctx):
        """Detect determines which endpoint to used based on the request type"""

        type = det.WhichOneof("request")
        if type == "img_manip_req":
            req = det.img_manip_req
            resp = self._CallEndpoint(self.IMAGE_MANIPULATION, req, analytic_pb2.ImageManipulation(), ctx)
            det.img_manip.CopyFrom(resp)
        elif type == "vid_manip_req":
            req = det.vid_manip_req
            resp = self._CallEndpoint(self.VIDEO_MANIPULATION, req, analytic_pb2.VideoManipulation(), ctx)
            det.vid_manip.CopyFrom(resp)
        elif type == "img_splice_req":
            req = det.img_splice_req
            resp =  self._CallEndpoint(self.IMAGE_SPLICE, req, analytic_pb2.ImageSplice(), ctx)
            det.CopyFrom(resp)
        elif type == "img_cam_match_req":
            req = det.img_cam_match_req
            resp =  self._CallEndpoint(self.IMAGE_CAMERA_MATCH, req, analytic_pb2.ImageCameraMatch(), ctx)
            det.CopyFrom(resp)
        elif type == "vid_cam_match_req":
            req = det.vid_cam_match_req
            resp = self._CallEndpoint(
                self.VIDEO_CAMERA_MATCH, req, analytic_pb2.VideoCameraMatch(), ctx)
            det.CopyFrom(resp)
        elif type == "img_cam_req":
            req = det.img_cam_req
            resp = self._CallEndpoint(
                self.IMAGE_CAMERAS, req, analytic_pb2.ImageCameras(), ctx)
            det.CopyFrom(resp)

        return det



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

    def RegisterImageCameras(self, f):
        return self._RegisterImpl(self.IMAGE_CAMERAS, f)

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
        except Exception:
            logging.exception('unknown error')
            ctx.abort(grpc.StatusCode.UNKNOWN, "Error processing endpoint {!r}: {}".format(ep_type, traceback.format_exc()))
        return resp


class FIFOTimeoutError(IOError):
    def __init__(self, op, timeout):
        return super(FIFOTimeoutError, self).__init__("timed out with op {!r} after {} seconds".format(op, timeout))


class FIFOContextAbortedError(IOError):
    def __init__(self, code, details):
        self.code = code
        self.details = details
        super(FIFOContextAbortedError, self).__init__("Context aborted with code: {!s}.  Message: {!s}".format(code, details))


class FIFOContext:
    def abort(self, code, details):
        raise FIFOContextAbortedError(code, details)


class AnalyticServiceFIFO(AnalyticService):
    """Service implementation using a FIFO connection to be used when libraries preclude the use of grpc """

    DEFAULT_INFILE = "ANALYTIC_FIFO_IN"
    DEFAULT_OUTFILE = "ANALYTIC_FIFO_OUT"

    TYPES = {
        "imgmanip": (AnalyticService.IMAGE_MANIPULATION,
                     analytic_pb2.ImageManipulationRequest,
                     analytic_pb2.ImageManipulation),
        "vidmanip": (AnalyticService.VIDEO_MANIPULATION,
                     analytic_pb2.VideoManipulationRequest,
                     analytic_pb2.VideoManipulation),
        "imgsplice": (AnalyticService.IMAGE_SPLICE,
                      analytic_pb2.ImageSpliceRequest,
                      analytic_pb2.ImageSplice),
        "imgcammatch": (AnalyticService.IMAGE_CAMERA_MATCH,
                        analytic_pb2.ImageCameraMatchRequest,
                        analytic_pb2.ImageCameraMatch),
        "vidcammatch": (AnalyticService.VIDEO_CAMERA_MATCH,
                        analytic_pb2.VideoCameraMatchRequest,
                        analytic_pb2.VideoCameraMatch),
        "imgcameras": (AnalyticService.IMAGE_CAMERAS,
                       analytic_pb2.ImageCamerasRequest,
                       analytic_pb2.ImageCameras)
    }

    def __init__(self, infile=None, outfile=None):
        self.lock = threading.Lock()
        self.infile = infile or os.environ.get(self.DEFAULT_INFILE)
        self.outfile = outfile or os.environ.get(self.DEFAULT_OUTFILE)
        self.receiver = None
        self.sender = None
        super(AnalyticServiceFIFO, self).__init__()

    def _ensureOpen(self):
        # No lock here - called from main single-request-serving method.
        if not self.receiver:
            r = os.open(self.infile, os.O_RDONLY)
            self.receiver = os.fdopen(r, 'rt')
        if not self.sender:
            s = os.open(self.outfile, os.O_WRONLY)
            self.sender = os.fdopen(s, 'wt')

    def close(self):
        with self.lock:
            if self.receiver:
                self.receiver.close()
            if self.sender:
                self.sender.close()

    def send(self, data, timeout=0):
        self._ensureOpen()
        f = self.sender
        selArgs = [[], [f], [f]]
        if timeout:
            selArgs.append(timeout)

        if not any(select.select(*selArgs)):
            raise FIFOTimeoutError("write", timeout)

        f.write(data + '\n')
        f.flush()

    def receive(self, timeout=0):
        self._ensureOpen()
        f = self.receiver
        selArgs = [[f], [], [f]]
        if timeout:
            selArgs.append(timeout)

        if not any(select.select(*selArgs)):
            raise FIFOTimeoutError("read", timeout)
        return f.readline()

    def serveOnce(self):
        with self.lock:
            line = self.receive()
            msg = json.loads(line)
            if "type" not in msg:
                raise ValueError("Message had no 'type' field")
            callType, makeReq, makeResp = self.TYPES[msg["type"]]
            req, resp = makeReq(), makeResp()

            json_format.ParseDict(msg["value"], req)
            try:
                resp = self._CallEndpoint(callType, req, resp, FIFOContext())
                self.send(json.dumps({
                    "code": "OK",
                    "value": json_format.MessageToDict(resp),
                }))
            except FIFOContextAbortedError as e:
                self.send(json.dumps({
                    "code": str(e.code),
                    "value": e.details,
                }))

    def Run(self):
        """Run the service - listens to read FIFO and responds on write FIFO."""
        with contextlib.closing(self):
            while True:
                self.serveOnce()
