#!/bin/python

import click
import grpc
import mimetypes
import os.path
import sys
import uuid

import logging

from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc
from google.protobuf import json_format

from medifor.v1 import analytic_pb2, analytic_pb2_grpc


"""
Initialize mimetypes library and add additional mimetypes not currently recognized.
The mimetype of an image/video is provided to the analytic as additional metadata
and is used to determine which endpoint to use when using the detect_batch
method.
"""
mimetypes.init()

mimetypes.add_type("image/x-adobe-dng", ".dng")
mimetypes.add_type("image/x-canon-cr2", ".cr2")
mimetypes.add_type("image/x-canon-crw", ".crw")
mimetypes.add_type("image/x-epson-erf", ".erf")
mimetypes.add_type("image/x-fuji-raf", ".raf")
mimetypes.add_type("image/x-kodak-dcr", ".dcr")
mimetypes.add_type("image/x-kodak-k25", ".k25")
mimetypes.add_type("image/x-kodak-kdc", ".kdc")
mimetypes.add_type("image/x-minolta-mrw", ".mrw")
mimetypes.add_type("image/x-nikon-nef", ".nef")
mimetypes.add_type("image/x-olympus-orf", ".orf")
mimetypes.add_type("image/x-panasonic-raw", ".raw")
mimetypes.add_type("image/x-pentax-pef", ".pef")
mimetypes.add_type("image/x-sigma-x3f", ".x3f")
mimetypes.add_type("image/x-sony-arw", ".arw")
mimetypes.add_type("image/x-sony-sr2", ".sr2")
mimetypes.add_type("image/x-sony-srf", ".srf")

mimetypes.add_type('video/avchd-stream', '.mts')
mimetypes.add_type("application/x-mpegURL", ".m3u8")
mimetypes.add_type("video/3gpp", ".3gp")
mimetypes.add_type("video/MP2T", ".ts")
mimetypes.add_type("video/mp4", ".mp4")
mimetypes.add_type("video/quicktime", ".mov")
mimetypes.add_type("video/x-flv", ".flv")
mimetypes.add_type("video/x-ms-wmv", ".wmv")
mimetypes.add_type("video/x-msvideo", ".avi")

additional_image_types = frozenset([
    "application/octet-stream",
    "application/pdf"
])

additional_video_types = frozenset([
    "application/x-mpegURL",
    "application/mxf"
])

def get_media_type(uri):
    """
    'get_media_type' takes a filepath and returns the typestring and media type.
    If the mimetype is not discernable, the typestring returned will be
    "application/octet-stream", and the media type "application".
    """
    filename, ext = os.path.splitext(uri)
    typestring = mimetypes.types_map.get(ext, 'application/octet-stream')

    if typestring in additional_video_types:
        return typestring, 'video'

    if typestring in additional_image_types:
        return typestring, 'image'

    return typestring, typestring.split("/")[0]

def _map_src_targ(src, targ, fname):
    src = os.path.normpath(os.path.abspath(os.path.expanduser(src))).rstrip('/')
    targ = os.path.normpath(os.path.abspath(os.path.expanduser(targ))).rstrip('/')
    fname = os.path.normpath(os.path.abspath(os.path.expanduser(fname)))

    if not os.path.isdir(src):
        raise ValueError('Source mapping must be a directory, but got {!r}'.format(src))

    if not fname.startswith(src):
        raise ValueError('Not a child of source: cannot map {!r} with {!r} -> {!r}'.format(fname, src, targ))

    suffix = fname[len(src):].lstrip('/')
    return os.path.join(targ, suffix)

class MediforClient(analytic_pb2_grpc.AnalyticStub):
    """
    MediforClient provides a client for communicating with media forensic analytics.

    For src->targ and osrc->otarg mappings, no mapping is done if either
    end of the pair is None. If only one end is None, a ValueError
    exception is raised.

    Args:
        host: The host address of the analytic service.
        port: The port of the analytic service
        src: The host-local input directory (maps to targ in the container).
        targ: The container-local input directory (mapped from src on host).
        osrc: The host-local output directory (maps to otarg in the container).
        otarg: The container-local output directory (mapped from osrc on host).

    Raises:
        ValueError: if either endpoint of src/targ or osrc/otarg is None
            but the other is specified.
    """
    def __init__(self, host="localhost", port="50051", src='', targ='', osrc='', otarg=''):
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
        super(MediforClient, self).__init__(channel)
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

    def detect_one(self, req, task):
        """
        Calls the specified analytic service and returns result.

        Args:
            req: Request protobuf of type 'ImageManipulationRequest', or
                'VideoManipulationRequest'.
            task: String specifying which endpoint to call ('imgManip'/'vidManip')

        Raises:
            ValueError: If the task type is invalid

        Returns:
            The response protobuf returned by the analytic.
        """

        if task == "imgManip":
            return self.DetectImageManipulation(req)
        elif task == "vidManip":
            return self.DetectVideoManipulation(req)
        else:
            logging.error("Invalid Task")
            raise ValueError("{!s} is not a valid task type".format(task))

    def img_manip(self, img, output_dir):
        """
        Builds an "ImageManipulationRequest" and calls 'detect_one'

        Args:
            img: The image uri to be provided to the analytic.
            output_dir: The output directoy for analytic output files

        Returns:
            The response "ImageManipulation" protobuf.
        """
        img = self.map(img)
        output_dir = self.o_map(output_dir)
        req = analytic_pb2.ImageManipulationRequest()
        mime, _ = get_media_type(img)
        req.image.uri = img
        req.image.type = mime
        req.request_id = str(uuid.uuid4())
        req.out_dir = output_dir

        return self.detect_one(req, "imgManip")

    def vid_manip(self, vid, output_dir):
        """
        Builds a "VideoManipulationRequest" and calls 'detect_one'

        Args:
            vid: The video uri to be provided to the analytic.
            output_dir: The output directoy for analytic output files

        Returns:
            The response "VideoManipulation" protobuf.
        """
        vid = self.map(vid)
        output_dir = self.o_map(output_dir)
        req = analytic_pb2.VideoManipulationRequest()
        mime, _ = get_media_type(vid)
        req.video.uri = vid
        req.video.type = mime
        req.request_id = str(uuid.uuid4())
        req.out_dir = output_dir

        return self.detect_one(req, "vidManip")


    def detect_batch(self, dir, output_dir):
        """
        Traverses an input directory building and sending the appropriate request
        proto based on the media type of the files.

        Args:
            dir: The input directoy containing media files.  Should contain only
                image or video files and any subdirectories will not be used.
            output_dir: The parent directory for analytic output directories.
                Each request will have it's own output directory underneath this
                parent directory.

        Returns:
            A dictionary that maps the request_id (automatically generated UUID)
            to the response proto.
        """
        # Simple directory parsing, assume one level and only image/video files
        for _, _, files in os.walk(dir): break
        output_dir = self.o_map(output_dir)
        results = {}
        for f in files:
            f = self.map(f)
            mime, type = get_media_type(f)
            logging.info("Processing {!s} of type {!s}".format(f, type))
            if type == "image":
                task = "imgManip"
                req = analytic_pb2.ImageManipulationRequest()
                req.image.uri = f
                req.image.type = mime

            elif type == "video":
                task = "vidManip"
                req = analytic_pb2.VideoManipulationRequest()
                req.video.uri = f
                req.video.type = mime

            req.request_id = str(uuid.uuid4())
            req.out_dir = os.path.join(output_dir, req.request_id)
            results[req.request_id] = self.detect_one(req, task)

        return results
