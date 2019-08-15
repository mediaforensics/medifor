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




### Library Functions ###
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
    filename, ext = os.path.splitext(uri)
    typestring = mimetypes.types_map.get(ext, 'application/octet-stream')

    if typestring in additional_video_types:
        return typestring, 'video'

    if typestring in additional_image_types:
        return typestring, 'image'

    return typestring, typestring.split("/")[0]

class MediforClient:
    def __init__(self, host="localhost", port="50051", src='', targ='', osrc='', otarg=''):
        self.conn = "{!s}:{!s}".format(host, port)
        # self.stub = analytic_pb2_grpc.AnalyticStub(channel)

    def detect_one(self, req, task):
        with grpc.insecure_channel(self.conn) as channel:
            stub = analytic_pb2_grpc.AnalyticStub(channel)
            if task == "imgManip":
                return stub.DetectImageManipulation(req)
            elif task == "imgSplice":
                return stub.DetectImageSplice(req)
            elif task == "vidManip":
                return stub.DetectVideoManipulation(req)
            elif task == "camVal":
                return stub.DetectImageCameraMatch(req)
            else:
                logging.error("Invalid Task")
                raise

    def img_manip(self, img, output_dir):
        req = analytic_pb2.ImageManipulationRequest()
        mime, _ = get_media_type(img)
        req.image.uri = img
        req.image.type = mime
        req.request_id = str(uuid.uuid4())
        req.out_dir = output_dir

        return self.detect_one(req, "imgManip")

    def vid_manip(self, vid, output_dir):
        req = analytic_pb2.VideoManipulationRequest()
        mime, _ = get_media_type(vid)
        req.video.uri = vid
        req.video.type = mime
        req.request_id = str(uuid.uuid4())
        req.out_dir = output_dir

        return self.detect_one(req, "vidManip")


    def detect_batch(self, dir):


class Context:
    pass

@click.group()
@click.option('--host', default='localhost', show_default=True, help='Send requests to the API service on this host.')
@click.option('--port', default='50051', show_default=True, help='Send requests to the API service on this port.')
@click.option('--src', '-s', default='', help='Source directory (on host), used for mapping host files to container volume mounts.')
@click.option('--targ', '-t', default='', help='Target directory (in container), used for mapping host files to container volume mounts.')
@click.option('--osrc', '-S', default='', help='Output host-local path for mapping to output volume mounts in container.')
@click.option('--otarg', '-T', default='', help='Output target directory (in container) for mapping output host files to container files.')
@click.pass_context
def main(ctx, host, port, src, targ, osrc, otarg):
    ctx.ensure_object(Context)
    ctx.obj.client = MediforClient(host=host, port=port,  src=src, targ=targ, osrc=osrc, otarg=otarg)

@main.command()
@click.pass_context
@click.argument('img')
@click.option('--out', '-o', required=True, help="Output directory for analytic to use.")
# @click.option('--dev_id', default="", help="Camera ID for the provided image, if available.")
def imgmanip(ctx, img, out):
    client = ctx.obj.client
    print(json_format.MessageToJson(client.img_manip(img, out)))

if __name__ == '__main__':
    main(obj=Context())
