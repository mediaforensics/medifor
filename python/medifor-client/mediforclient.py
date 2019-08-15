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

class MediforClient:
    """
    MediforClient provides a client for communicating with media forensic analytics.
    The client will establish a connection with the analytic at the host and port
    specified when instantiating the class.  Path translation coming soon.
    """
    def __init__(self, host="localhost", port="50051", src='', targ='', osrc='', otarg=''):
        self.conn = "{!s}:{!s}".format(host, port)
        self.src = src
        self.targ = targ
        self.osrc = osrc
        self.otarg = otarg
        
        # self.stub = analytic_pb2_grpc.AnalyticStub(channel)

    def detect_one(self, req, task):
        """
        'detect_one' sends a single request of type 'DetectImageManipulation' or
        'DetectVideoManipulation'.  Takes as argument a request (of type
        "ImageManipulationRequest" or "VideoManipulationRequest") and a task string
        (either "imgManip" or "vidManip").  Returns the appropriate response proto.
        """
        with grpc.insecure_channel(self.conn) as channel:
            stub = analytic_pb2_grpc.AnalyticStub(channel)
            if task == "imgManip":
                return stub.DetectImageManipulation(req)
            elif task == "vidManip":
                return stub.DetectVideoManipulation(req)
            else:
                logging.error("Invalid Task")
                raise

    def img_manip(self, img, output_dir):
        """
        'img_manip' builds an "ImageManipulationRequest" using the image uri
        and output directory provided as an argument, and calls 'detect_one'
        using the built request and the "imgManip" task.  Returns the response
        "ImageManipulation" proto.
        """
        req = analytic_pb2.ImageManipulationRequest()
        mime, _ = get_media_type(img)
        req.image.uri = img
        req.image.type = mime
        req.request_id = str(uuid.uuid4())
        req.out_dir = output_dir

        return self.detect_one(req, "imgManip")

    def vid_manip(self, vid, output_dir):
        """
        'vid_manip' builds an "VideoManipulationRequest" using the image uri
        and output directory provided as an argument, and calls 'detect_one'
        using the built request and the "vidManip" task.  Returns the response
        "VideoManipulation" proto.
        """
        req = analytic_pb2.VideoManipulationRequest()
        mime, _ = get_media_type(vid)
        req.video.uri = vid
        req.video.type = mime
        req.request_id = str(uuid.uuid4())
        req.out_dir = output_dir

        return self.detect_one(req, "vidManip")


    def detect_batch(self, dir, output_dir):
        """
        'detect_batch' takes as input a directory of media (image or video) files
        and a parent output directory.  The input directory should contain only
        image or video files, and the function will not parse subdirectories.  The
        output directory specified acts as the parent directory for all of the analytic
        output files.  For each file in the input directory the function will
        build the approriate proto, assigning each request a UUID and using this
        UUID to specify the subfolder with the output directory which is used as
        the out_dir when building the request.  Returns a dictionary that maps
        the request_id to the response proto.
        """
        # Simple directory parsing, assume one level and only image/video files
        for _, _, files in os.walk(dir): break

        results = {}
        for f in files:
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
def imgmanip(ctx, img, out):
    client = ctx.obj.client
    print(json_format.MessageToJson(client.img_manip(img, out)))

@main.command()
@click.pass_context
@click.argument('vid')
@click.option('--out', '-o', required=True, help="Output directory for analytic to use.")
def vidmanip(ctx, vid, out):
    client = ctx.obj.client
    print(json_format.MessageToJson(client.vid_manip(vid, out)))

@main.command()
@click.pass_context
@click.option('--dir', '-d', required=True, help="Input directory containing images or videos.")
@click.option('--out', '-o', required=True, help="Output directory for analytic to use.")
def detectbatch(ctx, dir, out):
    client = ctx.obj.client
    results = client.detect_batch(dir, out)
    output_dict = {}
    for id, resp in results.items():
        json_resp = json_format.MessageToJson(resp)
        output_dict[id] = json_resp

    print(output_dict)


if __name__ == '__main__':
    main(obj=Context())
