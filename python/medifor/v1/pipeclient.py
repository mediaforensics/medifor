#!/usr/bin/env python3

import click
import grpc
import mimetypes
import os.path
import sys
# import yaml
import uuid

import logging

from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc
from google.protobuf import json_format

from medifor.v1 import pipeline_pb2, pipeline_pb2_grpc
from medifor.v1.medifortools import get_detection_req, get_pipeline_req


class MediForPipeline(pipeline_pb2_grpc.PipelineStub):
    """A PipelineStub implementation that accepts an address instead of a channel.
    """
    def __init__(self, addr, src=None, targ=None, osrc=None, otarg=None, timeout=60):
        """Create a MediFor pipeline client.

        For src->targ and osrc->otarg mappings, no mapping is done if either
        end of the pair is None. If only one end is None, a ValueError
        exception is raised.

        Args:
            addr: The host:port address of the pipeline gRPC API service.
            src: The host-local input directory (maps to targ in the container).
            targ: The container-local input directory (mapped from src on host).
            osrc: The host-local output directory (maps to otarg in the container).
            otarg: The container-local output directory (mapped from osrc on host).
            timeout: Times out gRPC client connections after timeout seconds when > 0.

        Raises:
            ValueError: if either endpoint of src/targ or osrc/otarg is None
                but the other is specified.
        """
        self.addr = addr
        self.timeout = timeout

        if bool(src) != bool(targ):
            raise ValueError('src->targ mapping specified, but one end is None: {}->{}'.format(src, targ))

        if bool(osrc) != bool(otarg):
            raise ValueError('osrc->otarg mapping specified, but one end is None: {}->{}'.format(osrc, otarg))

        self.src = src
        self.targ = targ
        self.osrc = osrc
        self.otarg = otarg

        channel = grpc.insecure_channel(addr)
        super(MediForPipeline, self).__init__(channel)
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

    def detect_json(self, json_str_or_dict):
        req = pipeline_pb2.DetectionRequest()
        if isinstance(json_str_or_dict(dict)):
            json_format.ParseDict(json_str_or_dict, req.request)
        else:
            json_format.Parse(json_str_or_dict, req.request)

        resp = self.Detect(req, timeout=self.timeout)

        if isinstance(json_str_or_dict(dict)):
            return json_format.MessageToDict(resp)
        else:
            return json_format.MessageToJson(resp)

    def detect_batch(self, dir, analytic_ids=[], fuser_ids=[], output_dir="", tags=[]):
        if len(analytic_ids) == 0:
            raise ValueError("No analytic IDs specified.  Need at least one analytic ID.")

        for _, _, files in os.walk(dir): break
        output_dir = self.o_map(output_dir)
        results = []
        tags = parse_tags(tags)
        for f in files:
            f = self.map(f)
            req = get_pipeline_req(f, detection_id=str(uuid.uuid4()), analytic_ids=analytic_ids, fuser_id=fuser_ids, out_dir=output_dir, tags=tags)
            results.append(self.Detect(req))
        
        return results

    def img_manip(self, img, out_dir, ids, dev_id='', tags=None, user_tags=None, meta=None, detection_id=None, fuser_id=None):
        img = self.map(img)
        out_dir = self.o_map(out_dir)
        req = pipeline_pb2.DetectionRequest()
        req.tags.update(tags or {})
        req.user_tags.update(user_tags or {})
        req.meta.update(meta or {})
        req.id = detection_id or ''
        req.request.img_manip_req.image.uri = img
        req.request.img_manip_req.image.type = mimetypes.guess_type(img)[0]
        req.request.img_manip_req.out_dir = out_dir
        req.request.img_manip_req.hp_device_id = dev_id
        req.analytic_id.extend(ids)
        print("FuserID: {!s}".format(fuser_id))
        if fuser_id is not None:
            req.fuser_id.extend(fuser_id)

        print('created %s' % req)

        return self.Detect(req, timeout=self.timeout)

    def vid_manip(self, vid, out_dir, ids, dev_id='', tags=None, user_tags=None, meta=None, detection_id=None):
        vid = self.map(vid)
        out_dir = self.o_map(out_dir)
        req = pipeline_pb2.DetectionRequest()
        req.tags.update(tags or {})
        req.user_tags.update(user_tags or {})
        req.meta.update(meta or {})
        req.id = detection_id or ''
        req.request.vid_manip_req.video.uri = vid
        req.request.vid_manip_req.video.type = mimetypes.guess_type(vid)[0]
        req.request.vid_manip_req.out_dir = out_dir
        req.request.vid_manip_req.hp_device_id = dev_id
        req.analytic_id.extend(ids)

        return self.Detect(req, timeout=self.timeout)


    def detect_list(self, tags=None, page_size=0, page_token='', col_sort=None, fuser_id=None, want_fused=False, threshold_type=None, threshold_value=None, detection_ids=()):
        if tags is None:
            tags = {}
        cols = []
        for s in col_sort:
            sc = pipeline_pb2.SortCol()
            sc.is_asc = True
            if s.lower() == 'score':
                sc.key = pipeline_pb2.SCORE
            else:
                vals = s.split("=")
                if vals[0].lower() == 'meta':
                    keys = vals[1:]
                    for key in keys:
                        sc.key = pipeline_pb2.META
                        sc.meta_key = key
                        cols.append(sc)
                        sc = pipeline_pb2.SortCol()
                    continue
                else:
                    pass
            cols.append(sc)

        req = pipeline_pb2.DetectionListRequest(
            tags=tags,
            page_size=page_size,
            page_token=page_token,
            fuser_id=fuser_id,
            want_fused=want_fused,
            fusion_threshold_type=threshold_type,
            fusion_threshold_value=threshold_value,
            detection_ids=detection_ids,
            order_by=cols)

        return self.GetDetectionList(req)

    def detect_info(self, detection_id, want_fused):
        req = pipeline_pb2.DetectionInfoRequest(id=detection_id, want_fused=want_fused)
        return self.GetDetectionInfo(req)

    def delete_detection(self, detection_id):
        req = pipeline_pb2.DeleteDetectionRequest(detection_id=detection_id)
        return self.DeleteDetection(req)

    def detection_tag_info(self):
        req = pipeline_pb2.DetectionTagInfoRequest()
        return self.GetDetectionTagInfo(req)

    def update_detection_tags(self, detection_id, tags=None, delete_tags=(), delete_all=False):
        if tags is None:
            tags = {}
        if delete_all:
            tags = {}
            replace = True

        req = pipeline_pb2.UpdateDetectionTagsRequest(
            detection_id=detection_id,
            tags=tags,
            replace=replace,
            delete_tags=list(delete_tags))
        return self.UpdateDetectionTags(req)

    def fuse_by_id(self, fuser_ids, detection_id, out_dir, tags=None):
        if tags is None:
            tags = {}
        req = pipeline_pb2.FusionRequest()
        req.fuser_ids.extend(fuser_ids)
        req.tags.update(tags)
        req.detection_id=detection_id
        req.detection_id_out_dir = out_dir

        return self.FuseByID(req)

    def fuse_all_ids(self, fuser_ids, out_dir):
        req = pipeline_pb2.FuseAllIDsRequest(
            out_dir=out_dir,
            fuser_id=fuser_ids,
        )
        return self.FuseAllIDs(req)

    def get_analytic_meta(self):
        req = pipeline_pb2.Empty()
        return self.GetAnalyticMeta(req)

def parse_tags(tags):
    if not tags:
        return {}
    return dict(parse_tag(t) for t in tags)

def parse_tag(s):
    if not s:
        raise ValueError("Empty tag key")
    pieces = s.split('=', 1)
    while len(pieces) < 2:
        pieces.append('')
    return pieces[:2]

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


# class Context:
#     pass

# @click.group()
# @click.option('--addr', default='[::]:50051', show_default=True, help='Send requests to this API service.')
# @click.option('--src', '-s', default='', help='Source directory (on host), used for mapping host files to container volume mounts.')
# @click.option('--targ', '-t', default='', help='Target directory (in container), used for mapping host files to container volume mounts.')
# @click.option('--osrc', '-S', default='', help='Output host-local path for mapping to output volume mounts in container.')
# @click.option('--otarg', '-T', default='', help='Output target directory (in container) for mapping output host files to container files.')
# @click.pass_context
# def main(ctx, addr, src, targ, osrc, otarg):
#     ctx.ensure_object(Context)
#     ctx.obj.client = MediForPipeline(addr, src=src, targ=targ, osrc=osrc, otarg=otarg, timeout=30)


# @main.command()
# @click.pass_context
# def health(ctx):
#     print(json_format.MessageToJson(ctx.obj.client.health()))


# @main.command()
# @click.pass_context
# @click.argument('infile', type=click.File('rb'))
# @click.argument('ids', nargs=-1)
# @click.option('--detection_id', default='', help='Allows a specific detection ID to be used.')
# @click.option('--fuser_id', default=None, help="Fuser IDs for fusion algorithms to be used to fuse results")
# def detect(ctx, infile, ids, detection_id, fuser_ids):
#     req = pipeline_pb2.DetectionRequest()
#     json_format.Parse(infile.read(), req.request)
#     req.id = detection_id or ''
#     req.request.analytic_id.extend(ids)
#     if fuser_ids is not None:
#         req.fuser_id.extend(fuser_ids)
#     resp = ctx.obj.client.Detect(req)
#     print(json_format.MessageToJson(resp))


# @main.command()
# @click.pass_context
# @click.argument('img')
# @click.argument('ids', nargs=-1)
# @click.option('--out', '-o', required=True, help="Output directory for analytic to use.")
# @click.option('--dev_id', default="", help="Camera ID for the provided image, if available.")
# @click.option('--tag', '-l', multiple=True, help="Tags to apply to this detection.")
# @click.option('--user_tag', '-u', multiple=True, help="Tags to apply to this detection.")
# @click.option('--meta', '-m', multiple=True, help="Metadata key=val strings.")
# @click.option('--detection_id', default='', help='Allows a specific detection ID to be used.')
# @click.option('--fuser_id', '-f', default=None, multiple=True, help="Fuser IDs for fusion algorithms to be used to fuse results")
# def imgmanip(ctx, img, out, dev_id, tag, user_tag, meta, ids, detection_id, fuser_id):
#     if not ids:
#         logging.error("No IDs specified")
#     client = ctx.obj.client

#     print(json_format.MessageToJson(client.img_manip(
#         img, out, ids,
#         dev_id=dev_id,
#         tags=parse_tags(tag),
#         user_tags=parse_tags(user_tag),
#         meta=parse_tags(meta),
#         detection_id=detection_id,
#         fuser_id=fuser_id)))


# @main.command()
# @click.pass_context
# @click.argument('vid')
# @click.argument('ids', nargs=-1)
# @click.option('--out', '-o', required=True, help="Output directory for analytic to use.")
# @click.option('--dev_id', default="", help="Camera ID for the provided image, if available.")
# @click.option('--tag', '-l', multiple=True, help="Tags to apply to this detection.")
# @click.option('--user_tag', '-u', multiple=True, help="Tags to apply to this detection.")
# @click.option('--meta', '-m', multiple=True, help="Metadata key=val strings.")
# @click.option('--detection_id', default='', help='Allows a specific detection ID to be used.')
# def vidmanip(ctx, vid, out, dev_id, tag, user_tag, meta, ids, detection_id):
#     if not ids:
#         logging.error("No IDs specified")
#     client = ctx.obj.client
#     print(json_format.MessageToJson(client.vid_manip(
#         vid, out, ids,
#         dev_id=dev_id,
#         tags=parse_tags(tag),
#         user_tags=parse_tags(user_tag),
#         meta=parse_tags(meta),
#         detection_id=detection_id)))


# @main.command()
# @click.pass_context
# @click.option('--tag', '-l', multiple=True, help="Search for entries containing all given tags.")
# @click.option('--limit', '-n', default=100, help="Limit page size")
# @click.option('--page_token', default='', help="Page token, if any")
# @click.option('--col_sort', default = None, help = "DB  column to sort on", multiple=True)
# @click.option('--fuser_id', default = None, help = "FuserID used to sort by score", multiple=False)
# @click.option('--want_fused/--no-fused', default = False, help = "Flag to request fusion scores")
# @click.option('--threshold_type', default=None, help="Type of threshold for fusion score.  GT for greater than value, LT for less than value")
# @click.option('--threshold_value', default=None,type=float, help="Threshold for fusion score.")
# @click.option('--detection_id', multiple=True, help="Only return from this set of detection IDs.")
# def detectlist(ctx, tag, limit, page_token, col_sort, fuser_id, want_fused, threshold_type, threshold_value, detection_id):
#     if threshold_type == "GT":
#         threshold_type = pipeline_pb2.FUSION_GT_THRESHOLD
#     elif threshold_type == "LT":
#         threshold_type = pipeline_pb2.FUSION_LT_THRESHOLD
#     else:
#         threshold_type = pipeline_pb2.FUSION_NO_THRESHOLD

#     print(json_format.MessageToJson(ctx.obj.client.detect_list(
#         tags=parse_tags(tag),
#         page_size=limit,
#         page_token=page_token,
#         col_sort=col_sort,
#         fuser_id=fuser_id,
#         want_fused=want_fused,
#         detection_ids=detection_id,
#         threshold_type=threshold_type,
#         threshold_value=threshold_value)))


# @main.command()
# @click.pass_context
# @click.argument('id')
# @click.option('--want_fused/--no-fused', default = False, help = "Flag to request fusion scores")
# def detectinfo(ctx, id, want_fused):
#     print(json_format.MessageToJson(ctx.obj.client.detect_info(id,want_fused)))


# @main.command()
# @click.pass_context
# @click.option('--id', '-i', required=True, default='', help='Detection ID to delete.')
# def deletedetection(ctx, id):
#     print(json_format.MessageToJson(ctx.obj.client.delete_detection(id)))


# @main.command()
# @click.pass_context
# @click.argument('id', nargs=1)
# @click.option('--tag', '-l', multiple=True, help="Tags to merge into existing user tags")
# @click.option('--delete', '-d', multiple=True, help="Tag keys to delete")
# @click.option('--delete_all', is_flag=True, help="Delete all user tags")
# def updatetags(ctx, id, tag, delete, delete_all):
#     print(json_format.MessageToJson(ctx.obj.client.update_detection_tags(
#         detection_id=id,
#         tags=parse_tags(tag),
#         delete_tags=delete,
#         delete_all=delete_all,
#     )))


# @main.command()
# @click.pass_context
# def taginfo(ctx):
#     print(json_format.MessageToJson(ctx.obj.client.detection_tag_info()))


# @main.command()
# @click.pass_context
# @click.argument('detection_id')
# @click.argument('ids', nargs=-1)
# @click.option('--out', '-o', required=True, help="Output directory for analytic to use.")
# @click.option('--tag', '-l', multiple=True, help="Tags to apply to this detection.")
# def fusebyid(ctx, detection_id, out, tag, ids):
#     if not ids:
#         logging.error("No IDs specified")
#     client = ctx.obj.client
#     print(json_format.MessageToJson(client.fuse_by_id(ids, detection_id, out, tags=parse_tags(tag))))

# @main.command()
# @click.pass_context
# @click.option('--fuser_id', '-f', multiple=True, default=None, help="Fuser IDs to trigger fusion for on all detections.")
# @click.option('--out', '-o', help="Output directory for fusion masks.")
# def fuseall(ctx, fuser_id, out):
#     if not fuser_id:
#         logging.error("No Fuser IDs specified")
#         return
#     client = ctx.obj.client
#     client.fuse_all_ids(fuser_id, out)

# # @main.command()
# # @click.pass_context
# # @click.option('--detection_id', '-d', default=None, help="Detection ID to delete.")
# # def delete(ctx, detection_id):
# #     if not detection_id:
# #         logging.error("No DetectionID specified")
# #         return
# #     client = ctx.obj.client
# #     client.delete_detection(detection_id)

# if __name__ == '__main__':
#     main(obj=Context())
