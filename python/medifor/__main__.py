#!/bin/python

import click
import functools
import grpc
import json
import uuid
import sys

import logging

from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc
from google.protobuf import json_format

from medifor.v1 import mediforclient, pipeclient, medifortools, pipeline_pb2, provclient


class Context:
    pass


def _rpc_error_leaf_nodes(rpc_error):
    def _find_leaves(debug_info):
        errs = debug_info.get('referenced_errors')
        if debug_info is None or errs is None:
            yield debug_info
            return

        for e in errs:
            yield from _find_leaves(e)

    debug_info = json.loads(rpc_error.debug_error_string())
    yield from _find_leaves(debug_info)


def friendly_rpc_errors(f):
    @functools.wraps(f)
    def wrapper(*args, **kargs):
        try:
            f(*args, **kargs)
        except grpc.RpcError as e:
            print("RPC_ERROR ({code}) {detail}:".format(code=e.code(), detail=e.details()), file=sys.stderr)
            for root_cause in _rpc_error_leaf_nodes(e):
                print(json.dumps(root_cause, sort_keys=True, indent=2), file=sys.stderr)
            return
    return wrapper


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
    ctx.obj.host = host
    ctx.obj.port = port
    ctx.obj.src = src
    ctx.obj.targ = targ
    ctx.obj.osrc = osrc
    ctx.obj.otarg = otarg
    # ctx.obj.client = mediforclient.MediforClient(host=host, port=port,  src=src, targ=targ, osrc=osrc, otarg=otarg)

# @main.command()
# @click.pass_context
# @friendly_rpc_errors
# def health(ctx):
#     client = ctx.obj.client
#     print(json_format.MessageToJson(client.health()))


@main.group()
@click.pass_context
def detect(ctx):
    ctx.obj.client = mediforclient.MediforClient(host=ctx.obj.host, port=ctx.obj.port,  src=ctx.obj.src,
                                                 targ=ctx.obj.targ, osrc=ctx.obj.osrc, otarg=ctx.obj.otarg)


@detect.command()
@click.pass_context
@click.argument('img')
@click.option('--out', '-o', required=True, help="Output directory for analytic to use.")
def imgmanip(ctx, img, out):
    client = ctx.obj.client
    print(json_format.MessageToJson(client.img_manip(img, out)))


@detect.command()
@click.pass_context
@click.argument('vid')
@click.option('--out', '-o', required=True, help="Output directory for analytic to use.")
def vidmanip(ctx, vid, out):
    client = ctx.obj.client
    print(json_format.MessageToJson(client.vid_manip(vid, out)))


@detect.command()
@click.pass_context
@click.option('--dir', '-d', required=True, help="Input directory containing images or videos.")
@click.option('--out', '-o', required=True, help="Output directory for analytic to use.")
@click.option('--make_dirs/--no-make_dirs', '-m', default=False, help="If true, will make subdirectories using the request_id from the client (only works if client has access to the container's output directory)")
def detectbatch(ctx, dir, out, make_dirs):
    client = ctx.obj.client
    results = client.detect_batch(dir, out, make_dirs)
    output_dict = {}
    for id, resp in results.items():
        json_resp = json_format.MessageToJson(resp)
        output_dict[id] = json_resp

    print(output_dict)


@detect.command()
@click.pass_context
@click.option('--probe', '-f', required=True, help="Input file (image/video) path.")
@click.option('--donor', '-d', required=False, help="Additional image file for splice task.")
@click.option('--container_out', '-o', required=True, help="Output directory for analytic to use.")
@click.option('--local_out', required=True, help="Output directory for client to use.")
def streamdetect(ctx, probe, donor, container_out, local_out):
    client = ctx.obj.client
    print(json_format.MessageToJson(client.stream_detection(probe=probe, donor=donor, output_dir=container_out, client_output_path=local_out)))

###################################################################
@main.group()
@click.pass_context
def provenance(ctx):
    ctx.obj.client = provclient.ProvenanceClient(host=ctx.obj.host, port=ctx.obj.port,  src=ctx.obj.src,
                                                 targ=ctx.obj.targ, osrc=ctx.obj.osrc, otarg=ctx.obj.otarg)


@provenance.command()
@click.argument('img')
@click.option('--limit', '-l', required=False, default=5, help="Output directory for analytic to use.")
@click.pass_context
def filter(ctx, img, limit):
    client = ctx.obj.client
    print(json_format.MessageToJson(client.prov_filter(img=img, limit=limit)))


###################################################################
@main.group()
@click.option('--host', default='localhost', show_default=True, help='Send requests to the API service on this host.')
@click.option('--port', default='50051', show_default=True, help='Send requests to the API service on this port.')
@click.option('--src', '-s', default='', help='Source directory (on host), used for mapping host files to container volume mounts.')
@click.option('--targ', '-t', default='', help='Target directory (in container), used for mapping host files to container volume mounts.')
@click.option('--osrc', '-S', default='', help='Output host-local path for mapping to output volume mounts in container.')
@click.option('--otarg', '-T', default='', help='Output target directory (in container) for mapping output host files to container files.')
@click.pass_context
def pipeline(ctx, host, port, src, targ, osrc, otarg):
    ctx.ensure_object(Context)
    addr = "{!s}:{!s}".format(host, port)
    ctx.obj.pipeclient = pipeclient.MediForPipeline(addr=addr, src=src, targ=targ, osrc=osrc, otarg=otarg)


@pipeline.command()
@click.pass_context
@friendly_rpc_errors
def health(ctx):
    client = ctx.obj.pipeclient
    print(json_format.MessageToJson(client.health()))


@pipeline.command()
@click.pass_context
@click.argument('infile')
@click.option('--detection_id', default=None, help='Allows a specific detection ID to be used.')
@click.option('--fuser_id', multiple=True, help="Fuser IDs for fusion algorithms to be used to fuse results")
@click.option("--out", required=True, help="Output Directory for results.")
@click.option("--tag", "-t", multiple=True, help="Tag maps to apply of the form `tag=value` or 'tag'.")
@click.option("--analytic_id", "-i", multiple=True, help="Analytic IDs to use to process media files")
def detect(ctx, infile, analytic_id, detection_id, fuser_id, out, tag):
    if not detection_id:
        detection_id = str(uuid.uuid4())
    f = ctx.obj.pipeclient.map(infile)
    out = ctx.obj.pipeclient.o_map(out)
    tags = pipeclient.parse_tags(tags)
    req = medifortools(f, detection_id=detection_id, analytic_ids=analytic_id, out_dir=out, fuser_id=fuser_id, tags=tags)
    print(json_format(ctx.obj.pipeclient.Detect(req)))


@pipeline.command()
@click.pass_context
@click.option("--dir", required=True, help="Directory of media files to process.")
@click.option("--out", required=True, help="Output Directory for results.")
@click.option("--analytic_id", "-i", multiple=True, help="Analytic IDs to use to process media files")
@click.option("--fuser_id", "-f", multiple=True, help="Fuser IDs to use to fuse results.")
@click.option("--tag", "-t", multiple=True, help="Tag maps to apply of the form `tag=value` or 'tag'.")
def systembatch(ctx, dir, analytic_id, fuser_id, out, tag):
    print(json_format(ctx.obj.pipeclient.detect_batch(dir=dir, analytic_id=analytic_id, fuser_id=fuser_id, output_dir=out, tags=tag)))


@pipeline.command()
@click.pass_context
@click.option('--tag', '-l', multiple=True, help="Search for entries containing all given tags.")
@click.option('--limit', '-n', default=100, help="Limit page size")
@click.option('--page_token', default='', help="Page token, if any")
@click.option('--col_sort', default=None, help="DB  column to sort on", multiple=True)
@click.option('--fuser_id', default=None, help="FuserID used to sort by score", multiple=False)
@click.option('--want_fused/--no-fused', default=False, help="Flag to request fusion scores")
@click.option('--threshold_type', default=None, help="Type of threshold for fusion score.  GT for greater than value, LT for less than value")
@click.option('--threshold_value', default=None, type=float, help="Threshold for fusion score.")
@click.option('--detection_id', multiple=True, help="Only return from this set of detection IDs.")
def detectlist(ctx, tag, limit, page_token, col_sort, fuser_id, want_fused, threshold_type, threshold_value, detection_id):
    if threshold_type == "GT":
        threshold_type = pipeline_pb2.FUSION_GT_THRESHOLD
    elif threshold_type == "LT":
        threshold_type = pipeline_pb2.FUSION_LT_THRESHOLD
    else:
        threshold_type = pipeline_pb2.FUSION_NO_THRESHOLD

    print(json_format.MessageToJson(ctx.obj.pipeclient.detect_list(
        tags=pipeclient.parse_tags(tag),
        page_size=limit,
        page_token=page_token,
        col_sort=col_sort,
        fuser_id=fuser_id,
        want_fused=want_fused,
        detection_ids=detection_id,
        threshold_type=threshold_type,
        threshold_value=threshold_value)))


@pipeline.command()
@click.pass_context
@click.argument('id')
@click.option('--want_fused/--no-fused', default=False, help="Flag to request fusion scores")
def detectinfo(ctx, id, want_fused):
    print(json_format.MessageToJson(ctx.obj.pipeclient.detect_info(id, want_fused)))


@pipeline.command()
@click.pass_context
@click.option('--id', '-i', required=True, default='', help='Detection ID to delete.')
def deletedetection(ctx, id):
    print(json_format.MessageToJson(ctx.obj.pipeclient.delete_detection(id)))


@pipeline.command()
@click.pass_context
@click.argument('id', nargs=1)
@click.option('--tag', '-l', multiple=True, help="Tags to merge into existing user tags")
@click.option('--delete', '-d', multiple=True, help="Tag keys to delete")
@click.option('--delete_all', is_flag=True, help="Delete all user tags")
def updatetags(ctx, id, tag, delete, delete_all):
    print(json_format.MessageToJson(ctx.obj.pipeclient.update_detection_tags(
        detection_id=id,
        tags=pipeclient.parse_tags(tag),
        delete_tags=delete,
        delete_all=delete_all,
    )))


@pipeline.command()
@click.pass_context
def taginfo(ctx):
    print(json_format.MessageToJson(ctx.obj.pipeclient.detection_tag_info()))


if __name__ == '__main__':
    main(obj=Context())
