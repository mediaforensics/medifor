#!/bin/python

import click
import uuid

import logging

from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc
from google.protobuf import json_format

from medifor.v1 import mediforclient, pipeclient, medifortools, pipeline_pb2

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
    ctx.obj.client = mediforclient.MediforClient(host=host, port=port,  src=src, targ=targ, osrc=osrc, otarg=otarg)

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

@main.command()
@click.pass_context
@click.option('--probe', '-f', required=True, help="Input file (image/video) path.")
@click.option('--donor', '-d', required=False, help="Additional image file for splice task.")
@click.option('--container_out', '-o', required=True, help="Output directory for analytic to use.")
@click.option('--local_out', required=True, help="Output directory for client to use.")
def streamdetect(ctx, probe, donor, container_out, local_out):
    print(probe, donor, container_out, local_out)
    client = ctx.obj.client
    print(json_format.MessageToJson(client.stream_detection(probe=probe, donor=donor, output_dir=container_out, client_output_path=local_out)))

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
@click.argument('infile')
@click.argument('ids', nargs=-1)
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
    tags = parse_tags(tags)
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
    print(json_format.MessageToJson(ctx.obj.client.detect_info(id, want_fused)))


@pipeline.command()
@click.pass_context
@click.option('--id', '-i', required=True, default='', help='Detection ID to delete.')
def deletedetection(ctx, id):
    print(json_format.MessageToJson(ctx.obj.client.delete_detection(id)))


@pipeline.command()
@click.pass_context
@click.argument('id', nargs=1)
@click.option('--tag', '-l', multiple=True, help="Tags to merge into existing user tags")
@click.option('--delete', '-d', multiple=True, help="Tag keys to delete")
@click.option('--delete_all', is_flag=True, help="Delete all user tags")
def updatetags(ctx, id, tag, delete, delete_all):
    print(json_format.MessageToJson(ctx.obj.client.update_detection_tags(
        detection_id=id,
        tags=pipeclient.parse_tags(tag),
        delete_tags=delete,
        delete_all=delete_all,
    )))


@pipeline.command()
@click.pass_context
def taginfo(ctx):
    print(json_format.MessageToJson(ctx.obj.client.detection_tag_info()))


if __name__ == '__main__':
    main(obj=Context())
