#!/bin/python

import click
# import grpc
# import mimetypes
# import os.path
# import sys
# import uuid

import logging

from grpc_health.v1 import health_pb2
from grpc_health.v1 import health_pb2_grpc
from google.protobuf import json_format

from medifor.v1 import mediforclient

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

if __name__ == '__main__':
    main(obj=Context())
