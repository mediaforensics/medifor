import os
import sys

def iter_protos(parent=None):
    for root, _, files in os.walk('proto'):
        if not files:
            continue
        dest = root if not parent else os.path.join(parent, root)
        yield dest, [os.path.join(root, f) for f in files]

from setuptools import setup, find_packages

pkg_name = 'medifor_proto'

setup(name=pkg_name,
      package_dir={
          '': 'python',
      },
      version='1.0.0a3',
      description='Protocol wrapper for MediFor Analytics',
      author='Data Machines Corp.',
      author_email='help@mediforprogram.com',
      url='gitlab.mediforprogram.com/medifor/medifor-proto/py',
      license='Apache License, Version 2.0',
      packages=['medifor', 'medifor.v1'],#, 'medifor.v2'],
      install_requires=[
          'setuptools==39.0.1',
          'grpcio==1.15.0',
          'grpcio-tools==1.15.0',
          'grpcio_health_checking==1.15.0',
          'protobuf==3.6.1',
          'googleapis-common-protos==1.6.0'
      ],
      data_files=list(iter_protos(pkg_name)),
      py_modules=[
          'medifor.v1.analytic_pb2',
          'medifor.v1.analytic_pb2_grpc',
          'medifor.v1.fifoconn',
          'medifor.v1.fusion_pb2',
          'medifor.v1.fusion_pb2_grpc',
          'medifor.v1.analyticservice',
          'medifor.v1.fusionservice',
          # 'google.rpc.__init__', # needed for py2 proto
          # 'google.rpc.status_pb2',
          # 'google.rpc.code_pb2',
          # 'google.rpc.error_details_pb2',
      ]
      )
