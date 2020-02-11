import os
import sys

def iter_protos(parent=None):
    for root, _, files in os.walk('proto'):
        if not files:
            continue
        dest = root if not parent else os.path.join(parent, root)
        yield dest, [os.path.join(root, f) for f in files]

from setuptools import setup, find_packages

pkg_name = 'medifor'

setup(name=pkg_name,
      package_dir={
          '': 'python',
      },
      version='0.2.3',
      description='Protocol wrapper for MediFor Analytics',
      author='Data Machines Corp.',
      author_email='help@mediforprogram.com',
      url='gitlab.mediforprogram.com/medifor/medifor-proto/py',
      license='Apache License, Version 2.0',
      packages=find_packages(),
      install_requires=[
          'setuptools==39.0.1',
          'grpcio==1.15.0',
          'grpcio_health_checking==1.15.0',
          'protobuf>=3.6.1',
          'googleapis-common-protos==1.6.0',
          'Click',
          'requests==2.22.0',
          'Flask==1.1.1',
          'python-magic-bin==0.4.14',
      ],
      data_files=list(iter_protos(pkg_name)),
      py_modules=[
          'medifor.v1.analytic_pb2',
          'medifor.v1.analytic_pb2_grpc',
          'medifor.v1.streamingproxy_pb2',
          'medifor.v1.streamingproxy_pb2_grpc',
          'medifor.v1.fifoconn',
          'medifor.v1.fusion_pb2',
          'medifor.v1.fusion_pb2_grpc',
          'medifor.v1.analyticservice',
          'medifor.v1.fusionservice',
          'medifor.v1.mediforclient',
          'medifor.v1.provenanceservice',
          'medifor.v1.provenance_pb2',
          'medifor.v1.provenance_pb2_grpc',
          'medifor.v1.cli',
          'medifor.v1.pipeclient',
          'medifor.v1.pipeline_pb2',
          'medifor.v1.pipeline_pb2_grpc',
          'medifor.v1.medifortools',
          'medifor.v1.provclient',
          'medifor.__main__'
      ]
      )
