from setuptools import setup, find_packages

setup(name='pipeclient',
      version='1.0.0a2',
      description='Simple client for calling into MediFor pipeline service',
      author='Data Machines Corp.',
      author_email='chrismonson@datamachines.io',
      url='gitlab.mediforprogram.com/medifor/analytic-worker/py/pipeclient',
      license='Apache License, Version 2.0',
      packages=find_packages(),
      dependency_links=[
         'git+ssh://git@gitlab.mediforprogram.com/medifor/medifor-proto.git@develop#egg=medifor_proto-1.0.0a2'
         #'git+https://gitlab.mediforprogram.com/medifor/medifor-proto.git@develop#egg=medifor_proto-1.0.0a2'
      ],
      install_requires=[
          'Click',
          'dataclasses==0.6',
          'setuptools==39.0.1',
          'grpcio==1.15.0',
          'grpcio-tools==1.15.0',
          'grpcio_health_checking==1.15.0',
          'protobuf==3.6.1',
          'six==1.12.0',
      ],
      py_modules=[
          'pipeclient',
          'pipeline_pb2',
          'pipeline_pb2_grpc',
          'task_pb2',
          'task_pb2_grpc',
          'dataset'
      ])
