# Building Python Proto Stubs

You can rebuild the Python code in this directory by running `protoc.sh`. This
creates a virtual environment, pulls the needed tools into it, and runs the
generator. It then attempts to clean up the venv if possible.

This creates files with suffix `_pb2.py` and `_grpc_pb2.py`, which contain data
definitions and service/client stubs, respectively. These should be checked in,
and will be what users actually import.
