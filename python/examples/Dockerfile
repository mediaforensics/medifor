FROM datamachines/grpc-python:1.15.0

LABEL grpc_version="1.15.0"
LABEL proto_version="3.6.1"
LABEL language="python:3.7"

RUN pip install git+https://github.com/mediaforensics/medifor.git

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

EXPOSE 50051

CMD ["python", "example_analytic.py"]
