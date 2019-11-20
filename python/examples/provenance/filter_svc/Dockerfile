FROM ubuntu:18.04

RUN apt update && apt upgrade -y python3
RUN apt install -y python3-pip
RUN apt install -y libopenblas-base libomp5 libomp-dev
RUN apt-get install -y git

WORKDIR /app
COPY *.py *.txt ./

RUN pip3 install -r requirements.txt
RUN pip3 install --no-cache-dir git+https://github.com/mediaforensics/medifor.git@feature/provenance

ENTRYPOINT ["python3", "./filtersvc.py"]