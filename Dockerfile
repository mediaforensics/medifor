FROM golang:1.14-buster as gobuild

WORKDIR /go/src/github.com/mediaforensics/medifor/
COPY . ./

RUN apt-get update && apt-get upgrade -y && apt-get install -y protobuf-compiler
RUN go get -u github.com/golang/protobuf/protoc-gen-go
RUN cd pkg && ./protoc.sh

#ENV GOPATH ""

RUN mkdir -p /app/bin/
RUN for cmdname in `ls cmd/`; \
      do \
        cd cmd/$cmdname && go build && cp $cmdname /app/bin/ && cd -; \
    done

FROM python:3.8-slim-buster as pybuild

RUN mkdir -p /src/medifor/python \
 && apt-get update \
 && apt-get install -y g++

COPY ./setup.py /src/medifor/
COPY ./python/ /src/medifor/python/

RUN pip install -U pip && pip install /src/medifor

FROM python:3.8-slim-buster

RUN mkdir -p /app/bin \
 && apt-get update \
 && apt-get install -y libmagic1

COPY --from=gobuild /app/bin/ /app/bin/
COPY --from=pybuild /usr/local/lib/python3.8/ /usr/local/lib/python3.8/

ENV PATH ${PATH}:/app/bin

EXPOSE 50051

CMD ['python', '-m', 'medifor', '--help']
