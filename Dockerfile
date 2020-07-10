FROM golang:1.14-buster as gobuild

WORKDIR /go

ENV GOPATH ""

COPY . ./
RUN cd cmd/analyticproxy && go build; cd -; \
    cd cmd/medifor && go build; cd -

FROM python:3.8-slim-buster as pybuild

RUN mkdir -p /src/medifor/python \
 && apt update \
 && apt install -y g++ python3-magic

COPY ./setup.py /src/medifor/
COPY ./python/ /src/medifor/python/

RUN pip install -U pip && pip install /src/medifor

RUN mkdir -p /app/bin
COPY --from=gobuild /go/cmd/analyticproxy/analyticproxy /app/bin/
COPY --from=gobuild /go/cmd/medifor/medifor /app/bin/

ENV PATH ${PATH}:/app/bin

EXPOSE 50051

CMD ['python', '-m', 'medifor', '--help']
