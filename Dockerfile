FROM golang:1.14-buster as gobuild

WORKDIR /go

ENV GOPATH ""

COPY . ./
RUN cd cmd/analyticproxy && go build; cd -; \
    cd cmd/medifor && go build; cd - \
    cd cmd/analyticworker && go build; cd -  \
    cd cmd/analyticworkflow && go build; cd - \
    cd cmd/fusionworker && go build; cd -

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

COPY --from=gobuild /go/cmd/analyticproxy/analyticproxy /app/bin/
COPY --from=gobuild /go/cmd/medifor/medifor /app/bin/
COPY --from=gobuild /go/cmd/analyticworker/analyticworker /app/bin
COPY --from=gobuild /go/cmd/analyticworkflow/analyticworkflow /app/bin
COPY --from=gobuild /go/cmd/fusionworker/fusionworker /app/bin
COPY --from=pybuild /usr/local/lib/python3.8/ /usr/local/lib/python3.8/

ENV PATH ${PATH}:/app/bin

EXPOSE 50051

CMD ['python', '-m', 'medifor', '--help']
