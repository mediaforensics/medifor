FROM golang:1.14-buster as builder

WORKDIR /go

ENV GOPATH ""

COPY . ./
RUN cd cmd/analyticproxy && go build; cd -; \
    cd cmd/medifor && go build; cd -

FROM ubuntu:18.04

ENV PATH ${PATH}:/app/bin
RUN mkdir -p /app/bin

COPY --from=builder /go/cmd/analyticproxy/analyticproxy /app/bin/
COPY --from=builder /go/cmd/medifor/medifor /app/bin/

EXPOSE 50051

CMD ["analyticproxy", "--help"]
