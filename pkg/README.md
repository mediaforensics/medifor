# Generating Code From the Proto Directory (For Go)

Make sure you have
[protoc](https://developers.google.com/protocol-buffers/docs/downloads)
installed, then install the plugin if you haven't already:

```
go get -u github.com/golang/protobuf/protoc-gen-go
```

This installs it in `$GOBIN` or `$GOPATH/bin`. This needs to be on your
`$PATH`. Then you can call `protoc.sh`, which generates the .go files:

```
./protoc.sh
```
