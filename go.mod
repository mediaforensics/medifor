module github.com/mediaforensics/medifor

go 1.13

replace github.com/mediaforensics/medifor => ./

require (
	entrogo.com/entroq v0.3.19
	github.com/go-ini/ini v1.52.0 // indirect
	github.com/golang/protobuf v1.4.1
	github.com/google/uuid v1.1.1
	github.com/h2non/filetype v1.0.12
	github.com/lib/pq v1.8.0
	github.com/minio/minio-go v6.0.14+incompatible
	github.com/mitchellh/go-homedir v1.1.0
	github.com/pkg/errors v0.9.1
	github.com/prometheus/client_golang v0.9.3
	github.com/spf13/cobra v0.0.6
	github.com/spf13/viper v1.6.2
	golang.org/x/net v0.0.0-20200301022130-244492dfa37a
	golang.org/x/sync v0.0.0-20190911185100-cd5d95a43a6e
	google.golang.org/genproto v0.0.0-20200526211855-cb27e3aa2013
	google.golang.org/grpc v1.27.1
	google.golang.org/grpc/cmd/protoc-gen-go-grpc v1.0.1 // indirect
	google.golang.org/protobuf v1.25.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200615113413-eeeca48fe776
)
