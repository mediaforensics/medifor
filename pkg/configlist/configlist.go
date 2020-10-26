package configlist

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path"

	"github.com/golang/protobuf/jsonpb"
	"github.com/pkg/errors"
	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
	yaml "gopkg.in/yaml.v3"
)

// LoadAnalyticConfig loads either a JSON or YAML file containing
// a configuration protobuf.
func LoadAnalyticConfig(configPath string) (*pb.AnalyticList, error) {
	f, err := os.Open(configPath)
	if err != nil {
		return nil, errors.Wrap(err, "load analytic config")
	}
	defer f.Close()

	switch path.Ext(configPath) {
	case ".json":
		return LoadAnalyticConfigJSON(f)
	case ".yml", ".yaml":
		return LoadAnalyticConfigYAML(f)
	default:
		return nil, errors.Errorf("Unknown config file extension in %q", configPath)
	}
}

// LoadAnalyticConfigJSON loads the JSON configuration file containing the list
// of analytics and their associated metadata.
func LoadAnalyticConfigJSON(r io.Reader) (*pb.AnalyticList, error) {
	a := new(pb.AnalyticList)
	if err := jsonpb.Unmarshal(r, a); err != nil {
		return nil, errors.Wrap(err, "load analytic config JSON - decode")
	}
	if len(a.Analytics) == 0 {
		return nil, errors.New("no analytics registered with the API server")
	}
	return a, nil
}

// LoadAnalyticConfigYAML loads configuration from a YAML file.
func LoadAnalyticConfigYAML(r io.Reader) (*pb.AnalyticList, error) {
	v := make(map[string]interface{})
	if err := yaml.NewDecoder(r).Decode(&v); err != nil {
		return nil, errors.Wrap(err, "load analytic config YAML - decode YAML")
	}

	buf := new(bytes.Buffer)
	if err := json.NewEncoder(buf).Encode(v); err != nil {
		return nil, errors.Wrap(err, "load analytic config YAML - convert to JSON")
	}

	return LoadAnalyticConfigJSON(buf)
}
