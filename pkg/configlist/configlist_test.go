package configlist

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"testing"

	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
	yaml "gopkg.in/yaml.v3"
)

func TestLoadAnalyticConfig_JSON(t *testing.T) {
	configFilePath := "test_config.json"
	var analytics *pb.AnalyticList
	analytics, err := LoadAnalyticConfig(configFilePath)
	if err != nil {
		t.Fatalf("loading analytic list config %v", err)
	}
	if analytics == nil {
		t.Fatal("Error: No analytics loaded")
	}
	if len(analytics.Analytics) != 3 {
		t.Fatalf("%v analytics were loaded. Should be 3", len(analytics.Analytics))
	}
}

func TestLoadAnalyticConfig_YAML(t *testing.T) {
	originalPath := "test_config.json"
	jsonFile, err := os.Open(originalPath)
	if err != nil {
		t.Fatalf("Could not open %q: %v", originalPath, err)
	}
	defer jsonFile.Close()

	v := make(map[string]interface{})
	if err := json.NewDecoder(jsonFile).Decode(&v); err != nil {
		t.Fatalf("Could not decode JSON input file %q: %v", originalPath, err)
	}

	yamlFile, err := ioutil.TempFile("", "analytic-config-test-*.yml")
	if err != nil {
		t.Fatalf("Could not create temporary YAML file: %v", err)
	}
	defer yamlFile.Close()

	if err := yaml.NewEncoder(yamlFile).Encode(v); err != nil {
		t.Fatalf("Could not write YAML version of analytic config to %q: %v", yamlFile.Name(), err)
	}

	analytics, err := LoadAnalyticConfig(yamlFile.Name())
	if err != nil {
		t.Fatalf("Unexpected error loading analytic config: %v", err)
	}
	if analytics == nil {
		t.Fatal("No analytics loaded")
	}
	if got := len(analytics.Analytics); got != 3 {
		t.Fatalf("Want 3 analytics, got %d", got)
	}
}
