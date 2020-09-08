// Package cmd contains individual commands used in the medifor command line client.
package cmd

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/mediaforensics/medifor/pkg/fileutil"
	"github.com/mediaforensics/medifor/pkg/medifor"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// MarshalFunc is a function that accepts a protocol buffer and outputs string info for it.
type MarshalFunc func(proto.Message) error

// Flags
var (
	cfgFile string

	hosts []string

	hostCAFile string
	myCertFile string
	myKeyFile  string

	outDir string

	clientInputRoot string
	serverInputRoot string

	clientOutputRoot string
	serverOutputRoot string

	format string
)

var (
	outputProto      MarshalFunc
	inputTranslator  *fileutil.Translator
	outputTranslator *fileutil.Translator
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "medifor",
	Short: "An analytic client command used to send requests to MediFor analytic services",
	Long: `Send requests to MediFor services, such as image manipulation detection.

The medifor command can be used to send gRPC requests to MediFor analytic services.
For example, you can ask a service to detect whether an image has been manipulated:

	medifor detect imgmanip path/to/image
`,
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error executing root command: %v", err)
	}
}

func clientOptions() []medifor.Option {
	opts := []medifor.Option{
		medifor.WithInputTranslator(inputTranslator),
		medifor.WithOutputTranslator(outputTranslator),
		medifor.WithHealthy(),
	}
	if hostCAFile != "" && myCertFile != "" && myKeyFile != "" {
		opts = append(opts, medifor.WithMTLS(hostCAFile, myCertFile, myKeyFile))
	}
	return opts
}

// mustClient creates a medifor client or fatally logs an error. Includes
// filename translation as specified by flags, if needed.
func mustClient(ctx context.Context, opts ...medifor.Option) *medifor.Client {
	if len(hosts) != 1 {
		log.Fatalf("Number of hosts was %d, expected 1", len(hosts))
	}

	opts = append(opts, clientOptions()...)

	client, err := medifor.NewClient(ctx, hosts[0], opts...)
	if err != nil {
		log.Fatalf("Error dialing client: %v", err)
	}
	return client
}

// GetAnalyticMeta provides a list of analytics and associated metadata
func (s *apiService) GetAnalyticMeta(context.Context, *wpb.Empty) (*wpb.AnalyticList, error) {
	s.Lock()
	defer s.Unlock()

	if s.analyticList != nil {
		return s.analyticList, nil
	}

	log.Printf("Loading analytic config file %q", analyticConfigPath)
	var err error
	if s.analyticList, err = configlist.LoadAnalyticConfig(analyticConfigPath); err != nil {
		return nil, errors.Wrapf(err, "load analytic list from file %q", analyticConfigPath)
	}
	return s.analyticList, nil
}

func mustFusionClient(ctx context.Context, opts ...medifor.Option) *medifor.FusionClient {
	if len(hosts) != 1 {
		log.Fatalf("Number of hosts was %d, expected 1", len(hosts))
	}

	opts = append(opts, clientOptions()...)

	client, err := medifor.NewFusionClient(ctx, hosts[0], opts...)
	if err != nil {
		log.Fatalf("Error dialing fusion client: %v", err)
	}
	return client
}

// mustMultiClient creates a medifor multiclient or fatally logs an error.
// Similar to mustClient, but does not require only one host name to have been
// provided.
func mustMultiClient(ctx context.Context, opts ...medifor.Option) *medifor.MultiClient {
	opts = append(opts, clientOptions()...)
	client, err := medifor.NewMultiClient(context.Background(), hosts, opts...)
	if err != nil {
		log.Fatalf("Error creating multi-client: %v", err)
	}
	return client
}

var defaultConfigDir string

const defaultConfigName = "medifor.yml"

func init() {
	cobra.OnInitialize(initConfig)

	defaultConfigDir = os.Getenv("XDG_CONFIG_HOME")
	if defaultConfigDir == "" {
		home, err := homedir.Dir()
		if err != nil {
			log.Fatalf("Unable to get home dir: %v", err)
		}
		defaultConfigDir = filepath.Join(home, ".config")
	}

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", filepath.Join(defaultConfigDir, defaultConfigName), "config file: yaml file with flag settings")

	rootCmd.PersistentFlags().StringSliceVarP(&hosts, "host", "H", []string{":50051"}, "Host:port of service to connect to. Can be comma-delimited list for batch mode, or flag may be specified multiple times.")

	rootCmd.PersistentFlags().StringVar(&hostCAFile, "ca", "", "CA PEM file to use for authenticating the host.")
	rootCmd.PersistentFlags().StringVar(&myCertFile, "mycert", "", "Command-line client PEM file for authenticating to the host.")
	rootCmd.PersistentFlags().StringVar(&myKeyFile, "mykey", "", "Command-line client private key file for authenticating to the host.")

	rootCmd.PersistentFlags().StringVarP(&outDir, "out", "o", "", "Output directory for masks and supplementary files. Defaults to value of --source with /$PID/out appended, otherwise to /tmp/$PID/out.")

	rootCmd.PersistentFlags().StringVarP(&clientInputRoot, "source", "s", "", "Data directory from this command's perspective. All path components under this directory are looked for under the target directory as seen by the analytic.")
	rootCmd.PersistentFlags().StringVarP(&serverInputRoot, "target", "t", "", "Data directory from the analytic's perspective.")
	rootCmd.PersistentFlags().StringVarP(&clientOutputRoot, "out_source", "S", "", "Output directory as seen from this command's perspective. Defaults to value of --source.")
	rootCmd.PersistentFlags().StringVarP(&serverOutputRoot, "out_target", "T", "", "Output directory as seen from the analytic's perspective. Defaults to value of --target.")

	rootCmd.PersistentFlags().StringVarP(&format, "format", "f", "json", "Format of output, e.g., 'json' or 'proto'.")

	viper.BindPFlag("ca", rootCmd.PersistentFlags().Lookup("ca"))
	viper.BindPFlag("mycert", rootCmd.PersistentFlags().Lookup("mycert"))
	viper.BindPFlag("mykey", rootCmd.PersistentFlags().Lookup("mykey"))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		viper.AddConfigPath(defaultConfigDir)
		viper.SetConfigName(defaultConfigName)
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err != nil {
		// If we just didn't find it, that's fine. Otherwise, we probably want
		// to know if, e.g., the file was corrupt.
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			log.Printf("No config read: %v", err)
		}
	} else {
		log.Printf("Using config file %q", viper.ConfigFileUsed())
	}

	if clientOutputRoot == "" {
		clientOutputRoot = clientInputRoot
	}
	if serverOutputRoot == "" {
		serverOutputRoot = serverInputRoot
	}

	if outDir == "" {
		pid := fmt.Sprint(os.Getpid())
		if clientOutputRoot == "" {
			outDir = filepath.Join("/tmp", pid, "out")
		} else {
			outDir = filepath.Join(clientOutputRoot, pid, "out")
		}
	}

	inputTranslator = fileutil.NewTranslator(clientInputRoot, serverInputRoot)
	outputTranslator = fileutil.NewTranslator(clientOutputRoot, serverOutputRoot)

	switch strings.ToLower(format) {
	case "json", "":
		outputProto = func(p proto.Message) error {
			if err := new(jsonpb.Marshaler).Marshal(os.Stdout, p); err != nil {
				return err
			}
			os.Stdout.Write([]byte("\n"))
			return nil
		}
	case "proto":
		outputProto = func(p proto.Message) error {
			return proto.MarshalText(os.Stdout, p)
		}
	default:
		log.Fatalf("Unknown output format %q", format)
	}
	log.Printf("out: %v", outDir)
}
