package cmd

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"entrogo.com/entroq"
	rpcbackend "entrogo.com/entroq/grpc"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/mediaforensics/medifor/pkg/analytic"
	"github.com/mediaforensics/medifor/pkg/fusionworker"
	"github.com/mediaforensics/medifor/pkg/fileutil"
	"github.com/mediaforensics/medifor/pkg/medifor"
	"golang.org/x/sync/errgroup"
)

const (
	DefaultFuserInboxFmt = "/task/fusion/fuser/%s/inbox"
	DefaultMaxAttempts   = 3
)

// Flags
var (
	cfgFile      string
	analyticAddr string
	analyticID   string
	qAddr        string
	qFmts        []string

	maxAttempts        int
	inputMapping       string
	fusionInputMapping string
	outputMapping      string

	memoizeResults bool

	metricsPort int
)

// Execute adds all child commands to the root command and sets flags.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatalf("Error executing root command: %v", err)
	}
}

// mustParseMapping will parse a path:path string into its two parts or die trying.
func mustParseMapping(m string) medifor.PathTranslator {
	if m == "" {
		return medifor.DefaultPathTranslator
	}
	parts := strings.Split(m, ":")
	if len(parts) != 2 {
		log.Fatalf("Invalid mapping string %q: expect 2 colon-separated parts", m)
	}
	return fileutil.NewTranslator(strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]))

}

// mediforClient creates a medifor client from flags.
func mediforClient(ctx context.Context) (*medifor.FusionClient, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	mfc, err := medifor.NewFusionClient(ctx, analyticAddr,
		medifor.WithWaitForBackend(),
		medifor.WithInputTranslator(mustParseMapping(inputMapping)),
		medifor.WithOutputTranslator(mustParseMapping(outputMapping)),
		medifor.WithFusionInputTranslator(mustParseMapping(fusionInputMapping)),
	)
	if err != nil {
		return nil, fmt.Errorf("medifor client: %v", err)
	}
	return mfc, nil
}

// entroQClient creates an EntroQ client from flags.
func entroQClient(ctx context.Context) (*entroq.EntroQ, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	eqc, err := entroq.New(ctx, rpcbackend.Opener(qAddr, rpcbackend.WithInsecure(), rpcbackend.WithBlock()))
	if err != nil {
		return nil, fmt.Errorf("entroq client: %v", err)
	}
	return eqc, nil
}

// rootCmd represents the root command
var rootCmd = &cobra.Command{
	Use:   "fusionworker",
	Short: "Run an analytic worker, backed by a given EntroQ service and a MediFor analytic service.",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := context.Background()
		qNames := analytic.AnalyticInboxes(qFmts, analyticID)
		if len(qNames) == 0 {
			log.Fatal("No queue formats specified.")
		}

		eqc, err := entroQClient(ctx)
		if err != nil {
			log.Fatalf("Failed to open connection to EntroQ gRPC service: %v", err)
		}
		defer eqc.Close()

		mfc, err := mediforClient(ctx)
		if err != nil {
			log.Fatalf("Failed to open connection to analytic gRPC service: %v", err)
		}
		defer mfc.Close()

		g, ctx := errgroup.WithContext(ctx)

		g.Go(func() error {
			log.Printf("Running %q worker: %q <- %q", analyticID, analyticAddr, qNames)
			worker := fusionworker.New(mfc, eqc, qNames,
				fusionworker.WithMaxAttempts(maxAttempts),
				fusionworker.WithMemoization(memoizeResults))
			return errors.Wrap(worker.Run(ctx), "worker exited")
		})

		g.Go(func() error {
			http.Handle("/metrics", promhttp.Handler())
			return errors.Wrap(http.ListenAndServe(fmt.Sprintf(":%d", metricsPort), nil), "metrics server exited")
		})

		log.Fatal(g.Wait())
	},
}

func init() {
	cobra.OnInitialize(initConfig)
	pflags := rootCmd.PersistentFlags()

	pflags.StringVar(&cfgFile, "config", "", "Location of configuration file, if wanted instead of flags.")
	pflags.StringVarP(&analyticAddr, "backend", "a", ":50051", "Address of analytic service backend.")
	pflags.StringVarP(&analyticID, "id", "i", "", "Analytic ID to listen for. Used with --qfmt to determine which queue to listen on.")
	pflags.StringVarP(&qAddr, "eqaddr", "t", ":37706", "Address of task queue.")
	pflags.StringArrayVar(&qFmts, "qfmt", []string{DefaultFuserInboxFmt}, "Format strings for producing a task inbox from an analytic ID. The ID will be URL-escaped before being inserted into the single '%s' value in the format string.")
	pflags.IntVarP(&maxAttempts, "max_attempts", "n", DefaultMaxAttempts, "Maximum number of times a task will be attempted before returning to the outbox as an error.")
	pflags.StringVarP(&inputMapping, "xin", "x", "", "Mapping for inputs <caller_perspective>:<callee perspective>")
	pflags.StringVar(&fusionInputMapping, "xfin", "", "Mapping for analytic outputs = fusion inputs (not probes) <caller_perspective>:<callee_perspective>")
	pflags.StringVarP(&outputMapping, "xout", "X", "", "Mapping for outputs <caller_perspective>:<callee perspective>")
	pflags.BoolVar(&memoizeResults, "memoize", false, "Write results to output directory as json files as well as pushing to outbox.")
	pflags.IntVar(&metricsPort, "metrics_port", 2112, "Port for Prometheus metrics service")

	viper.BindPFlag("backend", pflags.Lookup("backend"))
	viper.BindPFlag("id", pflags.Lookup("id"))
	viper.BindPFlag("eqaddr", pflags.Lookup("eqaddr"))
	viper.BindPFlag("qFmt", pflags.Lookup("qFmt"))
	viper.BindPFlag("max_attempts", pflags.Lookup("max_attempts"))
	viper.BindPFlag("xin", pflags.Lookup("xin"))
	viper.BindPFlag("xfin", pflags.Lookup("xfin"))
	viper.BindPFlag("xout", pflags.Lookup("xout"))
	viper.BindPFlag("memoize", pflags.Lookup("memoize"))

	rootCmd.MarkPersistentFlagRequired("analytic_id")
}

func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			log.Fatalf("Error getting home directory: %v", err)
		}

		// Search config in home directory with name ".medifor" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".medifor")
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
}
