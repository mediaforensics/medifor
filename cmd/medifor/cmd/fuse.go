package cmd

import (
	"context"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// Flags.
var (
	fusionTimeoutSeconds = 60
	fusionWantMasks      = false
)

var fuseCmd = &cobra.Command{
	Use:   "fuse",
	Short: "Send a fusion request (results for analytics run on probes) to a fusion service",
	Long: `Send a fusion request (results for analytics run on probes) to a fusion service.
For example, to fuse image manipulation results you can send

  medifor fuse imgmanip path/to/fuse_request.json`,
}

func init() {
	rootCmd.AddCommand(fuseCmd)

	fuseCmd.PersistentFlags().IntVar(&fusionTimeoutSeconds, "timeout", 5*60, "Default seconds after which an individual fusion request will time out. 0 means no timeout.")
	fuseCmd.PersistentFlags().BoolVarP(&fusionWantMasks, "want_masks", "m", false, "Whether fusion should produce masks.")

	viper.BindPFlag("timeout", fuseCmd.PersistentFlags().Lookup("timeout"))
}

func timeoutFusionFlagCtx(ctx context.Context) context.Context {
	return timeoutSecCtx(ctx, fusionTimeoutSeconds)
}
