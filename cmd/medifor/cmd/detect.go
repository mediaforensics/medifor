package cmd

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Flags.
var devID string
var detectTimeoutSeconds = 60

// detectCmd represents the detect command
var detectCmd = &cobra.Command{
	Use:   "detect",
	Short: "Send a detection request to an analytic service.",
	Long: `Send a detection request to a running analytic.
For example, to detect image manipulation, you can send

	medifor detect imgmanip path/to/image

and that will send a DetectImageManipulation request to the default port.`,
}

func init() {
	rootCmd.AddCommand(detectCmd)

	detectCmd.PersistentFlags().StringVar(&devID, "dev_id", "", "Camera ID for detection commands that can use one (i.e., imgmanip, vidmanip).")

	detectCmd.PersistentFlags().IntVar(&detectTimeoutSeconds, "timeout", 5*60, "Default seconds after which an individual detect request will time out. 0 means no timeout.")

	viper.BindPFlag("timeout", detectCmd.PersistentFlags().Lookup("timeout"))
}

func timeoutSecCtx(ctx context.Context, seconds int) context.Context {
	if seconds <= 0 {
		return ctx
	}
	ctx, _ = context.WithTimeout(ctx, time.Duration(seconds)*time.Second)
	return ctx
}

func timeoutFlagCtx(ctx context.Context) context.Context {
	return timeoutSecCtx(ctx, detectTimeoutSeconds)
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	err = errors.Cause(err)
	if err == context.DeadlineExceeded {
		return true
	}
	if status.Code(err) == codes.DeadlineExceeded {
		return true
	}

	return false
}
