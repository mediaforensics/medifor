package cmd

import (
	"context"
	"log"

	"github.com/spf13/cobra"
	"gitlab.mediforprogram.com/medifor/medifor-proto/pkg/medifor"
)

// detectImgmanipCmd represents the imgmanip command
var detectImgmanipCmd = &cobra.Command{
	Use:   "imgmanip",
	Short: "Send an image manipulation detection request",
	Long:  `medifor detect imgmanip /path/to/probe`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			log.Fatalf("One image argument required: <probe>")
		}

		ctx := context.Background()

		client := mustClient(ctx)
		defer client.Close()

		ctx = timeoutFlagCtx(ctx)
		detection, err := client.DetectReq(ctx, medifor.NewImageManipulationRequest(args[0],
			medifor.WithOutputPrefix(outDir),
			medifor.WithDeviceID(devID)))
		if err != nil {
			if isTimeout(err) {
				log.Printf("Detection request timed out, continuing: %v", err)
			} else {
				log.Fatalf("Failed request: %v", err)
			}
		}

		if err := outputProto(detection); err != nil {
			log.Fatalf("Output failed: %v", err)
		}
	},
}

func init() {
	detectCmd.AddCommand(detectImgmanipCmd)
}
