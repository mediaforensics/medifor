package cmd

import (
	"context"
	"log"

	"github.com/spf13/cobra"
	"gitlab.mediforprogram.com/medifor/medifor-proto/pkg/medifor"
)

// detectImgspliceCmd represents the imgsplice command
var detectImgspliceCmd = &cobra.Command{
	Use:   "imgsplice",
	Short: "Send an image splice detection request for a given donor and probe",
	Long:  `medifor detect imgsplice /path/to/donor /path/to/probe`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 2 {
			log.Fatalf("Two image arguments required: <donor> <probe>")
		}

		ctx := context.Background()

		client := mustClient(ctx)
		defer client.Close()

		ctx = timeoutFlagCtx(ctx)
		detection, err := client.DetectReq(ctx, medifor.NewImageSpliceRequest(args[0], args[1],
			medifor.WithOutputPrefix(outDir)))
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
	detectCmd.AddCommand(detectImgspliceCmd)
}
