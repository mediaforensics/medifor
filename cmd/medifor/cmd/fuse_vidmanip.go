package cmd

import (
	"context"
	"log"
	"os"

	"github.com/golang/protobuf/jsonpb"
	"github.com/spf13/cobra"
	pb "gitlab.mediforprogram.com/medifor/medifor-proto/pkg/mediforproto"
)

// fuseVidmanipCmd represents the vidmanip command when run for fusion.
var fuseVidmanipCmd = &cobra.Command{
	Use:   "vidmanip",
	Short: "Send a fusion request for video manipulation results",
	Long: `medifor fuse vidmanip /path/to/request.json

	Where requests.json is a JSON representation of a FuseVideoManipulationRequest proto.
	`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			log.Fatalf("One JSON file containing results is required")
		}

		ctx := context.Background()

		client := mustFusionClient(ctx)
		defer client.Close()

		ctx = timeoutFusionFlagCtx(ctx)

		f, err := os.Open(args[0])
		if err != nil {
			log.Fatalf("Can't open input: %v", err)
		}
		defer f.Close()

		req := new(pb.FuseVideoManipulationRequest)
		if err := jsonpb.Unmarshal(f, req); err != nil {
			log.Fatalf("Can't parse json proto: %v", err)
		}
		fusion := &pb.Fusion{Request: &pb.Fusion_VidManipReq{VidManipReq: req}}
		if err := client.Fuse(ctx, fusion); err != nil {
			log.Fatalf("Fuse failure: %v", err)
		}

		if err := outputProto(fusion.GetVidManip()); err != nil {
			log.Fatalf("Output failed: %v", err)
		}
	},
}

func init() {
	fuseCmd.AddCommand(fuseVidmanipCmd)
}
