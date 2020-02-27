package cmd

import (
	"context"
	"log"
	"os"

	"github.com/golang/protobuf/jsonpb"
	"github.com/spf13/cobra"
	pb "gitlab.mediforprogram.com/medifor/medifor-proto/pkg/mediforproto"
)

// fuseImgmanipCmd represents the imgmanip command when run for fusion.
var fuseImgmanipCmd = &cobra.Command{
	Use:   "imgmanip",
	Short: "Send a fusion request for image manipulation results",
	Long: `medifor fuse imgmanip /path/to/request.json

	Where requests.json is a JSON representation of a FuseImageManipulationRequest proto.
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

		req := new(pb.FuseImageManipulationRequest)
		if err := jsonpb.Unmarshal(f, req); err != nil {
			log.Fatalf("Can't parse json proto: %v", err)
		}
		fusion := &pb.Fusion{Request: &pb.Fusion_ImgManipReq{ImgManipReq: req}}
		if err := client.Fuse(ctx, fusion); err != nil {
			log.Fatalf("Fuse failure: %v", err)
		}

		if err := outputProto(fusion.GetImgManip()); err != nil {
			log.Fatalf("Output failed: %v", err)
		}
	},
}

func init() {
	fuseCmd.AddCommand(fuseImgmanipCmd)
}
