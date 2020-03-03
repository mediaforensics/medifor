package cmd

import (
	"context"
	"log"

	"github.com/google/uuid"
	"github.com/mediaforensics/medifor/pkg/fileutil"
	"github.com/spf13/cobra"

	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
)

var (
	hpdid string
)

// detectImgcameramatchCmd represents the imgcameramatch command
var detectImgcameramatchCmd = &cobra.Command{
	Use:   "imgcameramatch",
	Short: "Send an image camera match detection request",
	Long:  `medifor detect imgcameramatch --hpdid=foo path/to/probe/image`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			log.Fatalf("One image argument required")
		}
		types, err := fileutil.FileTypes(args)
		if err != nil {
			log.Fatalf("Error getting file types: %v", err)
		}

		ctx := context.Background()

		client := mustClient(ctx)
		defer client.Close()

		ctx = timeoutFlagCtx(ctx)
		req := &pb.ImageCameraMatchRequest{
			RequestId: uuid.New().String(),
			OutDir:    outDir,
			Image: &pb.Resource{
				Uri:  args[0],
				Type: types[0],
			},
			CameraId: hpdid,
		}

		detection, err := client.DetectReq(ctx, req)
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
	detectCmd.AddCommand(detectImgcameramatchCmd)

	detectImgcameramatchCmd.PersistentFlags().StringVar(&hpdid, "hpdid", "", "High provenance device ID (camera ID) to check")
}
