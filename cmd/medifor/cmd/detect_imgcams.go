package cmd

import (
	"context"
	"log"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
	"gitlab.mediforprogram.com/medifor/medifor-proto/pkg/fileutil"

	pb "gitlab.mediforprogram.com/medifor/medifor-proto/pkg/mediforproto"
)

// detectImgcamerasCmd represents the imgcameras command
var detectImgcamerasCmd = &cobra.Command{
	Use:   "imgcameras",
	Short: "Send an image cameras detection request",
	Long:  `medifor detect imgcameras path/to/probe/image`,
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
		req := &pb.ImageCamerasRequest{
			RequestId: uuid.New().String(),
			OutDir:    outDir,
			Image: &pb.Resource{
				Uri:  args[0],
				Type: types[0],
			},
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
	detectCmd.AddCommand(detectImgcamerasCmd)
}
