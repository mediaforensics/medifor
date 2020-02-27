package cmd

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"os"

	"github.com/golang/protobuf/jsonpb"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	pb "gitlab.mediforprogram.com/medifor/medifor-proto/pkg/mediforproto"
)

func readAnnotatedDetections(r io.Reader) ([]*pb.AnnotatedDetection, error) {
	dec := json.NewDecoder(r)
	var detections []*pb.AnnotatedDetection
	for {
		d := new(pb.AnnotatedDetection)
		if err := jsonpb.UnmarshalNext(dec, d); err != nil {
			if err == io.EOF {
				break
			}
			return nil, errors.Wrap(err, "unmarshal detection json")
		}
		detections = append(detections, d)
	}
	return detections, nil
}

// fuseDetectionsCmd represents the imgmanip command when run for fusion.
var fuseDetectionsCmd = &cobra.Command{
	Use:   "detections",
	Short: "Send a fusion request for a list of detections",
	Long: `medifor fuse detections /path/to/detections.json

	Where detections.json is a JSON representation of AnnotatedDetection JSON protos.
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

		ds, err := readAnnotatedDetections(f)
		if err != nil {
			log.Fatalf("Failed to read annotated detections: %v", err)
		}
		fusion, err := client.FuseDetections(ctx, ds)
		if err != nil {
			log.Fatalf("Fuse failure from detections: %v", err)
		}

		if err := outputProto(fusion); err != nil {
			log.Fatalf("Output failed: %v", err)
		}
	},
}

func init() {
	fuseCmd.AddCommand(fuseDetectionsCmd)
}
