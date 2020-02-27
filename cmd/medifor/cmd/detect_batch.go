package cmd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/jfyne/csvd"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"gitlab.mediforprogram.com/medifor/medifor-proto/cmd/medifor/files"
	"gitlab.mediforprogram.com/medifor/medifor-proto/pkg/medifor"
	pb "gitlab.mediforprogram.com/medifor/medifor-proto/pkg/mediforproto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/status"
)

const (
	taskHeader       = "TaskID"
	taskManipulation = "Manipulation"
	taskSplice       = "Splice"
)

// jsonProto converts a proto message into JSON bytes terminated by a newline.
func jsonProto(msg proto.Message) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := new(jsonpb.Marshaler).Marshal(buf, msg); err != nil {
		return nil, errors.Wrap(err, "jsonpb marshal")
	}
	if err := buf.WriteByte('\n'); err != nil {
		return nil, errors.Wrap(err, "write newline to json output")
	}
	return buf.Bytes(), nil
}

// roundTrip gets a request value, figures out which endpoint it should go to
// based on the request's type, sends it on the next available client, then
// packages the request and response appropriately into a round-trip Detection
// protobuf and returns it.
func roundTrip(ctx context.Context, client *medifor.MultiClient, req medifor.Request) *pb.Detection {
	var detection *pb.Detection
	if err := client.CallWithClient(ctx, func(ctx context.Context, client *medifor.Client) error {
		var err error
		if detection, err = client.DetectReq(ctx, req); err != nil {
			log.Printf("Detection error received: %v", err)
			if detection == nil {
				return errors.Wrap(err, "client detection call")
			}
			// A non-nil detection means the error was put into status. We can still use it.
			log.Printf("Detection error in status, allowing to continue")
		}
		return nil
	}); err != nil {
		log.Printf("Error doing detection: %v", err)
		if detection == nil {
			var err error
			detection, err = medifor.NewDetectionReq(req)
			if err != nil {
				log.Fatalf("Failed to create detection from request: %v", err)
			}
			detection.Status = status.Convert(errors.Cause(err)).Proto()
		}
		return detection
	}

	// Now check that the score is in the right range, log if not.
	if err := medifor.CheckDetectionScore(detection); err != nil {
		log.Printf("Detection validation error for below request: %v:\n\t%s", err, detection.GetRequest())
	}
	return detection
}

func mustAbs(path, relRoot string) string {
	if filepath.IsAbs(path) {
		return path
	}
	abs, err := filepath.Abs(filepath.Join(relRoot, path))
	if err != nil {
		log.Fatalf("Absolute path creation failed: %v", err)
	}
	return abs
}

// nistRowToRequest takes an input CSV row from the NIST index format, figures
// out what kind of task it represents, and creates an appropriately typed
// request.
func nistRowToRequest(row map[string]string, dataDir string) (medifor.Request, error) {
	recType, err := files.NISTRowType(row)
	if err != nil {
		return nil, err
	}

	switch recType {
	case files.ImageManipulation:
		manip := files.NISTRowToManipulation(row)
		return medifor.NewImageManipulationRequest(mustAbs(manip.ProbeFileName, dataDir),
			medifor.WithOutputPrefix(outDir),
			medifor.WithDeviceID(manip.DeviceID())), nil
	case files.VideoManipulation:
		manip := files.NISTRowToManipulation(row)
		return medifor.NewVideoManipulationRequest(mustAbs(manip.ProbeFileName, dataDir),
			medifor.WithOutputPrefix(outDir),
			medifor.WithDeviceID(manip.DeviceID())), nil
	case files.ImageSplice:
		splice := files.NISTRowToSplice(row)
		return medifor.NewImageSpliceRequest(mustAbs(splice.DonorFileName, dataDir), mustAbs(splice.ProbeFileName, dataDir),
			medifor.WithOutputPrefix(outDir)), nil
	case files.ImageCameraMatch:
		match := files.NISTRowToCameraVerification(row)
		return medifor.NewImageCameraMatchRequest(mustAbs(match.ProbeFileName, dataDir),
			medifor.WithOutputPrefix(outDir),
			medifor.WithDeviceID(match.TrainCamID)), nil
	// TODO: add image camera search when NIST defines that task name.
	default:
		return nil, errors.Errorf("unrecognized task type %q", recType)
	}
}

// callForNISTRow parses a CSV row, then attempts to make a call to the given multi-client, returning JSON bytes.
func callForNISTRow(ctx context.Context, client *medifor.MultiClient, row map[string]string, dataDir string) (out []byte, err error) {
	req, err := nistRowToRequest(row, dataDir)
	if err != nil {
		return nil, err
	}
	detection := roundTrip(ctx, client, req)
	b, err := jsonProto(detection)
	if err != nil {
		return nil, errors.Wrap(err, "jsonproto")
	}
	return b, nil
}

func runBatchDetect(ctx context.Context, client *medifor.MultiClient, r io.Reader, w io.Writer, dataDir string) error {
	if dataDir == "" {
		dataDir = "."
	}

	reader := csvd.NewReader(r)
	rows, err := reader.ReadAll()
	if err != nil {
		return errors.Wrap(err, "failed to read CSV records")
	}

	work := make(chan map[string]string)
	results := make(chan []byte)

	g, ctx := errgroup.WithContext(ctx)

	header := rows[0]
	rows = rows[1:]

	rowMap := func(row []string) map[string]string {
		m := make(map[string]string)
		for i, c := range row {
			m[header[i]] = c
		}
		return m
	}

	// Produce rows for workers to pick up.
	g.Go(func() error {
		defer close(work)
		for _, row := range rows {
			select {
			case work <- rowMap(row):
			case <-ctx.Done():
				return errors.Wrap(ctx.Err(), "batch detect worker canceled")
			}
		}
		return nil
	})

	// Create workers, one per client in the multi-client.
	// Produce results on the results channel as they come in.
	for i := 0; i < client.Len(); i++ {
		i := i
		g.Go(func() error {
			log.Printf("Worker %d starting", i)
			defer log.Printf("Worker %d stopping", i)
			for row := range work {
				ctx := rowTimeoutCtx(ctx, files.IsNISTVideoManipulationRow(row))
				output, err := callForNISTRow(ctx, client, row, dataDir)
				if err != nil {
					if isTimeout(err) {
						log.Printf("nist row timed out: %v", err)
					} else {
						return errors.Wrap(err, "nist row failed")
					}
				}
				select {
				case results <- output:
				case <-ctx.Done():
					return errors.Wrap(ctx.Err(), "client worker canceled")
				}
			}
			return nil
		})
	}

	// When all processors are finished producing results, close the channel.
	// This terminates the synchronous loop below.
	go func() {
		g.Wait()
		close(results)
	}()

	for val := range results {
		fmt.Fprint(w, string(val))
	}

	if err := g.Wait(); err != nil {
		return errors.Wrap(err, "error processing file")
	}
	return nil
}

// Flags.
const (
	DataDirFlag      = "data_dir"
	VideoTimeoutFlag = "video_timeout"
)

// Flags.
var vidTimeout int

func rowTimeoutCtx(ctx context.Context, isVideo bool) context.Context {
	if isVideo {
		return timeoutSecCtx(ctx, vidTimeout)
	}
	return timeoutFlagCtx(ctx)
}

// detectBatchCmd represents the batch command
var detectBatchCmd = &cobra.Command{
	Use:   "batch",
	Short: "Run a batch detection against (potentially) multiple hosts.",
	Long: `Run a batch detection process against a NIST index pipe-delimited-values file. Provide two arguments:
	<csv input file name> <output json file name>

	If no output file is specified, the request/response log will be written to stdout.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) < 1 {
			log.Fatalf("One CSV file required as a single argument.")
		}
		inputName := args[0]

		var outFile io.Writer = os.Stdout
		if len(args) >= 2 {
			outName := args[1]
			log.Printf("Writing output to %q", outName)

			f, err := os.Create(outName)
			if err != nil {
				log.Fatalf("Could not create output file: %v", err)
			}
			defer f.Close()
			outFile = f
		} else {
			log.Printf("No output file specified: writing log to stdout.")
		}

		inFile, err := os.Open(inputName)
		if err != nil {
			log.Fatalf("Cannot open file %q: %v", inputName, err)
		}
		defer inFile.Close()

		dataDir, err := cmd.PersistentFlags().GetString(DataDirFlag)
		if err != nil {
			log.Fatalf("Could not get value for flag %q: %v", DataDirFlag, err)
		}

		ctx := context.Background()

		client := mustMultiClient(ctx)
		defer client.Close()

		if err := runBatchDetect(ctx, client, inFile, outFile, dataDir); err != nil {
			log.Fatalf("Could not run batch detection: %v", err)
		}
	},
}

func init() {
	detectCmd.AddCommand(detectBatchCmd)

	detectBatchCmd.PersistentFlags().String(DataDirFlag, ".", "Parent directory for relative CSV data paths.")
	detectBatchCmd.PersistentFlags().IntVar(&vidTimeout, VideoTimeoutFlag, 15*60, "Separate timeout for video processing.")
}
