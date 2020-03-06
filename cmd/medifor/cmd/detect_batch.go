package cmd

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"mime"
	"os"
	"path/filepath"
	"strings"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/mediaforensics/medifor/pkg/medifor"
	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
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

func runBatchDetect(ctx context.Context, client *medifor.MultiClient, r io.Reader, w io.Writer) error {
	scanner := bufio.NewScanner(r)

	// Load one-per-line JSON protos from input.
	var detections []*pb.Detection
	for scanner.Scan() {
		det := new(pb.Detection)
		if err := jsonpb.UnmarshalString(scanner.Text(), det); err != nil {
			return errors.Wrapf(err, "can't unmarshal text from JSON to detection proto: %v", scanner.Text())
		}
		detections = append(detections, det)
	}

	work := make(chan *pb.Detection)
	results := make(chan *pb.Detection)

	g, ctx := errgroup.WithContext(ctx)

	// Produce detections for workers to pick up.
	g.Go(func() error {
		defer close(work)
		for _, det := range detections {
			select {
			case work <- det:
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
			for det := range work {
				ctx := timeoutFlagCtx(ctx)
				if err := client.CallWithClient(ctx, func(ctx context.Context, cli *medifor.Client) error {
					if err := cli.Detect(ctx, det); err != nil {
						return errors.Wrap(err, "call detect")
					}
					return nil
				}); err != nil {
					if isTimeout(err) {
						log.Printf("Detection timed out: %v", err)
					} else {
						return errors.Wrap(err, "multiclient detection")
					}
				}

				select {
				case results <- det:
				case <-ctx.Done():
					return errors.Wrapf(ctx.Err(), "client worker %d canceled", i)
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

	for det := range results {
		if err := medifor.CheckDetectionScore(det); err != nil {
			log.Printf("Detection validation error for below request: %v:\n\t%s", err, det.GetRequest())
		}

		js, err := jsonProto(det)
		if err != nil {
			log.Printf("Error making detection into proto (detection below): %v\n%+v", err, det)
			continue
		}
		fmt.Fprint(w, string(js))
	}

	if err := g.Wait(); err != nil {
		return errors.Wrap(err, "error processing file")
	}
	return nil
}

func runBatchDetectNames(ctx context.Context, client *medifor.MultiClient, inputName string, outputName string) error {
	var reader io.Reader = os.Stdin
	if inputName != "" {
		inputInfo, err := os.Stat(inputName)
		if err != nil {
			return errors.Wrap(err, "runBatchDetectNames stat input")
		}
		if inputInfo.IsDir() {
			detections, err := detectionsFromDir(inputName)
			if err != nil {
				return errors.Wrap(err, "runBatchDetectNames from dir")
			}
			buf := new(bytes.Buffer)
			for _, det := range detections {
				if err := new(jsonpb.Marshaler).Marshal(buf, det); err != nil {
					return errors.Wrap(err, "runBatchDetectNames marshal")
				}
				fmt.Fprintln(buf)
			}
			reader = buf
		} else {
			inFile, err := os.Open(inputName)
			if err != nil {
				return errors.Wrap(err, "runBatchDetectNames open input")
			}
			defer inFile.Close()
			reader = inFile
		}
	}

	var outFile io.Writer = os.Stdout
	if outputName != "" {
		log.Printf("Writing output to %q", outputName)

		f, err := os.Create(outputName)
		if err != nil {
			return errors.Wrap(err, "runBatchDetectNames output file")
		}
		defer f.Close()
		outFile = f
	} else {
		log.Printf("No output file specified: writing log to stdout.")
	}

	return errors.Wrap(runBatchDetect(ctx, client, reader, outFile), "runBatchDetectNames")
}

func detectionsFromDir(dirName string) ([]*pb.Detection, error) {
	dir, err := os.Open(dirName)
	if err != nil {
		return nil, errors.Wrap(err, "detectionsFromDir")
	}

	names, err := dir.Readdirnames(0)
	if err != nil {
		return nil, errors.Wrap(err, "detectionsFromDir")
	}

	var detections []*pb.Detection
	for _, name := range names {
		fullPath := filepath.Join(dirName, name)
		ext := strings.ToLower(filepath.Ext(name))
		mtype := mime.TypeByExtension(ext)
		switch {
		case strings.HasPrefix(mtype, "image/"):
			detections = append(detections, medifor.MustDetectionReq(
				medifor.NewImageManipulationRequest(fullPath, medifor.WithOutputPrefix(outDir))),
			)
		case strings.HasPrefix(mtype, "video/"):
			detections = append(detections, medifor.MustDetectionReq(
				medifor.NewVideoManipulationRequest(fullPath, medifor.WithOutputPrefix(outDir))),
			)
		default:
			log.Printf("Ignoring unknown extension for file %q", name)
		}
	}
	return detections, nil
}

// detectBatchCmd represents the batch command
var detectBatchCmd = &cobra.Command{
	Use:   "batch",
	Short: "Run a batch detection against (potentially) multiple hosts.",
	Long: `Run a batch detection process against a directory or a file containing JSON detection protos, one per line.
	Produces a similar output file full of JSON detection objects, one per line.

	Arguments: [inputName [outputName]]

	If no input file or directory is given, detection proto JSON will be read from stdin.
	If no output file is specified, the request/response log will be written to stdout.`,
	Run: func(cmd *cobra.Command, args []string) {
		inputName := ""
		outputName := ""

		if len(args) >= 1 {
			inputName = args[0]
		}
		if len(args) >= 2 {
			outputName = args[1]
		}
		if len(args) > 2 {
			log.Fatal("Too many arguments: expect [inputName [outputName]]")
		}

		ctx := context.Background()
		client := mustMultiClient(ctx)
		defer client.Close()

		if err := runBatchDetectNames(ctx, client, inputName, outputName); err != nil {
			log.Fatalf("Could not run batch detection: %v", err)
		}
	},
}

func init() {
	detectCmd.AddCommand(detectBatchCmd)
}
