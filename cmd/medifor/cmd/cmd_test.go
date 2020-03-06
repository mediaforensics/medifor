// cmd_test is a suite of tests designed to make sure the CLI tool provided to
// performers works appropriately.
package cmd

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/mediaforensics/medifor/pkg/analyticservice"
	"github.com/mediaforensics/medifor/pkg/medifor"
	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const (
	dataDir     = "../../.."
	manipIndex  = dataDir + "/assets/manipulations.csv"
	manipIndexP = dataDir + "/assets/manipulationsPipes.csv"
	// manipIndex = "/Users/nicholasburnett/Workspace/src/github.com/mediaforensics/medifor/assets/manipulations.csv"
	bufsize = 1 << 20
)

func analyticClientServer(ctx context.Context) (*medifor.MultiClient, *analyticservice.AnalyticService, error) {
	lis := bufconn.Listen(1 << 20)
	svc, err := analyticservice.New(analyticservice.WithListener(lis))
	if err != nil {
		return nil, nil, errors.Wrap(err, "creating analytic service")
	}
	client, err := medifor.NewMultiClient(ctx, []string{"bufnet"}, medifor.WithDial(
		grpc.WithDialer(func(string, time.Duration) (net.Conn, error) {
			return lis.Dial()
		}),
		grpc.WithInsecure()))
	if err != nil {
		svc.Stop()
		return nil, nil, errors.Wrap(err, "connect medifor client")
	}
	return client, svc, nil
}

func TestBatchDetect(t *testing.T) {
	ctx := context.Background()

	client, svc, err := analyticClientServer(ctx)
	if err != nil {
		t.Fatalf("creating test analytic: %v", err)
	}
	defer svc.Stop()
	defer client.Close()

	if err := svc.RegisterImageManipulation(func(ctx context.Context, req *pb.ImageManipulationRequest) (*pb.ImageManipulation, error) {
		return &pb.ImageManipulation{
			Score: 0.616,
		}, nil
	}); err != nil {
		t.Fatalf("Failed to register imgmanip: %v", err)
	}

	if err := svc.RegisterImageSplice(func(ctx context.Context, req *pb.ImageSpliceRequest) (*pb.ImageSplice, error) {
		return &pb.ImageSplice{
			Link: &pb.Link{
				Score:   0.740,
				FromUri: req.DonorImage.GetUri(),
				ToUri:   req.ProbeImage.GetUri(),
			},
		}, nil
	}); err != nil {
		t.Fatalf("Failed to register imgmanip: %v", err)
	}

	if err := svc.RegisterVideoManipulation(func(ctx context.Context, req *pb.VideoManipulationRequest) (*pb.VideoManipulation, error) {
		return &pb.VideoManipulation{
			Score: 0.465,
		}, nil
	}); err != nil {
		t.Fatalf("Failed to register imgmanip: %v", err)
	}

	go func() {
		if err := svc.Run(ctx); err != nil {
			t.Fatalf("Error running service: %v", err)
		}
	}()

	for _, inputName := range []string{
		manipIndex,
		manipIndexP,
	} {
		r, err := os.Open(inputName)
		if err != nil {
			t.Errorf("Error opening input file %q: %v", inputName, err)
			continue
		}
		if err := runBatchDetect(ctx, client, r, os.Stdout); err != nil {
			t.Errorf("Error running batch detection on %q: %v", inputName, err)
		}
	}
	// TODO: add test for non-error processing (make sure the registered
	// functions are called and don't return errors by parsing output).
}
