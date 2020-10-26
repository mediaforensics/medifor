package analyticworker

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"entrogo.com/entroq"
	grpcbackend "entrogo.com/entroq/grpc"
	"entrogo.com/entroq/mem"
	"entrogo.com/entroq/qsvc/qtest"
	"github.com/golang/protobuf/jsonpb"
	"github.com/pkg/errors"
	"github.com/mediaforensics/medifor/pkg/analyticservice"
	"github.com/mediaforensics/medifor/pkg/medifor"
	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func waitForEmpty(ctx context.Context, client *entroq.EntroQ, qWork string) error {
	for {
		queues, err := client.Queues(ctx, entroq.MatchExact(qWork))
		if err != nil {
			return errors.Wrap(err, "empty check")
		}
		if queues[qWork] == 0 {
			return nil
		}
		log.Printf("Work queue still has %d tasks", queues[qWork])
		select {
		case <-ctx.Done():
			return errors.Wrap(ctx.Err(), "empty queue check")
		case <-time.After(5 * time.Second):
		}
	}
}

func analyticClientServer(ctx context.Context) (*medifor.Client, *analyticservice.AnalyticService, error) {
	lis := bufconn.Listen(1 << 20)
	svc, err := analyticservice.New(analyticservice.WithListener(lis))
	if err != nil {
		return nil, nil, errors.Wrap(err, "start analytic service")
	}

	client, err := medifor.NewClient(ctx, "bufnet", medifor.WithDial(
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

func entroQClientServer(ctx context.Context) (*entroq.EntroQ, *grpc.Server, error) {
	server, dial, err := qtest.StartService(ctx, mem.Opener())
	if err != nil {
		return nil, nil, errors.Wrap(err, "start entroq service")
	}

	client, err := entroq.New(ctx, grpcbackend.Opener("bufnet", grpcbackend.WithNiladicDialer(dial), grpcbackend.WithInsecure()))
	if err != nil {
		server.Stop()
		return nil, nil, errors.Wrap(err, "start entroq client")
	}
	return client, server, nil
}

func TestWorker(t *testing.T) {
	// Set up queues for the test.
	qNum := rand.Intn(10000)
	qWork := fmt.Sprintf("/test/ImgManipQueue/%d", qNum)
	qDone := fmt.Sprintf("/test/DoneQueue/%d", qNum)
	log.Printf("Beginning worker test")

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Create an EntroQ client and server in the test env.
	eqClient, eqServer, err := entroQClientServer(ctx)
	if err != nil {
		t.Fatalf("entroq communication setup: %v", err)
	}
	defer eqClient.Close()
	defer eqServer.Stop()

	// Assert that the work and done queues are empty when starting out.
	switch empty, err := eqClient.QueuesEmpty(ctx, entroq.MatchExact(qWork)); {
	case err != nil:
		t.Fatalf("Empty work queue: %v", err)
	case !empty:
		t.Fatalf("Expected empty work queue %q, but had tasks", qWork)
	}

	switch empty, err := eqClient.QueuesEmpty(ctx, entroq.MatchExact(qDone)); {
	case err != nil:
		t.Fatalf("Empty done queue: %v", err)
	case !empty:
		t.Fatalf("Expected empty done queue %q, but had tasks", qDone)
	}

	// Push a bunch of test tasks into the work queue.
	outDir, err := ioutil.TempDir("", "mfw-test-")
	if err != nil {
		t.Fatalf("outdir create: %v", err)
	}
	defer os.RemoveAll(outDir)

	log.Printf("Using %q as output root", outDir)

	work := []*pb.DetectionTask{
		{
			DoneQueue: qDone,
			Detection: &pb.Detection{
				Request: &pb.Detection_ImgManipReq{ImgManipReq: &pb.ImageManipulationRequest{
					Image: &pb.Resource{
						Uri:  "img1.jpg",
						Type: "image/jpeg",
					},
					OutDir: filepath.Join(outDir, "test-img1"),
				}},
			},
		},
		{
			DoneQueue: qDone,
			Detection: &pb.Detection{
				Request: &pb.Detection_ImgManipReq{ImgManipReq: &pb.ImageManipulationRequest{
					Image: &pb.Resource{
						Uri:  "img2.jpg",
						Type: "image/jpeg",
					},
					OutDir: filepath.Join(outDir, "test-img2"),
				}},
			},
		},
		{
			DoneQueue: qDone,
			Detection: &pb.Detection{
				Request: &pb.Detection_ImgManipReq{ImgManipReq: &pb.ImageManipulationRequest{
					Image: &pb.Resource{
						Uri:  "img3.jpg",
						Type: "image/jpeg",
					},
					OutDir: filepath.Join(outDir, "test-img3"),
				}},
			},
		},
		{
			DoneQueue: qDone,
			Detection: &pb.Detection{
				Request: &pb.Detection_ImgSpliceReq{ImgSpliceReq: &pb.ImageSpliceRequest{
					ProbeImage: &pb.Resource{
						Uri:  "splice_test.jpg",
						Type: "image/jpeg",
					},
					DonorImage: &pb.Resource{
						Uri:  "donor_test.jpg",
						Type: "image/jpeg",
					},
					OutDir: filepath.Join(outDir, "test-splice"),
				}},
			},
		},
		{
			DoneQueue: qDone,
			Detection: &pb.Detection{
				Request: &pb.Detection_VidManipReq{VidManipReq: &pb.VideoManipulationRequest{
					Video: &pb.Resource{
						Uri:  "video_test.mp4",
						Type: "video/mp4",
					},
					OutDir: filepath.Join(outDir, "test-video"),
				}},
			},
		},
	}

	var modArgs []entroq.ModifyArg
	for _, w := range work {
		data, err := new(jsonpb.Marshaler).MarshalToString(w)
		if err != nil {
			t.Fatalf("could not marshal work to JSON: %v", err)
		}
		modArgs = append(modArgs, entroq.InsertingInto(qWork, entroq.WithValue([]byte(data))))
	}
	log.Printf("Pushing tasks")
	if _, _, err := eqClient.Modify(ctx, modArgs...); err != nil {
		t.Fatalf("modify work queue %q: %v", qWork, err)
	}
	log.Printf("Tasks pushed")

	// Ensure that they got there.
	qs, err := eqClient.Queues(ctx, entroq.MatchExact(qWork))
	if err != nil {
		t.Fatalf("Error getting queues: %v", err)
	}
	if wSize, ok := qs[qWork]; !ok {
		t.Fatalf("Non-existent work queue %q", qWork)
	} else if len(work) != wSize {
		t.Fatalf("Want %d tasks in queue %q, got %d", len(work), qWork, wSize)
	}
	log.Printf("Tasks arrived to work queue")

	// Now we create an analytic client and server that we can hook our worker to.
	// Create the service, register functions, get it started, and stop when finished.
	log.Printf("Starting analytic service and creating client")
	aClient, aSvc, err := analyticClientServer(ctx)
	if err != nil {
		t.Fatalf("Can't create analytic server and client: %v", err)
	}
	defer aClient.Close()
	defer aSvc.Stop()

	log.Printf("Registering functions")
	if err := aSvc.RegisterImageManipulation(func(ctx context.Context, req *pb.ImageManipulationRequest) (*pb.ImageManipulation, error) {
		return &pb.ImageManipulation{
			Score: 0.616,
		}, nil
	}); err != nil {
		t.Fatalf("Failed to register imgmanip: %v", err)
	}

	if err := aSvc.RegisterImageSplice(func(ctx context.Context, req *pb.ImageSpliceRequest) (*pb.ImageSplice, error) {
		return &pb.ImageSplice{
			Link: &pb.Link{
				Score:   0.740,
				FromUri: req.DonorImage.GetUri(),
				ToUri:   req.ProbeImage.GetUri(),
			},
		}, nil
	}); err != nil {
		t.Fatalf("Failed to register imgsplice: %v", err)
	}

	if err := aSvc.RegisterVideoManipulation(func(ctx context.Context, req *pb.VideoManipulationRequest) (*pb.VideoManipulation, error) {
		return &pb.VideoManipulation{
			Score: 0.464,
		}, nil
	}); err != nil {
		t.Fatalf("Failed to register vidmanip: %v", err)
	}

	go aSvc.Run(ctx)

	g, gCtx := errgroup.WithContext(ctx)

	g.Go(func() error {
		if err := waitForEmpty(gCtx, eqClient, qWork); err != nil {
			return err
		}
		return context.Canceled // cause others in group to die, too, but cleanly.
	})

	// Service is now running, and we have our client. Create a worker and get it started.
	worker := New(aClient, eqClient, []string{qWork})

	g.Go(func() error {
		return worker.Run(gCtx)
	})

	if err := g.Wait(); errors.Cause(err) != context.Canceled {
		t.Fatalf("Error in worker: %v", err)
	}

	// Assert that the right things made it into the done queue.
	doneQueues, err := eqClient.Queues(ctx, entroq.MatchExact(qDone))
	if err != nil {
		t.Fatalf("Error checking for empty done queue: %v", err)
	}
	if dSize, ok := doneQueues[qDone]; !ok {
		t.Fatal("Nothing in done queue when expected")
	} else if dSize != len(work) {
		t.Fatalf("Want %d tasks in done queue, got %d", len(work), dSize)
	}

	// TODO: check actual contents.
}

func TestWorker_MultiQueue(t *testing.T) {
	// Set up queues for the test.
	qNum := rand.Intn(10000)
	qWorkLong := fmt.Sprintf("/test/%d/long", qNum)
	qWorkShort := fmt.Sprintf("/test/%d/short", qNum)
	qDone := fmt.Sprintf("/test/%d/done", qNum)

	outDir, err := ioutil.TempDir("", "mfw-test-")
	if err != nil {
		t.Fatalf("outdir create: %v", err)
	}
	defer os.RemoveAll(outDir)

	data, err := new(jsonpb.Marshaler).MarshalToString(&pb.DetectionTask{
		DoneQueue: qDone,
		Detection: &pb.Detection{
			Request: &pb.Detection_ImgManipReq{ImgManipReq: &pb.ImageManipulationRequest{
				Image: &pb.Resource{
					Uri:  "img1.jpg",
					Type: "image/jpeg",
				},
				OutDir: filepath.Join(outDir, "test-img1"),
			}},
		},
	})
	if err != nil {
		t.Fatalf("Can't marshal task data: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	// Create an EntroQ client and server in the test env.
	eqc, server, err := entroQClientServer(ctx)
	if err != nil {
		t.Fatalf("entroq communication setup: %v", err)
	}
	defer server.Stop()
	defer eqc.Close()

	aClient, aSvc, err := analyticClientServer(ctx)
	if err != nil {
		t.Fatalf("Can't create analytic server and client: %v", err)
	}
	defer aClient.Close()

	const sleepTime = 2 * time.Second

	if err := aSvc.RegisterImageManipulation(func(ctx context.Context, req *pb.ImageManipulationRequest) (*pb.ImageManipulation, error) {
		time.Sleep(sleepTime)
		return &pb.ImageManipulation{Score: .555}, nil
	}); err != nil {
		t.Fatalf("Failed to register: %v", err)
	}
	go aSvc.Run(ctx)
	defer aSvc.Stop()

	// The test goes like this:
	// - Insert a bunch of tasks into a "long" queue.
	// - Start a worker that claims, sleeps, and moves to done, using both the long and short (currently empty) queue.
	// - Immediately insert a task into the short queue.
	// - Verify that the short task is done before at least one of the long tasks (using modification time).

	if _, _, err := eqc.Modify(ctx,
		entroq.InsertingInto(qWorkLong, entroq.WithValue([]byte(data))),
		entroq.InsertingInto(qWorkLong, entroq.WithValue([]byte(data))),
		entroq.InsertingInto(qWorkLong, entroq.WithValue([]byte(data))),
		entroq.InsertingInto(qWorkLong, entroq.WithValue([]byte(data))),
		entroq.InsertingInto(qWorkLong, entroq.WithValue([]byte(data))),
		entroq.InsertingInto(qWorkLong, entroq.WithValue([]byte(data))),
		entroq.InsertingInto(qWorkLong, entroq.WithValue([]byte(data))),
		entroq.InsertingInto(qWorkLong, entroq.WithValue([]byte(data))),
		entroq.InsertingInto(qWorkLong, entroq.WithValue([]byte(data))),
		entroq.InsertingInto(qWorkLong, entroq.WithValue([]byte(data))),
		entroq.InsertingInto(qWorkLong, entroq.WithValue([]byte(data))),
		entroq.InsertingInto(qWorkLong, entroq.WithValue([]byte(data))),
		entroq.InsertingInto(qWorkLong, entroq.WithValue([]byte(data))),
		entroq.InsertingInto(qWorkLong, entroq.WithValue([]byte(data)))); err != nil {
		t.Fatalf("add tasks to long queue")
	}

	worker := New(aClient, eqc, []string{qWorkLong, qWorkShort})
	go worker.Run(ctx)

	// Wait for some to make progress, before jumping in with a short queue task.
	time.Sleep(2 * sleepTime)

	if _, _, err := eqc.Modify(ctx, entroq.InsertingInto(qWorkShort, entroq.WithValue([]byte(data)))); err != nil {
		t.Fatalf("Failed to insert short queue task: %v", err)
	}

	for {
		queues, err := eqc.Queues(ctx, entroq.MatchExact(qWorkLong), entroq.MatchExact(qWorkShort))
		if err != nil {
			t.Fatalf("Empty check: %v", err)
		}
		if queues[qWorkLong] != 0 && queues[qWorkShort] == 0 {
			// All done - everything worked.
			return
		}
		log.Printf("Work queue long=%d short=%d", queues[qWorkLong], queues[qWorkShort])
		if queues[qWorkLong] == 0 {
			t.Fatal("Long queue emptied out before short queue")
		}
		select {
		case <-ctx.Done():
			t.Fatalf("Empty queue check: %v", ctx.Err())
		case <-time.After(sleepTime):
		}
	}
	t.Fatal("Expected short queue to empty before long queue")
}
