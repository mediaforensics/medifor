package fusionworker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"entrogo.com/entroq"
	"github.com/golang/protobuf/jsonpb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/mediaforensics/medifor/pkg/analytic"
	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
	"github.com/mediaforensics/medifor/pkg/medifor"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Metrics.
var (
	numClaim = promauto.NewCounter(prometheus.CounterOpts{
		Name: "medifor_fuser_claim_total",
	})
	numFailure = promauto.NewCounter(prometheus.CounterOpts{
		Name: "medifor_fuser_success_total",
	})
	numSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Name: "medifor_fuser_failure_total",
	})
)

// FusionWorker provides means for running a worker against a task queue. The
// tasks it consumes are DetectionTask protos.
type FusionWorker struct {
	worker *entroq.Worker

	mfc *medifor.FusionClient

	maxAttempts    int
	memoizeResults bool
}

// Option is fuser worker option used when calling New.
type Option func(*FusionWorker)

// WithMaxAttempts sets the maximum attempts to the given value. Values less than 1 mean "unlimited".
func WithMaxAttempts(max int) Option {
	if max < 0 {
		max = 0
	}
	return func(w *FusionWorker) {
		w.maxAttempts = max
	}
}

// WithMemoization sets things up to memoize results to files as well as just pushing thing into the outbox.
func WithMemoization(on bool) Option {
	return func(w *FusionWorker) {
		w.memoizeResults = on
	}
}

// New creates a new FusionWorker. To operate, the worker needs access to a
// task client, the medifor client, and the name of a queue it should read
// tasks from.
func New(mfClient *medifor.FusionClient, eqClient *entroq.EntroQ, workQueues []string, opts ...Option) *FusionWorker {
	w := &FusionWorker{
		worker: eqClient.NewWorker(workQueues...),
		mfc:    mfClient,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func moveToDone(task *entroq.Task, work *pb.FusionTask) ([]entroq.ModifyArg, error) {
	value, err := new(jsonpb.Marshaler).MarshalToString(work)
	if err != nil {
		return nil, errors.Wrap(err, "work marshal")
	}
	return []entroq.ModifyArg{task.AsChange(
		entroq.QueueTo(work.DoneQueue),
		entroq.ArrivalTimeBy(0),
		entroq.ValueTo([]byte(value)),
	)}, nil
}

// memoizeDetectionInfo gets the information from a detection and its tags, and writes it to a file in out_dir.
func memoizeFusionInfo(fus *pb.Fusion, tags map[string]string) error {
	parent, err := analytic.FusionOutDir(fus)

	if err := os.MkdirAll(parent, os.ModeDir|0770); err != nil {
		return errors.Wrapf(err, "memoize mkdir %q", parent)
	}

	if err != nil {
		return errors.Wrap(err, "memoize get dir")
	}
	if parent == "" {
		return errors.New("memoize empty dir")
	}

	jFus, err := new(jsonpb.Marshaler).MarshalToString(fus)
	if err != nil {
		return errors.Wrap(err, "memoize detection to json")
	}

	jTagB, err := json.Marshal(tags)
	if err != nil {
		return errors.Wrap(err, "memoize tags to json")
	}
	jTag := string(jTagB)

	tsSuffix := time.Now().UTC().Format("20060102-150405")
	fusName := fmt.Sprintf("%s_det.json", tsSuffix)
	tagName := fmt.Sprintf("%s_tag.json", tsSuffix)

	fusFile, err := os.Create(filepath.Join(parent, fusName))
	if err != nil {
		return errors.Wrap(err, "memoize open file")
	}
	defer fusFile.Close()
	if _, err := fmt.Fprintln(fusFile, jFus); err != nil {
		return errors.Wrap(err, "memoize write json")
	}

	tagFile, err := os.Create(filepath.Join(parent, tagName))
	if err != nil {
		return errors.Wrap(err, "memoize open file")
	}
	defer tagFile.Close()
	if _, err := fmt.Fprintln(tagFile, jTag); err != nil {
		return errors.Wrap(err, "memoize write json")
	}
	return nil
}

// Run starts the FusionWorker. It claims tasks from its queues.
func (w *FusionWorker) Run(ctx context.Context) error {
	var crashErr error
	return w.worker.Run(ctx, func(ctx context.Context, task *entroq.Task) (args []entroq.ModifyArg, err error) {
		numClaim.Inc()
		defer func() {
			if err != nil {
				numFailure.Inc()
			} else {
				numSuccess.Inc()
			}
		}()

		if crashErr != nil {
			return nil, crashErr
		}
		work := new(pb.FusionTask)
		if err := jsonpb.UnmarshalString(string(task.Value), work); err != nil {
			return nil, errors.Wrap(err, "unmarshal work")
		}

		if w.maxAttempts > 0 && int(task.Claims) > w.maxAttempts {
			work.Fusion.Status = status.Newf(codes.Internal, "reached attempt %d / %d, giving up", task.Claims, w.maxAttempts).Proto()
			return moveToDone(task, work)
		}

		analyticIDs, err := analytic.AnalyticIDsFromFusion(work.Fusion)
		if err != nil {
			errors.Wrap(err, "Get analyticIDs for log in fusionworker")
		}
		log.Printf("FUSION WORKER (Pre Fusion):: detectionID: %v -- fuserID %v -- analyticIDs: %v", work.DetectionId, work.FuserId, analyticIDs)

		if ts := work.AnalyticTimeoutSec; ts != 0 {
			var cancel func()
			ctx, cancel = context.WithTimeout(ctx, time.Duration(ts)*time.Second)
			defer cancel()
		}

		if err := w.mfc.Fuse(ctx, work.Fusion); err != nil {
			log.Printf("FUSION WORKER (Post Fusion) :: detectionID: %v -- fuserID %v -- analyticIDs: %v", work.DetectionId, work.FuserId, analyticIDs)
			switch status.Code(errors.Cause(err)) {
			case codes.OK:
				// Success - proceed to place in outbox.
			case codes.Unknown,
				codes.InvalidArgument,
				codes.FailedPrecondition,
				codes.Unimplemented,
				codes.Internal:
				// Permanent failure - proceed to outbox with error (in detection already).
				return moveToDone(task, work)
			case codes.Unavailable,
				codes.DeadlineExceeded,
				codes.Aborted:
				crashErr = err // cause a crash next time around - we can't proceed safely.
				return moveToDone(task, work)
			default:
				// Unknown error, presume fatal.
				return nil, errors.Wrap(err, "fusion error")
			}
		}
		log.Printf("FUSION WORKER (Post Fusion) :: detectionID: %v -- fuserID %v -- analyticIDs: %v", work.DetectionId, work.FuserId, analyticIDs)
		if w.memoizeResults {
			if err := memoizeFusionInfo(work.GetFusion(), work.GetTags()); err != nil {
				return nil, errors.Wrap(err, "worker memoize")
			}
		}

		// No errors, all done.
		return moveToDone(task, work)
	})
}
