package analyticworker

import (
	"context"
	"log"
	"time"

	"entrogo.com/entroq"
	"github.com/golang/protobuf/jsonpb"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
	"github.com/mediaforensics/medifor/pkg/medifor"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Metrics.
var (
	numClaim = promauto.NewCounter(prometheus.CounterOpts{
		Name: "medifor_analytic_claim_total",
	})
	numFailure = promauto.NewCounter(prometheus.CounterOpts{
		Name: "medifor_analytic_success_total",
	})
	numSuccess = promauto.NewCounter(prometheus.CounterOpts{
		Name: "medifor_analytic_failure_total",
	})
)

// AnalyticWorker provides means for running a worker against a task queue. The
// tasks it consumes are DetectionTask protos.
type AnalyticWorker struct {
	worker *entroq.Worker

	mfc *medifor.Client

	maxAttempts int
}

// Option is an analytic worker option used when calling New.
type Option func(*AnalyticWorker)

// WithMaxAttempts sets the maximum attempts to the given value. Values less than 1 mean "unlimited".
func WithMaxAttempts(max int) Option {
	if max < 0 {
		max = 0
	}
	return func(w *AnalyticWorker) {
		w.maxAttempts = max
	}
}

// New creates a new AnalyticWorker. To operate, the worker needs access to a
// task client, the medifor client, and the name of a queue it should read
// tasks from.
func New(mfClient *medifor.Client, eqClient *entroq.EntroQ, workQueues []string, opts ...Option) *AnalyticWorker {
	w := &AnalyticWorker{
		worker: eqClient.NewWorker(workQueues...),
		mfc:    mfClient,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

func moveToDone(task *entroq.Task, work *pb.DetectionTask) ([]entroq.ModifyArg, error) {
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

// Run starts the AnalyticWorker. It claims tasks from the given queues.
func (w *AnalyticWorker) Run(ctx context.Context) error {
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
			log.Printf("Crashing due to crash-worthy error: %v", crashErr)
			// Don't wrap this error, because its cause might be DeadlineExceeded, which is non-fatal to the worker.
			return nil, errors.Errorf("crash-worthy error: %v", crashErr)
		}
		work := new(pb.DetectionTask)
		if err := jsonpb.UnmarshalString(string(task.Value), work); err != nil {
			return nil, errors.Wrap(err, "unmarshal work")
		}

		if w.maxAttempts > 0 && int(task.Claims) > w.maxAttempts {
			work.Detection.Status = status.Newf(codes.Internal, "reached attempt %d / %d, giving up", task.Claims, w.maxAttempts).Proto()
			return moveToDone(task, work)
		}

		if ts := work.AnalyticTimeoutSec; ts != 0 {
			var cancel func()
			ctx, cancel = context.WithTimeout(ctx, time.Duration(ts)*time.Second)
			defer cancel()
		}

		if err := w.mfc.Detect(ctx, work.Detection); err != nil {
			switch status.Code(errors.Cause(err)) {
			case codes.OK:
				// Success - proceed to place in outbox.
			case codes.Unknown,
				codes.InvalidArgument,
				codes.FailedPrecondition,
				codes.Unimplemented,
				codes.Internal:
				log.Printf("Storing error in done queue: %v", err)
				// Permanent failure - proceed to outbox with error (in detection already).
				return moveToDone(task, work)
			case codes.Unavailable,
				codes.DeadlineExceeded,
				codes.Aborted:
				log.Printf("Unrecoverable error, setting up for crash next time: %v", err)
				crashErr = err // cause a crash next time around - we can't proceed safely.
				return moveToDone(task, work)
			default:
				// Unknown error, presume fatal.
				log.Printf("Fatal error: %v", err)
				return nil, errors.Wrap(err, "detection error")
			}
		}

		// No errors, all done.
		return moveToDone(task, work)
	})
}
