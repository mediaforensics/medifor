package outboxworker

import (
	"context"
	"database/sql"
	"log"

	"entrogo.com/entroq"
	"github.com/golang/protobuf/jsonpb"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/pkg/errors"
	"github.com/mediaforensics/medifor/pkg/analytic"
	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
)

// OutboxWorker is a worker that reads from a task queue (analytic outbox) and stores
// results in a database.
type FusionOutboxWorker struct {
	*entroq.Worker

	eqc  *entroq.EntroQ
	pgdb *sql.DB
	// queue string
}

// New creates an outbox worker that can be run.
func NewFusion(eqc *entroq.EntroQ, pgdb *sql.DB, q string) *FusionOutboxWorker {
	return &FusionOutboxWorker{
		Worker: eqc.NewWorker(q),
		eqc:    eqc,
		pgdb:   pgdb,
		// queue: q,
	}
}

// Run starts a worker and doesn't exit until it is cleanly stopped, encounters
// an error, or the context is canceled.
func (w *FusionOutboxWorker) Run(ctx context.Context) error {
	return w.Worker.Run(ctx, func(ctx context.Context, t *entroq.Task) ([]entroq.ModifyArg, error) {
		val := new(pb.FusionTask)
		if err := jsonpb.UnmarshalString(string(t.Value), val); err != nil {
			return nil, errors.Wrap(err, "outbox get json")
		}
		if len(val.FuserId) != 1 {
			return nil, errors.Errorf("outbox task got %d IDs, expected 1", len(val.FuserId))
		}
		fID := val.FuserId[0]

		query := `UPDATE fusion SET finished = NOW(), task_id = $1, fusion = $2
				  WHERE detection_id = $3 AND fuser_id = $4 AND analytic_ids = $5 AND finished IS NULL`
		j, err := new(jsonpb.Marshaler).MarshalToString(val.Fusion)
		if err != nil {
			return nil, errors.Wrap(err, "outbox make json")
		}
		analyticIDs, err := analytic.AnalyticIDsFromFusion(val.Fusion)
		if err != nil {
			errors.Wrap(err, "FUSION OUTBOX WORKER: get analytic ids from fusion")
		}

		result, err := w.pgdb.ExecContext(ctx, query, uuid.Nil, j, val.DetectionId, fID, pq.Array(analyticIDs))

		if err != nil {
			return nil, errors.Wrapf(err, "outbox update row d=%s, a=%s", val.DetectionId, fID)
		}
		if n, err := result.RowsAffected(); err != nil {
			return nil, errors.Wrapf(err, "outbox update row d=%s, a=%s", val.DetectionId, fID)
		} else if n != 1 {
			log.Printf("ERROR in fusioOutboxWorker -- fuserID: %v DetectionID: %v -- Updated %d rows, usually expect to update 1", val.FuserId, val.DetectionId, n)
			return []entroq.ModifyArg{t.AsChange(entroq.QueueTo(t.Queue + "/err"))}, nil
		}
		return []entroq.ModifyArg{t.AsDeletion()}, nil
	})
}
