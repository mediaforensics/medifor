package outboxworker

import (
	"context"
	"database/sql"
	"log"

	"entrogo.com/entroq"
	"github.com/golang/protobuf/jsonpb"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
)

// OutboxWorker is a worker that reads from a task queue (analytic outbox) and stores
// results in a database.
type OutboxWorker struct {
	*entroq.Worker

	eqc     *entroq.EntroQ
	pgdb    *sql.DB
	fusGenQ string
}

// New creates an outbox worker that can be run.
func New(eqc *entroq.EntroQ, pgdb *sql.DB, q, fusGenQ string) *OutboxWorker {
	return &OutboxWorker{
		Worker:  eqc.NewWorker(q),
		eqc:     eqc,
		pgdb:    pgdb,
		fusGenQ: fusGenQ,
	}
}

// Run starts a worker and doesn't exit until it is cleanly stopped, encounters
// an error, or the context is canceled.
func (w *OutboxWorker) Run(ctx context.Context) error {
	return w.Worker.Run(ctx, func(ctx context.Context, t *entroq.Task) ([]entroq.ModifyArg, error) {
		val := new(pb.DetectionTask)
		if err := jsonpb.UnmarshalString(string(t.Value), val); err != nil {
			return nil, errors.Wrap(err, "outbox get json")
		}
		if len(val.AnalyticId) != 1 {
			return nil, errors.Errorf("outbox task got %d IDs, expected 1", len(val.AnalyticId))
		}
		aID := val.AnalyticId[0]

		query := `UPDATE detection SET finished = NOW(), task_id = $1, detection = $2
				  WHERE detection_id = $3 AND analytic_id = $4 AND finished IS NULL`
		j, err := new(jsonpb.Marshaler).MarshalToString(val.Detection)
		if err != nil {
			return nil, errors.Wrap(err, "outbox make json")
		}
		if _, err := w.pgdb.ExecContext(ctx, query, uuid.Nil, j, val.Id, aID); err != nil {
			log.Printf("Row missing for detection %q and analytic %q; giving up and deleting offending task: %v", val.Id, aID, err)
			return []entroq.ModifyArg{entroq.Changing(t, entroq.QueueTo(t.Queue+"/err"))}, nil
		}

		modArgs := []entroq.ModifyArg{t.AsDeletion()}
		if len(val.FuserId) != 0 {
			log.Printf("FuserIDs found for %v.  Creating tasks for fusion", val.FuserId)
			modArgs = append(modArgs, entroq.InsertingInto(w.fusGenQ, entroq.WithValue(t.Value)))
		}

		return modArgs, nil
	})
}
