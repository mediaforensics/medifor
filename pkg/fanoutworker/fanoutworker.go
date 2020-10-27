package fanoutworker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"path/filepath"
	"strings"
	"unicode"

	"entrogo.com/entroq"
	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/mediaforensics/medifor/pkg/analytic"

	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
)

// Worker is a worker that takes a task from one queue and creates multiple
// tasks in different output queues based on analytic IDs.
type Worker struct {
	*entroq.Worker

	eqc  *entroq.EntroQ
	pgdb *sql.DB

	outQFmt string

	omitAnalyticIDInOutDir bool
}

// Option is used to set worker creation options in New.
type Option func(*Worker)

// OmittingAnalyticIDInOutDir causes the fanout worker to *not* add the analytic ID
// to output directories specified in input tasks.
func OmittingAnalyticIDInOutDir() Option {
	return func(w *Worker) {
		w.omitAnalyticIDInOutDir = true
	}
}

// New creates a new runnable fanout worker, given an EntroQ client, a
// PostgreSQL database, an inbox name, and an output task queue format string
// with a single "%s" specifier. The analytic ID in a request will be inserted
// in that point in the format string, after applying url.PathEscape to it.
func New(eqc *entroq.EntroQ, pgdb *sql.DB, inbox, outQFmt string, opts ...Option) *Worker {
	w := &Worker{
		Worker:  eqc.NewWorker(inbox),
		eqc:     eqc,
		pgdb:    pgdb,
		outQFmt: outQFmt,
	}

	for _, o := range opts {
		o(w)
	}

	return w
}

// AnalyticInbox returns the inbox name for a given analytic ID.
func (w *Worker) AnalyticInbox(id string) string {
	return analytic.AnalyticInbox(w.outQFmt, id)
}

func tagsJSON(m map[string]string) string {
	if m == nil {
		return "{}"
	}
	j, err := json.Marshal(m)
	if err != nil {
		log.Printf("Error marshaling tags %v: %v", m, err)
		return "{}"
	}
	// PostgreSQL really doesn't like null unicode characters in JSON.
	s := strings.Replace(string(j), "\u0000", string(unicode.ReplacementChar), -1)
	if s == "" {
		return "{}"
	}
	return s
}

// Run runs the fanout worker until it is stopped or the context is canceled or
// a permanent error occurs.
func (w *Worker) Run(ctx context.Context) error {
	return w.Worker.Run(ctx, func(ctx context.Context, t *entroq.Task) ([]entroq.ModifyArg, error) {
		val := new(pb.DetectionTask)
		if err := jsonpb.UnmarshalString(string(t.Value), val); err != nil {
			return nil, errors.Wrap(err, "fanout get json")
		}

		if len(val.AnalyticId) == 0 {
			return nil, errors.Errorf("no analytic IDs specified for task %v, can't fan out", t.IDVersion())
		}

		// If tags are specified, we need to insert them into the appropriate
		// tag table, so we use a "common table expression", adding an
		// appropriate "WITH" statement to the head of the query.
		if len(val.Tags) > 0 || len(val.UserTags) > 0 || len(val.Meta) > 0 {
			jTags := tagsJSON(val.GetTags())
			jUTags := tagsJSON(val.GetUserTags())
			jMeta := tagsJSON(val.GetMeta())
			query := `
				INSERT INTO detectionTag (detection_id, tags, user_tags, meta)
				VALUES ($1, $2::jsonb, $3::jsonb, $4::jsonb)
				ON CONFLICT (detection_id)
				DO UPDATE SET
					tags = detectionTag.tags || excluded.tags,
					user_tags = detectionTag.user_tags || excluded.user_tags,
					meta = detectionTag.meta || excluded.meta`
			if _, err := w.pgdb.ExecContext(ctx, query, val.Id, jTags, jUTags, jMeta); err != nil {
				return nil, errors.Wrap(err, "upsert tags")
			}
		}

		// Then set up the detection insertions, as needed.
		//
		// Note that ON CONFLICT DO UPDATE performs an empty update. This is
		// required in PostgreSQL to make the RETURNING clause work correctly
		// when things don't update. This way we can get at the task ID that
		// is *already stored there* when we are attempting to insert something.
		//
		// We get the task IDs from existing rows *and* new rows this way, and
		// then we force those IDs to be the true task IDs in the update below.
		// That way, even if we get killed between the DB update and task
		// insertion, we will have stable IDs in the rows and can work with
		// idempotent updates.
		//
		// Technically, we could just force a brand new random task ID every time
		// we attempt an update here, but watching that happen live wil likely be
		// more confusing than trying to understand this query when doing
		// maintenance.
		var (
			args   []string
			params []interface{}
		)

		analyticsSeen := make(map[string]bool)
		var singleTasks []*pb.DetectionTask
		for i, aID := range val.AnalyticId {
			// Skip any we've already seen.
			if analyticsSeen[aID] {
				continue
			}
			analyticsSeen[aID] = true

			newVal := proto.Clone(val).(*pb.DetectionTask)
			newVal.AnalyticId = val.AnalyticId[i : i+1]
			det := newVal.GetDetection()
			if det == nil {
				return nil, errors.Errorf("nil detection in task: %v", val)
			}
			// If we are supposed to ensure that the output directory ends with
			// the analytic ID, take care of that now. The default is to add,
			// which is why we check for "not omit".
			if !w.omitAnalyticIDInOutDir {
				oDir, err := analytic.OutDir(det)
				if err != nil {
					return nil, errors.Wrapf(err, "outdir: %v", det)
				}
				if filepath.Base(oDir) != aID {
					if err := analytic.SetOutDir(det, filepath.Join(oDir, aID)); err != nil {
						return nil, errors.Wrapf(err, "set outdir: %v", det)
					}
				}
			}
			singleTasks = append(singleTasks, newVal)

			jDet, err := new(jsonpb.Marshaler).MarshalToString(det)
			if err != nil {
				return nil, errors.Wrap(err, "format insert json")
			}

			np := len(params)
			args = append(args, fmt.Sprintf("($%d, $%d, $%d, $%d)", np+1, np+2, np+3, np+4))
			params = append(params, newVal.Id, aID, uuid.New(), jDet)
		}
		query := `
			INSERT INTO detection AS d (detection_id, analytic_id, task_id, detection)
			VALUES ` + strings.Join(args, ", ") + `
			ON CONFLICT (detection_id, analytic_id) DO UPDATE SET created = d.created
			RETURNING task_id`

		rows, err := w.pgdb.QueryContext(ctx, query, params...)
		if err != nil {
			return nil, errors.Wrapf(err, "insert detections: args=%v", params)
		}
		defer rows.Close()

		var taskIDs []uuid.UUID
		for rows.Next() {
			var tid uuid.UUID
			if err := rows.Scan(&tid); err != nil {
				return nil, errors.Wrap(err, "task ID read from detection insert")
			}
			taskIDs = append(taskIDs, tid)
		}

		// Start our modification with a deletion of the claimed task.
		modArgs := []entroq.ModifyArg{t.AsDeletion()}
		// Add fanout tasks.
		// Note that there is a situation that can arise where we are
		// attempting to insert tasks that already exist. We use the database
		// to determine whether that's a problem. If any of the task IDs is the
		// zero value, then that detection is done and we don't insert the
		// task. That means that we'll attempt to insert only tasks that
		// correspond to unfinished work.
		//
		// We use entroq's facility for skipping colliding inserts and retrying
		// to only insert tasks that are not already in the task store.
		for i, aTask := range singleTasks {
			if len(aTask.AnalyticId) != 1 {
				return nil, fmt.Errorf("wrong number of analytics in fanout output task: %v", aTask)
			}
			aID := aTask.AnalyticId[0]
			taskID := taskIDs[i]
			if taskID == uuid.Nil {
				log.Printf("Detection %q for analytic %q already done, skipping", val.Id, aID)
				continue
			}
			j, err := new(jsonpb.Marshaler).MarshalToString(aTask)
			if err != nil {
				return nil, errors.Wrapf(err, "fanout new json: %v", aTask)
			}
			// Force task IDs to match those in the database.
			// This allows us to see which ones are claimed to create progress
			// information.
			modArgs = append(modArgs, entroq.InsertingInto(
				w.AnalyticInbox(aID),
				entroq.WithID(taskID),
				entroq.WithSkipColliding(true),
				entroq.WithValue([]byte(j))))
		}
		return modArgs, nil
	})
}
