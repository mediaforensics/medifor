package fusiongenworker

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"entrogo.com/entroq"
	"github.com/golang/protobuf/jsonpb"
	"github.com/pkg/errors"
	"github.com/mediaforensics/medifor/pkg/analytic"
	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
)

type FusionGenWorker struct {
	*entroq.Worker

	eqc  *entroq.EntroQ
	pgdb *sql.DB

	fusQ      string
	fusOutQ   string
	fusOutDir string
}

func New(eqc *entroq.EntroQ, pgdb *sql.DB, inbox, outQ, fusOutQ, fusOutDir string) *FusionGenWorker {
	return &FusionGenWorker{
		Worker:    eqc.NewWorker(inbox),
		eqc:       eqc,
		pgdb:      pgdb,
		fusQ:      outQ,
		fusOutQ:   fusOutQ,
		fusOutDir: fusOutDir,
	}
}

func (w *FusionGenWorker) Run(ctx context.Context) error {
	return w.Worker.Run(ctx, func(ctx context.Context, t *entroq.Task) ([]entroq.ModifyArg, error) {
		log.Print("Task claimed by FusionGenWorker")
		val := new(pb.DetectionTask)
		if err := jsonpb.UnmarshalString(string(t.Value), val); err != nil {
			return nil, errors.Wrap(err, "fusion task generator get task json")
		}

		// If there are no fuserIDs specified we don't create any fusion tasks
		if len(val.FuserId) == 0 {
			return nil, errors.Errorf("no fuserIDs specified for task %v, can't create fusion task", t.IDVersion())
		}

		if val.Id == "" {
			return nil, errors.New("no detection ID for fusion")
		}

		query := `SELECT detection, analytic_id FROM detection WHERE detection_id = $1 AND finished IS NOT NULL;`
		rows, err := w.pgdb.Query(query, val.Id)
		if err != nil {
			return nil, errors.Wrap(err, "list query for analytic IDs in fusion task gen worker")
		}
		defer rows.Close()

		var ds []*pb.AnnotatedDetection
		logLine := "Analytic IDs from query: "
		for rows.Next() {
			var (
				analyticID string
				detection  string
			)
			if err := rows.Scan(&detection, &analyticID); err != nil {
				return nil, errors.Wrap(err, "scan error")
			}
			det, err := analytic.UnmarshalDetectionJSON(detection)
			if err != nil {
				return nil, errors.Wrap(err, "unmarshal detection json")
			}
			ds = append(ds, &pb.AnnotatedDetection{
				AnalyticId: analyticID,
				Detection:  det,
			})
			logLine += fmt.Sprintf(" %v ", analyticID)
		}
		if len(ds) == 0 {
			log.Printf("Row missing for detection %q ; giving up and deleting offending task", val.Id)
			return []entroq.ModifyArg{entroq.Changing(t, entroq.QueueTo(t.Queue+"/err"))}, nil
		}
		log.Printf("FUSION GEN WORKER:: detectionID %v -- %v", val.Id, logLine)
		fusion, err := analytic.FusionFromDetections(ds, w.fusOutDir)
		if err != nil {
			return nil, errors.Wrap(err, "fusion from annotated detections")
		}

		newTask := &pb.FusionTask{
			DoneQueue:   w.fusOutQ,
			Fusion:      fusion,
			FuserId:     val.FuserId,
			Tags:        val.Tags, // TODO Consider adding fusion specific tags
			DetectionId: val.Id,
		}
		j, err := new(jsonpb.Marshaler).MarshalToString(newTask)
		if err != nil {
			return nil, errors.Wrap(err, "marshal new task")
		}

		modArgs := []entroq.ModifyArg{t.AsDeletion(), entroq.InsertingInto(
			w.fusQ,
			entroq.WithValue([]byte(j))),
		}
		log.Printf("Inserting fusion task into %v", w.fusQ)
		return modArgs, nil
	})

}
