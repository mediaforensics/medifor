package cmd

import (
	"context"

	"entrogo.com/entroq"
	sq "github.com/Masterminds/squirrel"
	"github.com/lib/pq"
	"github.com/pkg/errors"

	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
)

func (s *apiService) GetAnalyticStats(ctx context.Context, req *pb.GetAnalyticStatsRequest) (*pb.GetAnalyticStatsResponse, error) {
	analyticStats, err := s.getAnalyticStats(ctx, req.AnalyticIds)

	if err != nil {
		return nil, errors.Wrap(err, "analytic stats")
	}

	fuserStats, err := s.getFuserStats(ctx, req.FuserIds)

	if err != nil {
		return nil, errors.Wrap(err, "analytic stats")
	}

	resp := &pb.GetAnalyticStatsResponse{
		AnalyticStats: analyticStats,
		FuserStats:    fuserStats,
	}

	return resp, nil
}

func (s *apiService) getAnalyticStats(ctx context.Context, analyticIds []string) ([]*pb.AnalyticStats, error) {
	query := sq.Select().
		Column("analytic_id").
		Column(sq.Alias(filter("count(*)",
			sq.And{
				sq.NotEq{"finished": nil},
				sq.Expr("coalesce(detection->'status'->>'code', '0') = '0'"),
				sq.And{
					sq.Expr("coalesce(detection->'imgManip'->>'optOut', 'OPT_OUT_NONE') NOT IN ('OPT_OUT_ALL', 'OPT_OUT_DETECTION')"),
					sq.Expr("NOT (coalesce(detection->'vidManip'->'optOut', '[]'::jsonb) ?? 'VIDEO_OPT_OUT_DETECTION')"),
				},
			}),
			"succeeded")).
		Column(sq.Alias(filter("count(*)",
			sq.And{
				sq.NotEq{"finished": nil},
				sq.Expr("coalesce(detection->'status'->>'code', '0') = '0'"),
				sq.Or{
					sq.Expr("coalesce(detection->'imgManip'->>'optOut', 'OPT_OUT_NONE') IN ('OPT_OUT_ALL', 'OPT_OUT_DETECTION')"),
					sq.Expr("coalesce(detection->'vidManip'->'optOut', '[]'::jsonb) ?? 'VIDEO_OPT_OUT_DETECTION'"),
				},
			}),
			"opted_out")).
		Column(sq.Alias(filter("count(*)",
			sq.And{
				sq.NotEq{"finished": nil},
				sq.Expr("coalesce(detection->'status'->>'code', '0') <> '0'"),
			}),
			"failed")).
		From("detection").
		GroupBy("analytic_id")

	if len(analyticIds) > 0 {
		query = query.Where("analytic_id = any(?)", pq.StringArray(analyticIds))
	}

	rows, err := query.PlaceholderFormat(sq.Dollar).
		RunWith(s.pgdb).
		QueryContext(ctx)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	queueStats, err := s.eqc.QueueStats(ctx, entroq.MatchPrefix("/task/detection/analytic/"))

	if err != nil {
		return nil, err
	}

	analyticStats := []*pb.AnalyticStats{}

	for rows.Next() {
		var (
			id        string
			pending   int32
			running   int32
			succeeded int32
			optedOut  int32
			failed    int32
		)

		if err := rows.Scan(&id, &succeeded, &optedOut, &failed); err != nil {
			return nil, err
		}

		qs, ok := queueStats[queueConf.AnalyticInbox(id)]

		if ok {
			pending = int32(qs.Available)
			running = int32(qs.Claimed)
		}

		stats := &pb.AnalyticStats{
			Id:        id,
			Pending:   pending,
			Running:   running,
			Succeeded: succeeded,
			OptedOut:  optedOut,
			Failed:    failed,
		}

		analyticStats = append(analyticStats, stats)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return analyticStats, nil
}

func (s *apiService) getFuserStats(ctx context.Context, fuserIds []string) ([]*pb.AnalyticStats, error) {
	query := sq.Select().
		Column("fuser_id").
		Column(sq.Alias(filter("count(*)",
			sq.And{
				sq.NotEq{"finished": nil},
				sq.Expr("coalesce(fusion->'status'->>'code', '0') = '0'"),
				sq.And{
					sq.Expr("coalesce(fusion->'imgManip'->>'optOut', 'OPT_OUT_NONE') NOT IN ('OPT_OUT_ALL', 'OPT_OUT_DETECTION')"),
					sq.Expr("NOT (coalesce(fusion->'vidManip'->'optOut', '[]'::jsonb) ?? 'VIDEO_OPT_OUT_DETECTION')"),
				},
			}),
			"succeeded")).
		Column(sq.Alias(filter("count(*)",
			sq.And{
				sq.NotEq{"finished": nil},
				sq.Expr("coalesce(fusion->'status'->>'code', '0') = '0'"),
				sq.Or{
					sq.Expr("coalesce(fusion->'imgManip'->>'optOut', 'OPT_OUT_NONE') IN ('OPT_OUT_ALL', 'OPT_OUT_DETECTION')"),
					sq.Expr("coalesce(fusion->'vidManip'->'optOut', '[]'::jsonb) ?? 'VIDEO_OPT_OUT_DETECTION'"),
				},
			}),
			"opted_out")).
		Column(sq.Alias(filter("count(*)",
			sq.And{
				sq.NotEq{"finished": nil},
				sq.Expr("coalesce(fusion->'status'->>'code', '0') <> '0'"),
			}),
			"failed")).
		From("fusion").
		GroupBy("fuser_id")

	if len(fuserIds) > 0 {
		query = query.Where("fuser_id = any(?)", pq.StringArray(fuserIds))
	}

	rows, err := query.PlaceholderFormat(sq.Dollar).
		RunWith(s.pgdb).
		QueryContext(ctx)

	if err != nil {
		return nil, err
	}

	defer rows.Close()
	queueStats, err := s.eqc.QueueStats(ctx, entroq.MatchPrefix("/task/fusion/fuser/"))

	if err != nil {
		return nil, err
	}

	fuserStats := []*pb.AnalyticStats{}

	for rows.Next() {
		var (
			id        string
			pending   int32
			running   int32
			succeeded int32
			optedOut  int32
			failed    int32
		)

		if err := rows.Scan(&id, &succeeded, &optedOut, &failed); err != nil {
			return nil, err
		}

		qs, ok := queueStats[queueConf.FuserInbox(id)]

		if ok {
			pending = int32(qs.Available)
			running = int32(qs.Claimed)
		}

		stats := &pb.AnalyticStats{
			Id:        id,
			Pending:   pending,
			Running:   running,
			Succeeded: succeeded,
			OptedOut:  optedOut,
			Failed:    failed,
		}

		fuserStats = append(fuserStats, stats)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return fuserStats, nil
}

func filter(aggregateExpr interface{}, filterClause interface{}) sq.Sqlizer {
	return sq.ConcatExpr(aggregateExpr, " FILTER (WHERE ", filterClause, ")")
}
