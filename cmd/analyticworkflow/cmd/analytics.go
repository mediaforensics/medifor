package cmd

import (
	"context"
	"encoding/json"

	sq "github.com/Masterminds/squirrel"
	"github.com/pkg/errors"

	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
)

func (s *apiService) GetAnalyticsWithScores(ctx context.Context, req *pb.GetAnalyticsWithScoresRequest) (*pb.GetAnalyticsWithScoresResponse, error) {
	analyticIds, err := s.getAnalyticsWithScores(ctx, req)

	if err != nil {
		return nil, errors.Wrap(err, "analytics with scores")
	}

	fuserIds, err := s.getFusersWithScores(ctx, req)

	if err != nil {
		return nil, errors.Wrap(err, "analytics with scores")
	}

	resp := &pb.GetAnalyticsWithScoresResponse{
		AnalyticIds: analyticIds,
		FuserIds:    fuserIds,
	}

	return resp, nil
}

func (s *apiService) getAnalyticsWithScores(ctx context.Context, req *pb.GetAnalyticsWithScoresRequest) ([]string, error) {
	analyticIds := []string{}

	query := sq.Select("analytic_id").
		Distinct().
		From("detection").
		LeftJoin("detectionTag USING (detection_id)").
		Where(sq.NotEq{"finished": nil}).
		Where("coalesce(detection->'status'->>'code', '0') = '0'").
		Where(sq.Or{
			sq.And{
				sq.Expr("detection ?? 'imgManip'"),
				sq.Expr("coalesce(detection->'imgManip'->>'optOut', 'OPT_OUT_NONE') NOT IN ('OPT_OUT_ALL', 'OPT_OUT_DETECTION')"),
			},
			sq.And{
				sq.Expr("detection ?? 'vidManip'"),
				sq.Expr("NOT (coalesce(detection->'vidManip'->'optOut', '[]'::jsonb) ?? 'VIDEO_OPT_OUT_DETECTION')"),
			},
		})

	if req.Tags != nil {
		tags, err := json.Marshal(req.Tags)

		if err != nil {
			return nil, err
		}

		query = query.Where("(tags || user_tags) @> ?", tags)
	}

	if req.ExcludeTags != nil {
		excludeTags, err := json.Marshal(req.ExcludeTags)

		if err != nil {
			return nil, err
		}

		query = query.Where("NOT (user_tags ??| array(SELECT jsonb_object_keys(?)))", excludeTags)
	}

	rows, err := query.PlaceholderFormat(sq.Dollar).
		RunWith(s.pgdb).
		QueryContext(ctx)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var analyticId string

		if err := rows.Scan(&analyticId); err != nil {
			return nil, err
		}

		analyticIds = append(analyticIds, analyticId)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return analyticIds, nil
}

func (s *apiService) getFusersWithScores(ctx context.Context, req *pb.GetAnalyticsWithScoresRequest) ([]string, error) {
	fuserIds := []string{}

	query := sq.Select("fuser_id").
		Distinct().
		From("fusion").
		LeftJoin("detectionTag USING (detection_id)").
		Where(sq.NotEq{"finished": nil}).
		Where("coalesce(fusion->'status'->>'code', '0') = '0'").
		Where(sq.Or{
			sq.And{
				sq.Expr("fusion ?? 'imgManip'"),
				sq.Expr("coalesce(fusion->'imgManip'->>'optOut', 'OPT_OUT_NONE') NOT IN ('OPT_OUT_ALL', 'OPT_OUT_DETECTION')"),
			},
			sq.And{
				sq.Expr("fusion ?? 'vidManip'"),
				sq.Expr("NOT (coalesce(fusion->'vidManip'->'optOut', '[]'::jsonb) ?? 'VIDEO_OPT_OUT_DETECTION')"),
			},
		})

	if req.Tags != nil {
		tags, err := json.Marshal(req.Tags)

		if err != nil {
			return nil, err
		}

		query = query.Where("(tags || user_tags) @> ?", tags)
	}

	if req.ExcludeTags != nil {
		excludeTags, err := json.Marshal(req.ExcludeTags)

		if err != nil {
			return nil, err
		}

		query = query.Where("NOT ((tags || user_tags) @> ?)", excludeTags)
	}

	rows, err := query.PlaceholderFormat(sq.Dollar).
		RunWith(s.pgdb).
		QueryContext(ctx)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var fuserId string

		if err := rows.Scan(&fuserId); err != nil {
			return nil, err
		}

		fuserIds = append(fuserIds, fuserId)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return fuserIds, nil
}
