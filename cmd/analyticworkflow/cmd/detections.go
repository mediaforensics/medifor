package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"

	sq "github.com/Masterminds/squirrel"
	"github.com/pkg/errors"

	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
)

func (s *apiService) ListDetections(ctx context.Context, req *pb.ListDetectionsRequest) (*pb.DetectionList, error) {
	scores := selectScores(req.AnalyticId, req.FuserId)

	analyticStats := sq.Select().
		Column("detection_id").
		Column(sq.Alias(sq.Expr("count(*)"), "analytics_total")).
		Column(sq.Alias(sq.Expr("count(finished)"), "analytics_finished")).
		From("detection").
		GroupBy("detection_id")

	detections := sq.Select().
		Column("detection_id").
		Column("tags").
		Column("user_tags").
		Column("meta").
		Column("fused_score").
		Column("analytics_total").
		Column("analytics_finished").
		From("detectionTag").
		JoinClause(sq.ConcatExpr("LEFT JOIN ", sq.Alias(scores, "scores"), " USING (detection_id)")).
		JoinClause(sq.ConcatExpr("LEFT JOIN ", sq.Alias(analyticStats, "analytic_stats"), " USING (detection_id)"))

	detections = applyFilters(detections, req.ScoreFilter, req.MetaFilters, req.Tags, req.ExcludeTags, req.MetaQuery)
	detections = applyOrderBy(detections, req.OrderBy, req.MetaQuery)

	pageSize := req.PageSize

	if pageSize < 1 {
		pageSize = DefaultPageSize
	} else if pageSize > MaxPageSize {
		pageSize = MaxPageSize
	}

	detections = detections.Limit(uint64(pageSize))
	var offset int32

	if req.PageToken != "" {
		token, err := DecodePageToken(req.PageToken)

		if err != nil {
			return nil, errors.Wrap(err, "list detections")
		}

		offset = token.Offset
		detections = detections.Offset(uint64(offset))
	}

	rows, err := detections.PlaceholderFormat(sq.Dollar).
		RunWith(s.pgdb).
		QueryContext(ctx)

	if err != nil {
		return nil, errors.Wrap(err, "list detections")
	}

	defer rows.Close()
	resp := &pb.DetectionList{}

	for rows.Next() {
		var (
			id                string
			tags              []byte
			userTags          []byte
			meta              []byte
			fusedScore        sql.NullFloat64
			analyticsTotal    int32
			analyticsFinished int32
		)

		if err := rows.Scan(&id, &tags, &userTags, &meta, &fusedScore, &analyticsTotal, &analyticsFinished); err != nil {
			return nil, errors.Wrap(err, "list detections")
		}

		detection := &pb.DetectionInfo{
			Id:                id,
			HasFused:          fusedScore.Valid,
			FusedScore:        fusedScore.Float64,
			AnalyticsTotal:    analyticsTotal,
			AnalyticsFinished: analyticsFinished,
		}

		if err := json.Unmarshal(tags, &detection.Tags); err != nil {
			return nil, errors.Wrap(err, "list detections")
		}

		if err := json.Unmarshal(userTags, &detection.UserTags); err != nil {
			return nil, errors.Wrap(err, "list detections")
		}

		if err := json.Unmarshal(meta, &detection.Meta); err != nil {
			return nil, errors.Wrap(err, "list detections")
		}

		resp.Detections = append(resp.Detections, detection)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "list detections")
	}

	count := int32(len(resp.Detections))

	if count == pageSize {
		resp.PageToken = pageToken{Offset: offset + count}.Encode()
	}

	detectionCount := sq.Select().
		Column(sq.Alias(sq.Expr("count(*)"), "total")).
		From("detectionTag").
		JoinClause(sq.ConcatExpr("LEFT JOIN ", sq.Alias(scores, "scores"), " USING (detection_id)"))

	detectionCount = applyFilters(detectionCount, req.ScoreFilter, req.MetaFilters, req.Tags, req.ExcludeTags, req.MetaQuery)

	err = detectionCount.PlaceholderFormat(sq.Dollar).
		RunWith(s.pgdb).
		QueryRowContext(ctx).
		Scan(&resp.Total)

	if err != nil {
		return nil, errors.Wrap(err, "list detections")
	}

	return resp, nil
}

func selectScores(analyticId string, fuserId string) sq.SelectBuilder {
	var scores sq.SelectBuilder

	if analyticId != "" {
		scores = sq.Select().
			Column("detection_id").
			Column(sq.Alias(sq.Case().
				When(
					sq.And{
						sq.NotEq{"detection": nil},
						sq.Expr("detection ?? 'imgManip'"),
						sq.Expr("coalesce(detection->'imgManip'->>'optOut', 'OPT_OUT_NONE') NOT IN ('OPT_OUT_ALL', 'OPT_OUT_DETECTION')"),
					},
					"coalesce(detection->'imgManip'->>'score', '0')::float",
				).
				When(
					sq.And{
						sq.NotEq{"detection": nil},
						sq.Expr("detection ?? 'vidManip'"),
						sq.Expr("NOT (coalesce(detection->'vidManip'->'optOut', '[]'::jsonb) ?? 'VIDEO_OPT_OUT_DETECTION')"),
					},
					"coalesce(detection->'vidManip'->>'score', '0')::float",
				).
				Else("NULL"),
				"fused_score",
			)).
			From("detection").
			Where(sq.NotEq{"finished": nil}).
			Where(sq.Expr("coalesce(detection->'status'->>'code', '0') = '0'")).
			Where(sq.Eq{"analytic_id": analyticId})
	} else {
		bestFusions := sq.Select().
			Column("detection_id").
			Column(sq.Alias(sq.Expr("(array_agg(fusion ORDER BY array_length(analytic_ids, 1) DESC))[1]"), "best_fusion")).
			From("fusion").
			Where(sq.NotEq{"finished": nil}).
			Where(sq.Expr("coalesce(fusion->'status'->>'code', '0') = '0'")).
			Where(sq.Eq{"fuser_id": fuserId}).
			GroupBy("detection_id")

		scores = sq.Select().
			Column("detection_id").
			Column(sq.Alias(sq.Case().
				When(
					sq.And{
						sq.NotEq{"best_fusion": nil},
						sq.Expr("best_fusion ?? 'imgManip'"),
						sq.Expr("coalesce(best_fusion->'imgManip'->>'optOut', 'OPT_OUT_NONE') NOT IN ('OPT_OUT_ALL', 'OPT_OUT_DETECTION')"),
					},
					"coalesce(best_fusion->'imgManip'->>'score', '0')::float",
				).
				When(
					sq.And{
						sq.NotEq{"best_fusion": nil},
						sq.Expr("best_fusion ?? 'vidManip'"),
						sq.Expr("NOT (coalesce(best_fusion->'vidManip'->'optOut', '[]'::jsonb) ?? 'VIDEO_OPT_OUT_DETECTION')"),
					},
					"coalesce(best_fusion->'vidManip'->>'score', '0')::float",
				).
				Else("NULL"),
				"fused_score",
			)).
			FromSelect(bestFusions, "best_fusions")
	}

	return scores
}

func applyFilters(detections sq.SelectBuilder, scoreFilter *pb.ScoreFilter, metaFilters []*pb.MetaFilter, tags map[string]string, excludeTags map[string]string, metaQuery string) sq.SelectBuilder {
	if scoreFilter != nil {
		if scoreFilter.HasMin && scoreFilter.HasMax {
			detections = detections.Where("fused_score BETWEEN ? AND ?", scoreFilter.Min, scoreFilter.Max)
		} else if scoreFilter.HasMin {
			detections = detections.Where("fused_score >= ?", scoreFilter.Min)
		} else if scoreFilter.HasMax {
			detections = detections.Where("fused_score <= ?", scoreFilter.Max)
		}
	}

	for _, f := range metaFilters {
		if f.HasMin && f.HasMax {
			detections = detections.Where("meta->>? BETWEEN ? AND ?", f.MetaKey, f.Min, f.Max)
		} else if f.HasMin {
			detections = detections.Where("meta->>? >= ?", f.MetaKey, f.Min)
		} else if f.HasMax {
			detections = detections.Where("meta->>? <= ?", f.MetaKey, f.Max)
		}
	}

	if tags != nil {
		tagsJSON, err := json.Marshal(tags)

		if err != nil {
			log.Printf("Ignoring tags: %v", err)
		} else {
			detections = detections.Where("(tags || user_tags) @> ?", tagsJSON)
		}
	}

	if excludeTags != nil {
		excludeTagsJSON, err := json.Marshal(excludeTags)

		if err != nil {
			log.Printf("Ignoring exclude_tags: %v", err)
		} else {
			detections = detections.Where("NOT (user_tags ??| array(SELECT jsonb_object_keys(?)))", excludeTagsJSON)
		}
	}

	if metaQuery != "" {
		detections = detections.Where("to_tsvector('english', meta) @@ plainto_tsquery('english', ?)", metaQuery)
	}

	return detections
}

func applyOrderBy(detections sq.SelectBuilder, orderBy []*pb.SortCol, metaQuery string) sq.SelectBuilder {
	for _, o := range orderBy {
		switch o.Key {
		case pb.SortKey_SCORE:
			if o.IsAsc {
				detections = detections.OrderBy("fused_score ASC  NULLS LAST")
			} else {
				detections = detections.OrderBy("fused_score DESC NULLS LAST")
			}

		case pb.SortKey_META:
			if o.IsAsc {
				detections = detections.OrderByClause("meta->>? ASC  NULLS LAST", o.MetaKey)
			} else {
				detections = detections.OrderByClause("meta->>? DESC NULLS LAST", o.MetaKey)
			}

		case pb.SortKey_META_QUERY:
			if o.IsAsc {
				detections = detections.OrderByClause("ts_rank_cd(to_tsvector('english', meta), plainto_tsquery('english', ?)) ASC  NULLS LAST", metaQuery)
			} else {
				detections = detections.OrderByClause("ts_rank_cd(to_tsvector('english', meta), plainto_tsquery('english', ?)) DESC NULLS LAST", metaQuery)
			}

		default:
			log.Printf("Ignoring unsupported sort key: %v", o.Key.String())
		}
	}

	return detections
}

func (s *apiService) UpdateDetectionMetadata(ctx context.Context, req *pb.UpdateDetectionMetadataRequest) (*pb.DetectionInfo, error) {
	if req.Metadata != nil {
		metadata, err := json.Marshal(req.Metadata)

		if err != nil {
			return nil, errors.Wrap(err, "update detection metadata")
		}

		_, err = sq.Update("detectionTag").
			Set("meta", sq.Expr("meta || ?::jsonb", metadata)).
			Where(sq.Eq{"detection_id": req.DetectionId}).
			PlaceholderFormat(sq.Dollar).
			RunWith(s.pgdb).
			ExecContext(ctx)

		if err != nil {
			return nil, errors.Wrap(err, "update detection metadata")
		}
	}

	return s.GetDetectionInfo(ctx, &pb.DetectionInfoRequest{Id: req.DetectionId})
}

func (s *apiService) DeleteFailedAnalytics(ctx context.Context, req *pb.DeleteFailedAnalyticsRequest) (*pb.DeleteFailedAnalyticsResponse, error) {
	sql, args, err := sq.Delete("detection").
		Where(sq.NotEq{"finished": nil}).
		Where("coalesce(detection->'status'->>'code', '0') <> '0'").
		Suffix("RETURNING detection_id, analytic_id").
		PlaceholderFormat(sq.Dollar).
		ToSql()

	if err != nil {
		return nil, errors.Wrap(err, "delete failed analytics")
	}

	rows, err := s.pgdb.QueryContext(ctx, sql, args...)

	if err != nil {
		return nil, errors.Wrap(err, "delete failed analytics")
	}

	defer rows.Close()
	deletedAnalytics := make(map[string][]string)

	for rows.Next() {
		var (
			detectionId string
			analyticId  string
		)

		if err := rows.Scan(&detectionId, &analyticId); err != nil {
			return nil, errors.Wrap(err, "delete failed analytics")
		}

		_, err := sq.Delete("fusion").
			Where(sq.Eq{"detection_id": detectionId}).
			Where("? = ANY(analytic_ids)", analyticId).
			PlaceholderFormat(sq.Dollar).
			RunWith(s.pgdb).
			ExecContext(ctx)

		if err != nil {
			return nil, errors.Wrap(err, "delete failed analytics")
		}

		deletedAnalytics[detectionId] = append(deletedAnalytics[detectionId], analyticId)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "delete failed analytics")
	}

	resp := &pb.DeleteFailedAnalyticsResponse{
		DeletedAnalytics: make(map[string]*pb.DeletedAnalytics),
	}

	for detectionId, analyticIds := range deletedAnalytics {
		resp.DeletedAnalytics[detectionId] = &pb.DeletedAnalytics{
			DeletedAnalyticIds: analyticIds,
		}
	}

	return resp, nil;
}
