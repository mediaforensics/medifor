package cmd

import (
	"context"

	sq "github.com/Masterminds/squirrel"
	"github.com/pkg/errors"

	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
)

func (s *apiService) GetHistogram(ctx context.Context, req *pb.GetHistogramRequest) (*pb.GetHistogramResponse, error) {
	numBuckets := int(req.NumBuckets)

	if numBuckets == 0 {
		numBuckets = 10
	}

	scores := selectScores(req.AnalyticId, req.FuserId).
		LeftJoin("detectionTag USING (detection_id)")

	scores = applyFilters(scores, nil, nil, req.Tags, req.ExcludeTags, "")

	rows, err := sq.Select().
		Column(sq.Alias(sq.Expr("width_bucket(fused_score, 0, 1, ?)", numBuckets), "bucket")).
		Column(sq.Alias(sq.Expr("count(*)"), "frequency")).
		Column(sq.Alias(sq.Expr("min(fused_score)"), "min")).
		Column(sq.Alias(sq.Expr("max(fused_score)"), "max")).
		FromSelect(scores, "scores").
		Where(sq.NotEq{"fused_score": nil}).
		GroupBy("bucket").
		OrderBy("bucket").
		PlaceholderFormat(sq.Dollar).
		RunWith(s.pgdb).
		QueryContext(ctx)

	if err != nil {
		return nil, errors.Wrap(err, "histogram")
	}

	defer rows.Close()

	buckets := make([]*pb.Bucket, numBuckets + 2)

	for i := 0; i < numBuckets + 2; i++ {
		buckets[i] = &pb.Bucket{}
	}

	for rows.Next() {
		var (
			bucketIndex int32
			frequency   int32
			min         float64
			max         float64
		)

		if err := rows.Scan(&bucketIndex, &frequency, &min, &max); err != nil {
			return nil, errors.Wrap(err, "histogram")
		}

		buckets[bucketIndex].Frequency = frequency;
		buckets[bucketIndex].Min = min;
		buckets[bucketIndex].Max = max;
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "histogram")
	}

	buckets[numBuckets].Frequency += buckets[numBuckets + 1].Frequency
	buckets[numBuckets].Max = buckets[numBuckets + 1].Max;

	resp := &pb.GetHistogramResponse{
		Buckets: buckets[1:(numBuckets + 1)],
	}

	return resp, nil
}
