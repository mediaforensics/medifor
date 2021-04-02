package cmd

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"entrogo.com/entroq"
	grpcbackend "entrogo.com/entroq/grpc"
	"github.com/golang/protobuf/jsonpb"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"github.com/mediaforensics/medifor/pkg/analytic"
	"github.com/mediaforensics/medifor/pkg/configlist"
	"github.com/mediaforensics/medifor/pkg/fanoutworker"
	"github.com/mediaforensics/medifor/pkg/fusionfanoutworker"
	"github.com/mediaforensics/medifor/pkg/fusiongenworker"
	"github.com/mediaforensics/medifor/pkg/medifor"
	"github.com/mediaforensics/medifor/pkg/outboxworker"
	"github.com/mediaforensics/medifor/pkg/pg"
	homedir "github.com/mitchellh/go-homedir"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/status"

	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
	hpb "google.golang.org/grpc/health/grpc_health_v1"
)

const (
	DefaultGlobalQueuePrefix = ""
	DefaultAnalyticInboxFmt  = "/task/detection/analytic/%s/inbox"
	DefaultDetectionInbox    = "/task/detection/inbox"
	DefaultDetectionOutbox   = "/task/detection/outbox"
	DefaultFuserInboxFmt     = "/task/fusion/fuser/%s/inbox"
	DefaultFusionInbox       = "/task/fusion/inbox"
	DefaultFusionOutbox      = "/task/fusion/outbox"
	DefaultFusionGenInbox    = "/task/fusiongen/inbox"

	DefaultPageSize = 100
	MaxPageSize     = 250
)

type QueueConfig struct {
	prefix           string
	analyticInboxFmt string
	detectionInbox   string
	detectionOutbox  string
	fuserInboxFmt    string
	fusionInbox      string
	fusionOutbox     string
	fusionGenInbox   string
}

func NewQueueConfig() *QueueConfig {
	return &QueueConfig{
		prefix:           "",
		analyticInboxFmt: DefaultAnalyticInboxFmt,
		detectionInbox:   DefaultDetectionInbox,
		detectionOutbox:  DefaultDetectionOutbox,
		fuserInboxFmt:    DefaultFuserInboxFmt,
		fusionInbox:      DefaultFusionInbox,
		fusionOutbox:     DefaultFusionOutbox,
		fusionGenInbox:   DefaultFusionGenInbox,
	}
}

func (qc *QueueConfig) AnalyticInboxFmt() string {
	return qc.prefix + qc.analyticInboxFmt
}

func (qc *QueueConfig) AnalyticInbox(id string) string {
	return analytic.AnalyticInbox(qc.AnalyticInboxFmt(), id)
}

func (qc *QueueConfig) DetectionInbox() string {
	return qc.prefix + qc.detectionInbox
}

func (qc *QueueConfig) DetectionOutbox() string {
	return qc.prefix + qc.detectionOutbox
}

func (qc *QueueConfig) FuserInboxFmt() string {
	return qc.prefix + qc.fuserInboxFmt
}

func (qc *QueueConfig) FuserInbox(id string) string {
	return analytic.AnalyticInbox(qc.FuserInboxFmt(), id)
}

func (qc *QueueConfig) FusionInbox() string {
	return qc.prefix + qc.fusionInbox
}

func (qc *QueueConfig) FusionOutbox() string {
	return qc.prefix + qc.fusionOutbox
}

func (qc *QueueConfig) FusionGenInbox() string {
	return qc.prefix + qc.fusionGenInbox
}

type ServerSettings struct {
	TLS            bool
	VerifyClient   bool
	ClientCAFile   string
	ServerCertFile string
	ServerKeyFile  string
}

func NewServerSettings() *ServerSettings {
	return &ServerSettings{}
}

func (s *ServerSettings) GRPCServerOptions() ([]grpc.ServerOption, error) {
	if !s.TLS {
		return nil, nil
	}

	if s.ServerCertFile == "" || s.ServerKeyFile == "" {
		return nil, errors.New("TLS serving specified, but either the key or cert file are missing")
	}

	tc := &tls.Config{}

	// TLS is on - try to get the cert.
	cert, err := tls.LoadX509KeyPair(s.ServerCertFile, s.ServerKeyFile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to load server key/cert for TLS")
	}
	tc.Certificates = []tls.Certificate{cert}

	if s.VerifyClient {
		tc.ClientAuth = tls.RequireAndVerifyClientCert
		if s.ClientCAFile == "" {
			log.Printf("No client CA pool specified, using system CA pool")
			pool, err := x509.SystemCertPool()
			if err != nil {
				return nil, errors.Wrap(err, "could not get system cert pool for client TLS")
			}
			tc.ClientCAs = pool
		} else {
			pool := x509.NewCertPool()
			pem, err := ioutil.ReadFile(s.ClientCAFile)
			if err != nil {
				return nil, errors.Wrap(err, "unable to read client CA certs")
			}
			if ok := pool.AppendCertsFromPEM(pem); !ok {
				return nil, errors.Wrap(err, "no valid CA certs were found in client config")
			}
			tc.ClientCAs = pool
		}
	}

	creds := credentials.NewTLS(tc)
	return []grpc.ServerOption{grpc.Creds(creds)}, nil
}

var (
	cfgFile string
	fusDir  string
	// GRPC listening network address.
	listen string

	// PostgreSQL service flags.
	pgAddr string
	pgUser string
	pgPass string
	pgDB   string

	// EntroQ service address.
	eqAddr string

	// Fusion address, if any
	fuseAddr string

	// Queue configuration flags.
	queueConf = NewQueueConfig()

	// Other flags.
	numFanout          int
	numOutbox          int
	analyticConfigPath string
	verbose            bool
	skipIDOutDirSuffix bool

	serverConf ServerSettings
)

var errFuserID = errors.New("No fuserID specified for score sorting")

type apiService struct {
	sync.Mutex

	eqc  *entroq.EntroQ
	pgdb *sql.DB

	analyticList *pb.AnalyticList
}

func newAPIService(eqc *entroq.EntroQ, pgdb *sql.DB) *apiService {
	return &apiService{
		eqc:  eqc,
		pgdb: pgdb,
	}
}

func isGRPCFatal(err error) bool {
	switch status.Code(errors.Cause(err)) {
	case codes.OK, codes.Canceled, codes.DeadlineExceeded, codes.NotFound:
		return false
	default:
		return true
	}
}

// Detect handles a request to start a detection process (of some kind). It inserts an appropriate task in the inbox.
func (s *apiService) Detect(ctx context.Context, req *pb.DetectionRequest) (*pb.DetectionInfo, error) {
	id := req.Id
	if id == "" {
		id = uuid.New().String()
	}
	contents := &pb.DetectionTask{
		Id:                 id,
		DoneQueue:          queueConf.DetectionOutbox(),
		Detection:          req.Request,
		AnalyticId:         req.AnalyticId,
		Tags:               req.Tags,
		UserTags:           req.UserTags,
		Meta:               req.Meta,
		AnalyticTimeoutSec: req.AnalyticTimeoutSec,
		FuserId:            req.FuserId,
	}

	val, err := new(jsonpb.Marshaler).MarshalToString(contents)
	if err != nil {
		return nil, errors.Wrap(err, "detect task json")
	}

	if _, _, err := s.eqc.Modify(ctx, entroq.InsertingInto(queueConf.DetectionInbox(), entroq.WithValue([]byte(val)))); err != nil {
		if isGRPCFatal(err) {
			log.Fatalf("Fatal rpc error in modify: %v", err)
		}
		return nil, errors.Wrap(err, "detect task insert")
	}

	info := &pb.DetectionInfo{
		Id:       contents.Id,
		Tags:     req.Tags,
		UserTags: req.UserTags,
		Meta:     req.Meta,
	}

	for _, aID := range contents.AnalyticId {
		info.AnalyticInfo = append(info.AnalyticInfo, &pb.AnalyticDetectionInfo{
			AnalyticId: aID,
			Detection:  req.Request,
		})
	}

	return info, nil
}

// Fuse performs analytic fusion across all analytics on the analytic results contained within the request.
func (s *apiService) Fuse(ctx context.Context, req *pb.FusionRequest) (*pb.FusionInfo, error) {
	id := req.Id
	if id == "" {
		id = uuid.New().String()
	}
	contents := &pb.FusionTask{
		DoneQueue:   queueConf.FusionOutbox(),
		Fusion:      req.Request,
		FuserId:     req.FuserId,
		Tags:        req.Tags,
		DetectionId: req.DetectionId,
	}

	val, err := new(jsonpb.Marshaler).MarshalToString(contents)
	if err != nil {
		return nil, errors.Wrap(err, "fusion task json")
	}

	if _, _, err := s.eqc.Modify(ctx, entroq.InsertingInto(queueConf.FusionInbox(), entroq.WithValue([]byte(val)))); err != nil {
		if isGRPCFatal(err) {
			log.Fatalf("Fatal rpc error in modify: %v", err)
		}
		return nil, errors.Wrap(err, "fusion task insert")
	}

	info := new(pb.FusionInfo)

	for _, fID := range contents.FuserId {
		info.FusionInfos = append(info.FusionInfos, &pb.FuserFusionInfo{
			FuserId: fID,
			Fusion:  req.Request,
		})
	}

	return info, nil
}

// FuseByID performs analytic fusion across all analytics on the analytic results for a given detectionID.
func (s *apiService) FuseByID(ctx context.Context, req *pb.FusionRequest) (*pb.FusionInfo, error) {
	query := "SELECT detection, analytic_id FROM detection WHERE detection_id = $1 AND finished IS NOT NULL"
	if req.DetectionId == "" {
		return nil, errors.New("No detection ID in request")
	}
	rows, err := s.pgdb.Query(query, req.DetectionId)
	if err != nil {
		return nil, errors.Wrap(err, "list query")
	}
	defer rows.Close()

	var ds []*pb.AnnotatedDetection
	for rows.Next() {
		var (
			detection  string
			analyticID string
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
		if verbose {
			log.Printf("%v -- AnalyticID: %v", req.DetectionId, analyticID)
		}
	}

	fusion, err := analytic.FusionFromDetections(ds, req.DetectionIdOutDir)
	if err != nil {
		return nil, errors.Wrap(err, "fusion from annotated detections")
	}

	req.Request = fusion

	return s.Fuse(ctx, req)
}

// FuseAllIDs sets up tasks to fuse all known detection IDs.
func (s *apiService) FuseAllIDs(ctx context.Context, req *pb.FuseAllIDsRequest) (*pb.FuseAllIDsResponse, error) {
	rows, err := s.pgdb.Query("SELECT DISTINCT detection_id FROM detection")
	if err != nil {
		return nil, errors.Wrap(err, "fuse all, get IDs")
	}
	defer rows.Close()

	for rows.Next() {
		var detectionID string
		if err := rows.Scan(&detectionID); err != nil {
			return nil, errors.Wrap(err, "fuse all, scan ID")
		}

		if _, err := s.FuseByID(ctx, &pb.FusionRequest{
			DetectionId:       detectionID,
			DetectionIdOutDir: req.OutDir,
			FuserId:           req.FuserId,
		}); err != nil {
			return nil, errors.Wrapf(err, "fuse all, fuse by ID=%s", detectionID)
		}
	}
	return new(pb.FuseAllIDsResponse), nil
}

// GetDetectionInfo requests detection info for the given ID from the database.
func (s *apiService) GetDetectionInfo(ctx context.Context, req *pb.DetectionInfoRequest) (*pb.DetectionInfo, error) {
	resp := &pb.DetectionInfo{
		Id: req.Id,
	}

	tagQuery := `SELECT tags, user_tags, meta FROM detectionTag WHERE detection_id = $1`
	jTags := "{}"
	jUTags := "{}"
	jMeta := "{}"
	if err := s.pgdb.QueryRowContext(ctx, tagQuery, req.Id).Scan(&jTags, &jUTags, &jMeta); err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrapf(err, "get tags %q", req.Id)
	}
	if err := json.Unmarshal([]byte(jTags), &resp.Tags); err != nil {
		return nil, errors.Wrap(err, "unmarshal tags")
	}
	if err := json.Unmarshal([]byte(jUTags), &resp.UserTags); err != nil {
		return nil, errors.Wrap(err, "unmarshal user tags")
	}
	if err := json.Unmarshal([]byte(jMeta), &resp.Meta); err != nil {
		return nil, errors.Wrap(err, "unmarshal metadata")
	}

	countQuery := "SELECT count(*) as total, count(*) FILTER(WHERE finished IS NOT NULL) as done FROM detection WHERE detection_id = $1"
	if err := s.pgdb.QueryRowContext(ctx, countQuery, req.Id).Scan(&resp.AnalyticsTotal, &resp.AnalyticsFinished); err != nil {
		return nil, errors.Wrapf(err, "detectionInfo -- get analytic progress counts")
	}

	query := `SELECT analytic_id, task_id, detection FROM detection WHERE detection_id = $1`
	rows, err := s.pgdb.QueryContext(ctx, query, req.Id)
	if err != nil {
		return nil, errors.Wrapf(err, "query detection %q", req.Id)
	}
	defer rows.Close()

	var detections []*pb.AnnotatedDetection

	for rows.Next() {
		var (
			aID       string
			tID       uuid.UUID
			detection string
		)
		if err := rows.Scan(&aID, &tID, &detection); err != nil {
			return nil, errors.Wrap(err, "get detection info row scan")
		}
		aName, aVersion := medifor.ParseAnalyticID(aID)

		det, err := analytic.UnmarshalDetectionJSON(detection)
		if err != nil {
			return nil, errors.Wrap(err, "unmarshal detection")
		}

		// Save for later potential fusion needs.
		detections = append(detections, &pb.AnnotatedDetection{
			Id:        aName,
			Version:   aVersion,
			Detection: det,
		})

		info := &pb.AnalyticDetectionInfo{
			AnalyticId: aID,
			Detection:  det,
		}

		if tID == uuid.Nil {
			// No task ID: this means we're done.
			info.Stage = pb.DetectionStage_DETECTION_STAGE_FINISHED
			if det.GetStatus().GetCode() == int32(codes.OK) {
				info.Status = pb.DetectionStatus_DETECTION_STATUS_SUCCESS
			} else {
				info.Status = pb.DetectionStatus_DETECTION_STATUS_FAILURE
			}
		} else {
			// We have a task ID - we could be in any of the non-finished situations, and
			// can only really figure out where we are by requesting the referenced task.
			queue := queueConf.AnalyticInbox(aID)
			tasks, err := s.eqc.Tasks(ctx, queue, entroq.WithTaskID(tID))
			if err != nil {
				if isGRPCFatal(err) {
					log.Fatalf("Fatal rpc error in modify: %v", err)
				}
				return nil, errors.Wrap(err, "get task corresponding to detection")
			}
			if len(tasks) == 0 {
				// We already know we aren't finished, so no corresponding task means it has not yet been created.
				info.Stage = pb.DetectionStage_DETECTION_STAGE_NONE
			} else {
				t := tasks[0]
				if t.Version == 0 || t.At.Before(time.Now()) {
					info.Stage = pb.DetectionStage_DETECTION_STAGE_QUEUED
				} else {
					info.Stage = pb.DetectionStage_DETECTION_STAGE_CLAIMED
				}
			}
		}

		resp.AnalyticInfo = append(resp.AnalyticInfo, info)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "get detection rows")
	}

	if req.WantFused {
		if verbose {
			log.Printf("Fusion requested")
		}

		// TODO: fix this query to be more future-proof.
		// Details: task_id has a one-to-one correspondence with analytic_ids
		// sets, so if things are unfinished, this falls apart. Also, we might
		// want to return everything all the time. Unclear on that point.
		query := `
			SELECT
				fuser_id,
				task_id,
				(ARRAY_AGG(fusion ORDER BY ARRAY_LENGTH(analytic_ids, 1) DESC))[1] AS best_fusion
			FROM
				fusion
			WHERE
				finished IS NOT NULL AND
				coalesce(fusion->'status'->>'code', '0') = '0' AND
				detection_id = $1
			GROUP BY (fuser_id, task_id)
		`
		rows, err := s.pgdb.QueryContext(ctx, query, req.Id)
		if err != nil {
			return nil, errors.Wrapf(err, "query detection %q", req.Id)
		}
		defer rows.Close()

		for rows.Next() {
			resp.HasFused = true
			var (
				fID    string
				tID    uuid.UUID
				fusion string
			)
			if err := rows.Scan(&fID, &tID, &fusion); err != nil {
				return nil, errors.Wrap(err, "get detection info row scan")
			}

			fus, err := analytic.UnmarshalFusionJSON(fusion)
			if err != nil {
				return nil, errors.Wrap(err, "GetDetectionInfo unmarshal fusion")
			}
			info := &pb.FuserFusionInfo{
				FuserId: fID,
				Fusion:  fus,
			}

			if tID == uuid.Nil {
				// No task ID: this means we're done.
				info.Stage = pb.DetectionStage_DETECTION_STAGE_FINISHED
				if fus.GetStatus().GetCode() == int32(codes.OK) {
					info.Status = pb.DetectionStatus_DETECTION_STATUS_SUCCESS
				} else {
					info.Status = pb.DetectionStatus_DETECTION_STATUS_FAILURE
				}
			} else {
				// We have a task ID - we could be in any of the non-finished situations, and
				// can only really figure out where we are by requesting the referenced task.
				queue := queueConf.FuserInbox(fID)
				tasks, err := s.eqc.Tasks(ctx, queue, entroq.WithTaskID(tID))
				if err != nil {
					if isGRPCFatal(err) {
						log.Fatalf("Fatal rpc error in modify: %v", err)
					}
					return nil, errors.Wrap(err, "get task corresponding to detection")
				}
				if len(tasks) == 0 {
					// We already know we aren't finished, so no corresponding task means it has not yet been created.
					info.Stage = pb.DetectionStage_DETECTION_STAGE_NONE
				} else {
					t := tasks[0]
					if t.Version == 0 || t.At.Before(time.Now()) {
						info.Stage = pb.DetectionStage_DETECTION_STAGE_QUEUED
					} else {
						info.Stage = pb.DetectionStage_DETECTION_STAGE_CLAIMED
					}
				}
			}

			resp.FusionInfo = append(resp.FusionInfo, info)
		}

	}

	// Add top-level resource information.
	if err := annotateDetectionInfoResources(resp); err != nil {
		return nil, errors.Wrap(err, "annotate detection info resources")
	}

	return resp, nil
}

// DeleteDetection cleans up the database given a particular ID, including removing any tasks that are outstanding.
func (s *apiService) DeleteDetection(ctx context.Context, req *pb.DeleteDetectionRequest) (*pb.Empty, error) {
	// We don't worry about deleting tasks - if the detection ID is gone from the database, the task will fail and get deleted.
	queries := []string{
		"DELETE FROM detection WHERE detection_id = $1",
		"DELETE FROM detectionTag WHERE detection_id = $1",
		"DELETE FROM fusion WHERE detection_id = $1",
		"DELETE FROM fusionTag WHERE detection_id = $1",
	}
	for _, query := range queries {
		if _, err := s.pgdb.ExecContext(ctx, query, req.DetectionId); err != nil {
			return nil, errors.Wrap(err, "delete detection - query")
		}
	}

	return new(pb.Empty), nil
}

type pageToken struct {
	Offset int32 `json:"offset"`
}

func reqTagJSON(req *pb.DetectionListRequest) (string, error) {
	if len(req.Tags) == 0 {
		return "", nil
	}
	jTags, err := json.Marshal(req.GetTags())
	if err != nil {
		return "", errors.Wrap(err, "request tag -> JSON")
	}
	return string(jTags), nil
}

func correctDetectionMap(m map[string]interface{}) {
	lookFor := ""
	for k := range m {
		if strings.HasSuffix(k, "Req") {
			lookFor = strings.TrimSuffix(k, "Req")
			break
		}
	}

	if lookFor == "" {
		// Just create an empty request - this should never happen, but if it does, we'll make it serializable.
		m["imgManipReq"] = make(map[string]interface{})
		lookFor = "imgManip"
	}

	if m[lookFor] == nil {
		m[lookFor] = make(map[string]interface{})
	}
}

func mustCorrectDetectionInfoJSON(j string) string {
	val := make(map[string]interface{})
	if err := json.Unmarshal([]byte(j), &val); err != nil {
		log.Fatalf("Unmarshal to correct missing oneof failed: %v", err)
	}

	for _, ai := range val["analyticInfo"].([]interface{}) {
		correctDetectionMap(ai.(map[string]interface{})["detection"].(map[string]interface{}))
	}

	fixed, err := json.Marshal(val)
	if err != nil {
		log.Fatalf("Marshal fixed JSON failed: %v", err)
	}
	return string(fixed)
}

func (s *apiService) detectionCount(ctx context.Context, req *pb.DetectionListRequest) (int, error) {
	jsonTags, err := reqTagJSON(req)
	if err != nil {
		return 0, errors.Wrap(err, "detection list")
	}

	var params []interface{}

	query := `
	SELECT COUNT(ids.detection_id)
	FROM (
		SELECT
			detection_id
		FROM detection
		WHERE true
	`

	if len(req.DetectionIds) != 0 {
		query += fmt.Sprintf(" AND detection_id = any($%d)", len(params)+1)
		params = append(params, pq.StringArray(req.DetectionIds))
	}

	if ss, es := req.GetDateRange().GetStartSecondsUtc(), req.GetDateRange().GetEndSecondsUtc(); ss != 0 || es != 0 {
		if ss != 0 {
			query += fmt.Sprintf(" AND extract(EPOCH FROM created) >= $%d", len(params)+1)
			params = append(params, ss)
		}
		if es != 0 {
			query += fmt.Sprintf(" AND extract(EPOCH FROM created) <= $%d", len(params)+1)
			params = append(params, es)
		}
	}

	query += `
		GROUP BY detection_id
	) AS ids
	LEFT JOIN detectionTag AS dt
	USING (detection_id)`

	if jsonTags != "" {
		query += fmt.Sprintf(`
	WHERE (dt.tags || dt.user_tags) @> $%d`, len(params)+1)
		params = append(params, jsonTags)
	}

	if verbose {
		log.Printf("Detection count:\n\tquery: %v\n\tparams: %v", query, params)
	}

	var count int
	if err := s.pgdb.QueryRowContext(ctx, query, params...).Scan(&count); err != nil {
		return 0, errors.Wrap(err, "get detection count")
	}
	return count, nil
}

func DecodePageToken(tok string) (*pageToken, error) {
	jBytes, err := base64.StdEncoding.DecodeString(tok)
	if err != nil {
		return nil, errors.Wrapf(err, "decode page token %v", tok)
	}
	pt := new(pageToken)
	if err := json.Unmarshal(jBytes, pt); err != nil {
		return nil, errors.Wrap(err, "unmarshal page token")
	}
	return pt, nil
}

func (t pageToken) Encode() string {
	tBytes, err := json.Marshal(t)
	if err != nil {
		log.Fatalf("Error marshaling page token: %+v", t)
	}
	return base64.StdEncoding.EncodeToString(tBytes)
}

// annotateDetectionInfoResources adds top-level req_resources to an otherwise full DetectionInfo.
// If there are no analytics in it, this is a no-op.
func annotateDetectionInfoResources(dInfo *pb.DetectionInfo) error {
	if len(dInfo.GetAnalyticInfo()) != 0 {
		resources, err := analytic.FindDetectionRequestResources(dInfo.GetAnalyticInfo()[0].GetDetection())
		if err != nil {
			return errors.Wrap(err, "find resources in analytic list")
		}
		dInfo.ReqResources = resources
	}
	return nil
}

func (s *apiService) detectionList(ctx context.Context, req *pb.DetectionListRequest) (*pb.DetectionList, error) {
	pageSize := req.PageSize
	if pageSize < 1 {
		pageSize = DefaultPageSize
	}
	if pageSize > MaxPageSize {
		log.Printf("Page size %d requested, only allowing %d", pageSize, MaxPageSize)
		pageSize = MaxPageSize
	}
	var offset int32
	if req.PageToken != "" {
		tok, err := DecodePageToken(req.PageToken)
		if err != nil {
			return nil, errors.Wrap(err, "detection list")
		}
		offset = tok.Offset
	}

	jTags, err := reqTagJSON(req)
	if err != nil {
		return nil, errors.Wrap(err, "detection list")
	}

	log.Printf("Request filters: ids=%q, tags=%v, query=%q", req.DetectionIds, jTags, req.MetaQuery)

	var params []interface{}
	query := `
	SELECT
		jsonb_build_object(
			'id', d.detection_id,
			'tags', d.tags,
			'userTags', d.user_tags,
			'meta', d.meta,
			'hasFused', d.fused_score IS NOT NULL,
			'fusedScore', d.fused_score,
			'analyticsTotal', count(*),
			'analyticsFinished', count(*) filter(WHERE finished IS NOT NULL),
			'analyticInfo', array_agg(jsonb_build_object(
				'analyticId', detection.analytic_id,
				'detection', detection.detection
			))
		) AS detection_info
	FROM
		(SELECT
			tagged.detection_id AS detection_id,
			tagged.tags AS tags,
			tagged.user_tags AS user_tags,
			tagged.meta AS meta,
			tagged.fused_score AS fused_score
		FROM
			(SELECT
				ids.detection_id AS detection_id,
				dt.tags AS tags,
				dt.user_tags AS user_tags,
				dt.meta AS meta,
				CASE
					WHEN
						fused.best_fusion IS NOT NULL AND
						fused.best_fusion ? 'imgManip' AND
						coalesce(fused.best_fusion->'imgManip'->>'optOut', 'OPT_OUT_NONE') NOT IN ('OPT_OUT_ALL', 'OPT_OUT_DETECTION')
					  	THEN (fused.best_fusion->'imgManip'->>'score')::float
					WHEN
						fused.best_fusion IS NOT NULL AND
						fused.best_fusion ? 'vidManip' AND
						NOT (coalesce(fused.best_fusion->'vidManip'->'optOut', '[]'::jsonb) ? 'VIDEO_OPT_OUT_DETECTION')
						THEN (fused.best_fusion->'vidManip'->>'score')::float
					ELSE NULL
				END AS fused_score
			FROM
				(SELECT
					detection_id
				FROM detection
				WHERE true
	`

	if len(req.DetectionIds) != 0 {
		query += fmt.Sprintf(" AND detection_id = any($%d)", len(params)+1)
		params = append(params, pq.StringArray(req.DetectionIds))
	}

	if ss, es := req.GetDateRange().GetStartSecondsUtc(), req.GetDateRange().GetEndSecondsUtc(); ss != 0 || es != 0 {
		if ss != 0 {
			query += fmt.Sprintf(" AND extract(EPOCH FROM created) >= $%d", len(params)+1)
			params = append(params, ss)
		}
		if es != 0 {
			query += fmt.Sprintf(" AND extract(EPOCH FROM created) <= $%d", len(params)+1)
			params = append(params, es)
		}
	}

	query += fmt.Sprintf(`
				GROUP BY detection_id
				) AS ids
			LEFT JOIN
				detectionTag AS dt
			USING (detection_id)
			LEFT JOIN
				(SELECT
					detection_id,
					(array_agg(fusion ORDER BY array_length(analytic_ids, 1) DESC))[1] AS best_fusion
				FROM fusion
				WHERE
					finished IS NOT NULL AND
					coalesce(fusion->'status'->>'code', '0') = '0' AND
					fuser_id = $%d
				GROUP BY (detection_id)
				) AS fused
			USING (detection_id)
			WHERE true
	`, len(params)+1)
	params = append(params, req.GetFuserId())

	if jTags != "" {
		query += fmt.Sprintf(" AND (dt.tags || dt.user_tags) @> $%d", len(params)+1)
		params = append(params, jTags)
	}

	if req.MetaQuery != "" {
		query += fmt.Sprintf(" AND to_tsvector('english', dt.meta) @@ to_tsquery('english', $%d)", len(params)+1)
		params = append(params, req.MetaQuery)
	}

	var orderClause = ""
	if len(req.OrderBy) != 0 {
		query += " ORDER BY "
		orderClause += " ORDER BY "
		for i, ob := range req.OrderBy {
			if i > 0 {
				query += ","
				orderClause += ","
			}
			switch ob.Key {
			case pb.SortKey_SCORE:
				if req.FuserId == "" {
					return nil, errors.New("no fuser ID specified for fusion sorting")
				}
				query += " fused_score"
				orderClause += " d.fused_score"
			case pb.SortKey_META:
				query += fmt.Sprintf(" meta->>$%d", len(params)+1)
				orderClause += fmt.Sprintf(" d.meta->>$%d", len(params)+1)
				params = append(params, ob.MetaKey)
			case pb.SortKey_META_QUERY:
				query += fmt.Sprintf(" ts_rank_cd(ts_vector('english', meta), ts_query('english', $%d", len(params)+1)
				orderClause += fmt.Sprintf(" ts_rank_cd(ts_vector('english', d.meta), ts_query('english', $%d", len(params)+1)
				params = append(params, ob.MetaKey)
			default:
				return nil, errors.Errorf("unsupported sort option %v", ob.Key)
			}
			if ob.IsAsc {
				query += " ASC"
				orderClause += " ASC"
			} else {
				query += " DESC"
				orderClause += " DESC"
			}
			query += " NULLS LAST"
			orderClause += " NULLS LAST"
		}
	}

	query += `
		) AS tagged
	`

	if req.FusionThresholdType != pb.FusionThresholdType_FUSION_NO_THRESHOLD {
		switch req.FusionThresholdType {
		case pb.FusionThresholdType_FUSION_LT_THRESHOLD:
			query += fmt.Sprintf(" WHERE fused_score < $%d", len(params)+1)
		case pb.FusionThresholdType_FUSION_GT_THRESHOLD:
			query += fmt.Sprintf(" WHERE fused_score > $%d", len(params)+1)
		default:
			return nil, errors.Errorf("unexpected threshold type: %v", req.FusionThresholdType)
		}
		params = append(params, req.FusionThresholdValue)
	}

	query += `
	) AS d
	INNER JOIN detection
	USING(detection_id)
	GROUP BY (d.detection_id, d.tags, d.user_tags, d.meta, d.fused_score)
	` + orderClause + fmt.Sprintf(" OFFSET $%d LIMIT $%d", len(params)+1, len(params)+2)
	params = append(params, offset, pageSize)

	if verbose {
		log.Printf("Detection list\n\tquery: %v\n\tparams: %v", query, params)
	}

	rows, err := s.pgdb.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, errors.Wrap(err, "detection list query")
	}
	defer rows.Close()

	resp := new(pb.DetectionList)
	for rows.Next() {
		var dListJSON string
		if err := rows.Scan(&dListJSON); err != nil {
			return nil, errors.Wrap(err, "scan error")
		}

		dInfo := new(pb.DetectionInfo)
		if err := jsonpb.UnmarshalString(mustCorrectDetectionInfoJSON(dListJSON), dInfo); err != nil {
			return nil, errors.Wrap(err, "unmarshal detection info from list")
		}

		// If we have any analytics, ensure that the input resources are represented at the top level for convenience.
		// Also remove them all if we're in minimal output mode.
		if err := annotateDetectionInfoResources(dInfo); err != nil {
			return nil, errors.Wrap(err, "annotate detection resources")
		}

		// Wipe out all analytic info if verbosity is set to minimum.
		if req.Verbosity == pb.DetectionListVerbosity_DETECTION_VERBOSITY_MINIMAL {
			dInfo.AnalyticInfo = nil
		}

		resp.Detections = append(resp.Detections, dInfo)
	}
	if err := rows.Err(); err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrap(err, "detection row iter")
	}
	if pageSize != 0 && int32(len(resp.Detections)) == pageSize {
		resp.PageToken = pageToken{Offset: offset + int32(len(resp.Detections))}.Encode()
	}
	return resp, nil
}

// GetDetectionList returns a list of all known detection IDs by default.  If
// tags are present in the request, only returns detections matching all
// specified tags simultaneously.
func (s *apiService) GetDetectionList(ctx context.Context, req *pb.DetectionListRequest) (*pb.DetectionList, error) {
	var (
		resp  *pb.DetectionList
		total int
	)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() (err error) {
		resp, err = s.detectionList(ctx, req)
		return err
	})

	g.Go(func() (err error) {
		total, err = s.detectionCount(ctx, req)
		return err
	})

	if err := g.Wait(); err != nil {
		return nil, errors.Wrap(err, "get detection list")
	}

	resp.Total = int32(total)
	return resp, nil
}

var kvReplacer = strings.NewReplacer("\\", `\\`, "=", `\=`)

func fmtKeyVal(key, val string) string {
	return kvReplacer.Replace(key) + "=" + kvReplacer.Replace(val)
}

// GetDetectionTagInfo gets high-level information about all detection tags in the system.
func (s *apiService) GetDetectionTagInfo(ctx context.Context, req *pb.DetectionTagInfoRequest) (*pb.DetectionTagInfo, error) {
	requireJSON := ""
	if len(req.RequireTags) != 0 {
		js, err := json.Marshal(req.RequireTags)
		if err != nil {
			return nil, errors.Wrap(err, "marshal required tags")
		}
		requireJSON = string(js)
	}

	var params []interface{}
	query := `
	SELECT type, (tag).key, (tag).value, c
	FROM (
		SELECT 'sys' AS type, JSONB_EACH_TEXT(tags) AS tag, COUNT(*) AS c
		FROM detectionTag
		WHERE true
	`

	if requireJSON != "" {
		query += fmt.Sprintf(" AND (tags || user_tags) @> $%d ", len(params)+1)
		params = append(params, requireJSON)
	}

	query += `
		GROUP BY type, tag
		UNION ALL
		SELECT 'user' AS type, JSONB_EACH_TEXT(user_tags) AS tag, COUNT(*) AS c
		FROM detectionTag
		WHERE true
	`

	if requireJSON != "" {
		query += fmt.Sprintf(" AND (tags || user_tags) @> $%d ", len(params)+1)
		params = append(params, requireJSON)
	}

	query += `
		GROUP BY type, tag
	) AS tt
	WHERE true
	`

	for _, p := range req.GetSkipPrefixes() {
		query += fmt.Sprintf(" AND (tag).key NOT LIKE $%d ", len(params)+1)
		params = append(params, p+"%")
	}

	rows, err := s.pgdb.QueryContext(ctx, query, params...)
	if err != nil {
		return nil, errors.Wrap(err, "query tags")
	}
	defer rows.Close()

	tags := make(map[string]int32)
	userTags := make(map[string]int32)
	for rows.Next() {
		var (
			typ, key, val string
			count         int32
		)
		if err := rows.Scan(&typ, &key, &val, &count); err != nil {
			return nil, errors.Wrap(err, "tag scan")
		}

		tag := fmtKeyVal(key, val)

		switch typ {
		case "sys":
			tags[tag] = count
		case "user":
			userTags[tag] = count
		default:
			return nil, errors.Errorf("Unknown tag type: %q", typ)
		}
	}
	if err := rows.Err(); err != nil && err != sql.ErrNoRows {
		return nil, errors.Wrap(err, "tag row iter")
	}

	return &pb.DetectionTagInfo{
		TagCounts:     tags,
		UserTagCounts: userTags,
	}, nil
}

// GetAnalyticMeta provides a list of analytics and associated metadata
func (s *apiService) GetAnalyticMeta(context.Context, *pb.Empty) (*pb.AnalyticList, error) {
	s.Lock()
	defer s.Unlock()

	if s.analyticList != nil {
		return s.analyticList, nil
	}

	log.Printf("Loading analytic config file %q", analyticConfigPath)
	var err error
	if s.analyticList, err = configlist.LoadAnalyticConfig(analyticConfigPath); err != nil {
		return nil, errors.Wrapf(err, "load analytic list from file %q", analyticConfigPath)
	}
	return s.analyticList, nil
}

// UpdateDetectionTags attempts to update user tags, either replacing or
// merging them. Returns the full detection info corresponding to the given ID.
func (s *apiService) UpdateDetectionTags(ctx context.Context, req *pb.UpdateDetectionTagsRequest) (*pb.DetectionInfo, error) {
	var (
		jTag []byte
		err  error
	)

	if req.GetTags() == nil {
		jTag = []byte("{}")
	} else {
		if jTag, err = json.Marshal(req.GetTags()); err != nil {
			return nil, errors.Wrap(err, "marshal tags")
		}
	}

	var params []interface{}
	query := "UPDATE detectionTag SET user_tags = "
	if req.Replace {
		query += fmt.Sprintf("$%d", len(params)+1)
		params = append(params, string(jTag))
	} else {
		query += fmt.Sprintf("(user_tags || $%d)::jsonb", len(params)+1)
		params = append(params, string(jTag))
		if len(req.DeleteTags) != 0 {
			var args []string
			for _, t := range req.DeleteTags {
				args = append(args, fmt.Sprintf("$%d", len(params)+1))
				params = append(params, t)
			}
			query += fmt.Sprintf(" - ARRAY[%s]", strings.Join(args, ", "))
		}
	}

	query += fmt.Sprintf(" WHERE detection_id = $%d", len(params)+1)
	params = append(params, req.GetDetectionId())

	if verbose {
		log.Printf("query: %q", query)
		log.Printf("param: %v", params)
	}

	if _, err := s.pgdb.ExecContext(ctx, query, params...); err != nil {
		return nil, errors.Wrapf(err, "update tags, query=%q, params=%v", query, params)
	}

	return s.GetDetectionInfo(ctx, &pb.DetectionInfoRequest{Id: req.GetDetectionId()})
}

func entroqClient(ctx context.Context, addr string) (*entroq.EntroQ, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	eqc, err := entroq.New(ctx, grpcbackend.Opener(addr, grpcbackend.WithInsecure(), grpcbackend.WithBlock()))
	if err != nil {
		return nil, errors.Wrap(err, "eq open")
	}
	return eqc, nil
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "analyticworkflow",
	Short: "An HTTP service that exports a simple API for sending analytic processing requests.",
	Long: `This is a service that exports an API for starting and managing
	analytic processing requests. It also serves and allows for image uploads.`,
	Run: func(cmd *cobra.Command, args []string) {
		g, ctx := errgroup.WithContext(context.Background())

		pgdb, err := pg.Open(ctx, pgAddr,
			pg.WithUsername(pgUser),
			pg.WithPassword(pgPass),
			pg.WithDB(pgDB),
			pg.WithDialAttempts(10))

		if err != nil {
			log.Fatalf("failed to open DB: %v", err)
		}
		defer pgdb.Close()

		if err := ensureTables(ctx, pgdb); err != nil {
			log.Fatalf("Failure ensuring existence of tables: %v", err)
		}

		// Start the server.
		g.Go(func() (err error) {
			defer log.Printf("Ending API server: %v", err)
			log.Printf("Starting API server %q <- %q", queueConf.DetectionInbox(), listen)

			eqc, err := entroqClient(ctx, eqAddr)
			if err != nil {
				return errors.Wrap(err, "failed to connect to entroq")
			}
			defer eqc.Close()

			svc := newAPIService(eqc, pgdb)

			lis, err := net.Listen("tcp", listen)
			if err != nil {
				log.Fatalf("could not listen on %q: %v", listen, err)
			}

			serverOpts, err := serverConf.GRPCServerOptions()
			if err != nil {
				log.Fatalf("Failed to create server options: %v", err)
			}
			s := grpc.NewServer(serverOpts...)
			pb.RegisterPipelineServer(s, svc)
			hpb.RegisterHealthServer(s, health.NewServer())

			// In the background, check for a canceled group context.
			// If the group is canceled, this stops the server (running below)
			// and sets the outer function's returned error value, which will
			// cause other goroutines in this group to exit, as well.
			go func() {
				<-ctx.Done()
				err = ctx.Err()
				s.Stop()
			}()

			if err := s.Serve(lis); err != nil {
				return err
			}
			return // Naked - returns whatever the function's err already is.
		})

		// Start the fanout workers.
		for i := 0; i < numFanout; i++ {
			i := i
			g.Go(func() (err error) {
				defer log.Printf("Ending fanout worker (%d): %v", i, err)
				log.Printf("Starting fanout worker (%d) %q <- %q", i, queueConf.AnalyticInbox("<analytic_id>"), queueConf.DetectionInbox())
				eqc, err := entroqClient(ctx, eqAddr)
				if err != nil {
					return errors.Wrap(err, "failed to connect to entroq")
				}
				defer eqc.Close()

				var opts []fanoutworker.Option
				if skipIDOutDirSuffix {
					opts = append(opts, fanoutworker.OmittingAnalyticIDInOutDir())
				}

				return fanoutworker.New(eqc, pgdb, queueConf.DetectionInbox(), queueConf.AnalyticInboxFmt(), opts...).Run(ctx)
			})

			g.Go(func() (err error) {
				defer log.Printf("Ending fusion fanout worker (%d): %v", i, err)
				log.Printf("Starting fusion fanout worker (%d) %q <- %q", i, queueConf.FuserInbox("<fuser_id>"), queueConf.FusionInbox())
				eqc, err := entroqClient(ctx, eqAddr)
				if err != nil {
					return errors.Wrap(err, "failed to connect to entroq")
				}
				defer eqc.Close()

				var opts []fusionfanoutworker.Option
				if skipIDOutDirSuffix {
					opts = append(opts, fusionfanoutworker.OmittingFuserIDInOutDir())
				}

				return fusionfanoutworker.New(eqc, pgdb, queueConf.FusionInbox(), queueConf.FuserInboxFmt(), opts...).Run(ctx)
			})

		}

		// Start the outbox consumers.
		for i := 0; i < numOutbox; i++ {
			i := i
			g.Go(func() (err error) {
				defer log.Printf("Ending outbox worker (%d): %v", i, err)
				log.Printf("Starting outbox worker (%d) %q <- %q", i, pgAddr, queueConf.DetectionOutbox())
				eqc, err := entroqClient(ctx, eqAddr)
				if err != nil {
					return errors.Wrap(err, "failed to connect to entroq")
				}
				defer eqc.Close()

				return outboxworker.New(eqc, pgdb, queueConf.DetectionOutbox(), queueConf.FusionGenInbox()).Run(ctx)
			})

			// Start the fusion outbox consumers.
			g.Go(func() (err error) {
				defer log.Printf("Ending fusion outbox worker (%d): %v", i, err)
				log.Printf("Starting fusion outbox worker (%d) %q <- %q", i, pgAddr, queueConf.FusionOutbox())
				eqc, err := entroqClient(ctx, eqAddr)
				if err != nil {
					return errors.Wrap(err, "failed to connect to entroq")
				}
				defer eqc.Close()

				return outboxworker.NewFusion(eqc, pgdb, queueConf.FusionOutbox()).Run(ctx)
			})
		}

		// Start the fusionGen consumers.
		g.Go(func() (err error) {
			defer log.Print("Ending fusion task generation worker ", err)
			log.Printf("Starting fusion task generation worker %q <- %q", pgAddr, queueConf.FusionOutbox())
			eqc, err := entroqClient(ctx, eqAddr)
			if err != nil {
				return errors.Wrap(err, "failed to connect to entroq")
			}
			defer eqc.Close()

			return fusiongenworker.New(eqc, pgdb, queueConf.FusionGenInbox(), queueConf.FusionInbox(), queueConf.FusionOutbox(), fusDir).Run(ctx)
		})

		g.Wait()
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Printf("Error executing service: %v", err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	pflags := rootCmd.PersistentFlags()

	pflags.StringVar(&cfgFile, "config", "", "config file (default is $HOME/.config/analyticworkflow.yaml)")
	pflags.StringVar(&fusDir, "fusionOutDir", "/tmp", "base directory for fusion outputs")
	pflags.IntVar(&numFanout, "num_fanout", 1, "Number of fanout workers")
	pflags.IntVar(&numOutbox, "num_outbox", 1, "Number of outbox workers")
	pflags.StringVar(&listen, "listen", ":50051", "Address to listen for gRPC requests on.")
	pflags.StringVar(&pgAddr, "pgaddr", "localhost:5432", "Address of PostgreSQL database instance.")
	pflags.StringVar(&pgDB, "pgdb", "postgres", "Name of database within specified PostgreSQL instance.")
	pflags.StringVar(&pgUser, "pguser", "postgres", "PostgreSQL user name.")
	pflags.StringVar(&pgPass, "pgpass", "postgres", "PostgreSQL password.")
	pflags.StringVar(&eqAddr, "eqaddr", ":37706", "Address of EntroQ gRPC service.")
	pflags.StringVar(&queueConf.detectionInbox, "inbox", DefaultDetectionInbox, "Entrypoint task queue for all detection requests. The fanout worker picks up tasks from this queue.")
	pflags.StringVar(&queueConf.detectionOutbox, "outbox", DefaultDetectionOutbox, "Queue name for finished analytic worker tasks. The analytic worker is instructed to leave things here, and the outbox worker picks them up.")
	pflags.StringVar(&queueConf.analyticInboxFmt, "idqfmt", DefaultAnalyticInboxFmt, "Format string for creating a queue name from an analytic ID. Must contain exactly one %s, and the analytic ID will be automatically url path-escaped before being inserted in that location.")

	pflags.StringVar(&queueConf.prefix, "queue_prefix", DefaultGlobalQueuePrefix, "If present, will be prepended to every task queue name.")

	pflags.StringVar(&queueConf.fusionInbox, "fuse_inbox", DefaultFusionInbox, "Entrypoint task queue for all fusion requests. The fanout worker picks up tasks from this queue.")
	pflags.StringVar(&queueConf.fusionGenInbox, "fuse_gen_inbox", DefaultFusionGenInbox, "Entrypoint task queue for all intermediate fusion generation requests, which are used to create fusion requests from completed analytic results")
	pflags.StringVar(&queueConf.fusionOutbox, "fuse_outbox", DefaultFusionOutbox, "Queue name for finished fusion worker tasks. The worker is instructed to leave things here, and the outbox worker picks them up.")
	pflags.StringVar(&queueConf.fuserInboxFmt, "fuse_idqfmt", DefaultFuserInboxFmt, "Format string for creating a queue name from an fuser ID. Must contain exactly one %s, and the analytic ID will be automatically url path-escaped before being inserted in that location.")
	pflags.StringVar(&analyticConfigPath, "analytic_config", "", "Configuration file providing a list of analytics and associated metadata.")
	pflags.BoolVar(&skipIDOutDirSuffix, "skip_outdir_analytic_suffix", false, "When moving from inbox to individual analytic inboxes, do not append the analytic ID to the request out_dir. Setting this to true allows you to havec a constant mount path for every analytic (e.g., all are /mnt/medifor/output, instead of each being /mnt/medifor/output/analytic_id, even if they are mounted from different places)")

	pflags.BoolVar(&serverConf.TLS, "tls", false, "Serve only TLS traffic on the listen port, instead of insecure. If specified, CA and cert files must also be provided.")
	pflags.BoolVar(&serverConf.VerifyClient, "tls_verify_client", false, "Verify client against client CAs when --tls is on.")
	pflags.StringVar(&serverConf.ServerKeyFile, "tls_server_key_file", "", "PEM file containing server private key.")
	pflags.StringVar(&serverConf.ServerCertFile, "tls_server_cert_file", "", "PEM file containing server certificate.")
	pflags.StringVar(&serverConf.ClientCAFile, "tls_client_ca_file", "", "File containing CAs to check client cert against.")

	pflags.BoolVarP(&verbose, "verbose", "V", false, "Verbose logging.")

	viper.BindPFlag("pgaddr", pflags.Lookup("pgaddr"))
	viper.BindPFlag("pgdb", pflags.Lookup("pgdb"))
	viper.BindPFlag("pguser", pflags.Lookup("pguser"))
	viper.BindPFlag("pgpass", pflags.Lookup("pgpass"))
	viper.BindPFlag("eqaddr", pflags.Lookup("eqaddr"))
	viper.BindPFlag("fuseaddr", pflags.Lookup("fuseaddr"))
	viper.BindPFlag("listen", pflags.Lookup("listen"))
	viper.BindPFlag("inbox", pflags.Lookup("inbox"))
	viper.BindPFlag("outbox", pflags.Lookup("outbox"))
	viper.BindPFlag("idqfmt", pflags.Lookup("idqfmt"))
	viper.BindPFlag("fuse_inbox", pflags.Lookup("fuse_inbox"))
	viper.BindPFlag("fuse_gen_inbox", pflags.Lookup("fuse_gen_inbox"))
	viper.BindPFlag("fuse_outbox", pflags.Lookup("fuse_outbox"))
	viper.BindPFlag("fuse_idqfmt", pflags.Lookup("fuse_idqfmt"))
	viper.BindPFlag("num_fanout", pflags.Lookup("num_fanout"))
	viper.BindPFlag("num_outbox", pflags.Lookup("num_outbox"))
	viper.BindPFlag("analytic_config", pflags.Lookup("analytic_config"))
	viper.BindPFlag("tls", pflags.Lookup("tls"))
	viper.BindPFlag("tls_verify_client", pflags.Lookup("tls_verify_client"))
	viper.BindPFlag("tls_server_key_file", pflags.Lookup("tls_server_key_file"))
	viper.BindPFlag("tls_server_cert_file", pflags.Lookup("tls_server_cert_file"))
	viper.BindPFlag("tls_client_ca_file", pflags.Lookup("tls_client_ca_file"))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" {
		// Use config file from the flag.
		viper.SetConfigFile(cfgFile)
	} else {
		// Find home directory.
		home, err := homedir.Dir()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		// Search config in home directory with name ".analyticworkflow" (without extension).
		viper.AddConfigPath(home)
		viper.SetConfigName(".config/analyticworkflow")
	}

	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

//  sets up a database by trying to create appropriate tables, etc.
func ensureTables(ctx context.Context, pgdb *sql.DB) error {
	_, err := pgdb.ExecContext(ctx,
		`CREATE TABLE IF NOT EXISTS detection (
			detection_id TEXT NOT NULL CHECK (detection_id <> ''),
			analytic_id TEXT NOT NULL CHECK (analytic_id <> ''),
			task_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
			created TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			finished TIMESTAMP WITH TIME ZONE,
			probe_hash TEXT NOT NULL DEFAULT '',
			donor_hash TEXT NOT NULL DEFAULT '',
			detection JSONB,
			PRIMARY KEY (detection_id, analytic_id)
		);
		COMMENT ON TABLE detection IS 'Detections per probe and analytic.';
		COMMENT ON COLUMN detection.detection_id IS 'Unique ID of this detection request, shared among analytics.';
		COMMENT ON COLUMN detection.analytic_id IS 'ID of the analytic performing this detection.';
		COMMENT ON COLUMN detection.task_id IS 'ID of the workflow task (not including version).';
		COMMENT ON COLUMN detection.detection IS 'Full request/response information for a particular detection.';
		CREATE INDEX IF NOT EXISTS detectionByAnalytic ON detection (analytic_id);
		CREATE INDEX IF NOT EXISTS detectionByID ON detection (detection_id);
		CREATE INDEX IF NOT EXISTS detectionByDetection ON detection USING gin (detection);

		CREATE TABLE IF NOT EXISTS detectionTag (
			detection_id TEXT NOT NULL CHECK (detection_id <> ''),
			tags JSONB NOT NULL DEFAULT '{}',
			user_tags JSONB NOT NULL DEFAULT '{}',
			meta JSONB NOT NULL DEFAULT '{}',
			PRIMARY KEY (detection_id)
		);
		COMMENT ON TABLE detectionTag IS 'Assigns tags to detections. Many-to-many.';
		CREATE INDEX IF NOT EXISTS detectionTagByID ON detectionTag (detection_id);
		CREATE INDEX IF NOT EXISTS detectionTagBySysAndUserTags ON detectionTag USING gin ((tags || user_tags));
		CREATE INDEX IF NOT EXISTS detectionTagByMetaQuery ON detectionTag USING gin (to_tsvector('english', meta));

		CREATE TABLE IF NOT EXISTS fusion (
			detection_id TEXT NOT NULL CHECK (detection_id <> ''),
			fuser_id TEXT NOT NULL CHECK (fuser_id <> ''),
			analytic_ids TEXT [],
			task_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
			created TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
			finished TIMESTAMP WITH TIME ZONE,
			fusion JSONB,
			PRIMARY KEY (detection_id, fuser_id, analytic_ids)
		);

		CREATE TABLE IF NOT EXISTS fusionTag (
			detection_id TEXT NOT NULL CHECK (detection_id <> ''),
			tags JSONB,
			PRIMARY KEY (detection_id)
		);
		CREATE INDEX IF NOT EXISTS fusionTagByID ON fusionTag (detection_id);
		COMMENT ON TABLE fusionTag IS 'Assigns tags to detections. Many-to-many.';

		COMMENT ON TABLE fusion IS 'Fusions per detection ID and fuser.';
		COMMENT ON COLUMN fusion.detection_id IS 'The ID of the detection for which fusion was done across analytics.';
		COMMENT ON COLUMN fusion.fuser_id IS 'The ID of the fuser algorithm used.';
		COMMENT ON COLUMN fusion.analytic_ids IS 'A list of all analytic IDs that participated in this fusion. Should be normalized on input (sort, deduplicate).';
		COMMENT ON COLUMN fusion.task_id IS 'The ID of the task used to compute this fusion.';
		COMMENT ON COLUMN fusion.fusion IS 'The fusion request/response round trip object.';
		CREATE INDEX IF NOT EXISTS fusionByDetectionID ON fusion (detection_id);
		CREATE INDEX IF NOT EXISTS fusionByFinished ON fusion (finished);
		CREATE INDEX IF NOT EXISTS fusionByFuser ON fusion (fuser_id);
		CREATE INDEX IF NOT EXISTS fusionByFusion ON fusion USING gin (fusion);
		CREATE INDEX IF NOT EXISTS detectionTagByTag ON detectionTag USING gin (tags);
		CREATE INDEX IF NOT EXISTS detectionTagByUserTag ON detectionTag USING gin (user_tags);
		CREATE INDEX IF NOT EXISTS detectionTagByMeta ON detectionTag USING gin (meta);
		COMMENT ON TABLE detectionTag IS 'Assigns tags (json) to detections.';
		`,
	)

	return errors.Wrap(err, "ensure table")
}
