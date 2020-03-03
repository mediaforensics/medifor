package medifor

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"google.golang.org/grpc"

	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
	"github.com/mediaforensics/medifor/pkg/protoutil"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// FusionClient is a client library for speaking to MediFor Analytic Fusion Services over gRPC
type FusionClient struct {
	conn     *grpc.ClientConn
	health   healthpb.HealthClient
	analytic pb.FuserClient

	inputTranslator  PathTranslator
	outputTranslator PathTranslator

	fusionInputTranslator PathTranslator
}

// NewFusionClient creates a new medfifor fusion client and dials the host
func NewFusionClient(ctx context.Context, hostPort string, opts ...Option) (*FusionClient, error) {
	mo := &mfcOpts{
		inputTranslator:       DefaultPathTranslator,
		fusionInputTranslator: DefaultPathTranslator,
		outputTranslator:      DefaultPathTranslator,
	}
	for _, opt := range opts {
		if err := opt(mo); err != nil {
			return nil, errors.Wrap(err, "fusion option error")
		}
	}
	conn, err := grpc.DialContext(ctx, hostPort, mo.GRPCDialOptions()...)
	if err != nil {
		return nil, errors.Wrap(err, "dial fusion analytic")
	}
	c := &FusionClient{
		conn:                  conn,
		health:                healthpb.NewHealthClient(conn),
		analytic:              pb.NewFuserClient(conn),
		inputTranslator:       mo.inputTranslator,
		fusionInputTranslator: mo.fusionInputTranslator,
		outputTranslator:      mo.outputTranslator,
	}

	if mo.requireHealthy {
		resp, err := c.Health(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "health check for fusion creation")
		}
		if resp.Status != healthpb.HealthCheckResponse_SERVING {
			return nil, errors.Errorf("status SERVING required for client creation, but got %s", resp.Status)
		}
	}

	return c, nil
}

// Close closes the underlying connection to the host.
func (c *FusionClient) Close() error {
	return c.conn.Close()
}

// Health sends a health check request and returns a response proto.
func (c *FusionClient) Health(ctx context.Context) (*healthpb.HealthCheckResponse, error) {
	return c.health.Check(ctx, new(healthpb.HealthCheckRequest))
}

func mustStripReq(req Request) Request {
	req = proto.Clone(req).(Request)
	if err := protoutil.SetMessageStringField(req, "RequestId", ""); err != nil {
		log.Fatalf("Error stripping request_id: %v", err)
	}

	if err := protoutil.SetMessageStringField(req, "OutDir", ""); err != nil {
		log.Fatalf("Error stripping out_dir: %v", err)
	}
	return req
}

// FuseImageManipulation calls fusion with the given request.
func (c *FusionClient) FuseImageManipulation(ctx context.Context, req *pb.FuseImageManipulationRequest) (*pb.ImageManipulation, error) {
	if err := EnsureIDAndOutput(req); err != nil {
		return nil, errors.Wrap(err, "fuse image manipulation")
	}
	req = proto.Clone(req).(*pb.FuseImageManipulationRequest)
	if err := protoutil.RewriteOutDir(req, c.outputTranslator.Translate); err != nil {
		return nil, errors.Wrap(err, "translate fuse imgmanip outdir")
	}
	if err := protoutil.RewriteURIs(req.ImgManipReq, c.inputTranslator.Translate); err != nil {
		return nil, errors.Wrap(err, "translate fuse imgmanip inputs")
	}
	if err := protoutil.RewriteURIs(req.ImgManip, c.fusionInputTranslator.Translate); err != nil {
		return nil, errors.Wrap(err, "translate fusion analytic outputs imgmanip")
	}
	resp, err := c.analytic.FuseImageManipulation(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "rpc fuse image manipulation")
	}
	if err := protoutil.RewriteURIs(resp, c.outputTranslator.Untranslate); err != nil {
		return nil, errors.Wrap(err, "fuse imgmanip untranslate")
	}
	return resp, nil
}

// FuseVideoManipulation calls fusion with the given request.
func (c *FusionClient) FuseVideoManipulation(ctx context.Context, req *pb.FuseVideoManipulationRequest) (*pb.VideoManipulation, error) {
	if err := EnsureIDAndOutput(req); err != nil {
		return nil, errors.Wrap(err, "fuse video manipulation")
	}
	req = proto.Clone(req).(*pb.FuseVideoManipulationRequest)
	if err := protoutil.RewriteOutDir(req, c.outputTranslator.Translate); err != nil {
		return nil, errors.Wrap(err, "translate fuse vidmanip outdir")
	}
	if err := protoutil.RewriteURIs(req.VidManipReq, c.inputTranslator.Translate); err != nil {
		return nil, errors.Wrap(err, "translate fuse vidmanip inputs")
	}
	if err := protoutil.RewriteURIs(req.VidManip, c.fusionInputTranslator.Translate); err != nil {
		return nil, errors.Wrap(err, "translate fusion analytic outputs vidmanip")
	}
	resp, err := c.analytic.FuseVideoManipulation(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "rpc fuse video manipulation")
	}
	if err := protoutil.RewriteURIs(resp, c.outputTranslator.Untranslate); err != nil {
		return nil, errors.Wrap(err, "fuse vidmanip untranslate")
	}
	return resp, nil
}

// ParseAnalyticID separates the ID into name and version components if possible.
func ParseAnalyticID(id string) (name, version string) {
	parts := strings.SplitN(id, "_", 2)
	name = parts[0]
	if len(parts) > 1 {
		version = parts[1]
	}
	return name, version
}

// IDVersionFromDetection gets the analytic ID and version from an AnnotatedDetection proto.
func IDVersionFromDetection(d *pb.AnnotatedDetection) (id, version string, err error) {
	id = d.GetId()
	version = d.GetVersion()
	if id == "" {
		if d.GetAnalyticId() == "" {
			return "", "", errors.New("no ID/Version or analytic ID in proto")
		}
		id, version = ParseAnalyticID(d.GetAnalyticId())
	}
	return id, version, nil
}

// FusedScore pulls the score from a Fusion object.
func FusedScore(f *pb.Fusion) (float64, error) {
	switch val := f.GetResponse().(type) {
	case *pb.Fusion_ImgManip:
		return f.GetImgManip().GetScore(), nil
	case *pb.Fusion_VidManip:
		return f.GetVidManip().GetScore(), nil
	case *pb.Fusion_ImgSplice:
		return f.GetImgSplice().GetLink().GetScore(), nil
	default:
		return 0, errors.Errorf("unknown type for fusion score: %T", val)
	}
}

// FuseDetections calls the endpoint corresponding to the request type in the
// first of a slice of AnnotatedDetection protos. All protos are assumed to
// have the same underlying request, and their responses are converted into
// inputs for fusion.
func (c *FusionClient) FuseDetections(ctx context.Context, ds []*pb.AnnotatedDetection) (fusion *pb.Fusion, err error) {
	if len(ds) == 0 {
		return nil, errors.New("no detections given for fusion")
	}

	firstDet := ds[0].GetDetection()

	fusion = new(pb.Fusion)
	switch val := firstDet.GetRequest().(type) {
	case *pb.Detection_ImgManipReq:
		fusion.Request = &pb.Fusion_ImgManipReq{
			ImgManipReq: &pb.FuseImageManipulationRequest{
				ImgManipReq: mustStripReq(firstDet.GetImgManipReq()).(*pb.ImageManipulationRequest),
			},
		}
		freq := fusion.GetImgManipReq()
		for _, d := range ds {
			// TODO: check that none of the requests differ materially from the first one.
			id, ver, err := IDVersionFromDetection(d)
			if err != nil {
				return nil, errors.Wrap(err, "imgmanip detection id/version")
			}
			freq.ImgManip = append(freq.ImgManip, &pb.AnnotatedImageManipulation{
				Id:      id,
				Version: ver,
				Data:    d.GetDetection().GetImgManip(),
			})
		}
	case *pb.Detection_VidManipReq:
		fusion.Request = &pb.Fusion_VidManipReq{
			VidManipReq: &pb.FuseVideoManipulationRequest{
				VidManipReq: mustStripReq(firstDet.GetVidManipReq()).(*pb.VideoManipulationRequest),
			},
		}
		freq := fusion.GetVidManipReq()
		for _, d := range ds {
			// TODO: check that none of the requests differ materially from the first one.
			id, ver, err := IDVersionFromDetection(d)
			if err != nil {
				return nil, errors.Wrap(err, "vidmanip detection id/version")
			}
			freq.VidManip = append(freq.VidManip, &pb.AnnotatedVideoManipulation{
				Id:      id,
				Version: ver,
				Data:    d.GetDetection().GetVidManip(),
			})
		}
	default:
		return nil, errors.Errorf("unknown fusion request type %T", val)
	}

	if err := c.Fuse(ctx, fusion); err != nil {
		return nil, errors.Wrap(err, "fuse detections")
	}
	return fusion, nil
}

// Fuse calls the endpoint corresponding to the request type in the given
// Fusion, fills in the response and status on reply. The request ID can be
// left off, and will be assigned if missing.
func (c *FusionClient) Fuse(ctx context.Context, fusion *pb.Fusion) (err error) {
	fusion.StartTimeMillis = time.Now().UnixNano() / 1e6
	defer func() {
		fusion.Status = status.Convert(errors.Cause(err)).Proto()
		fusion.EndTimeMillis = time.Now().UnixNano() / 1e6
	}()

	switch val := fusion.GetRequest().(type) {
	case *pb.Fusion_ImgManipReq:
		resp, err := c.FuseImageManipulation(ctx, fusion.GetImgManipReq())
		fusion.Response = &pb.Fusion_ImgManip{ImgManip: resp}
		return errors.Wrap(err, "fuse imgmanip")
	case *pb.Fusion_VidManipReq:
		resp, err := c.FuseVideoManipulation(ctx, fusion.GetVidManipReq())
		fusion.Response = &pb.Fusion_VidManip{VidManip: resp}
		return errors.Wrap(err, "fuse vidmanip")
	default:
		return errors.Errorf("unknown fusion request type %T", val)
	}
}
