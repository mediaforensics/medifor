// Package medifor contains a basic client library for dialing and communicating with MediFor services.
package medifor

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/mediaforensics/medifor/pkg/fileutil"
	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
	"github.com/mediaforensics/medifor/pkg/protoutil"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// Option changes aspects of client creation.
type Option func(o *mfcOpts) error

type mfcOpts struct {
	secure         bool
	requireHealthy bool
	dialOpts       []grpc.DialOption

	inputTranslator  PathTranslator
	outputTranslator PathTranslator

	fusionInputTranslator PathTranslator
}

// GRPCDialOptions returns a full set of grpc dial options.
func (o *mfcOpts) GRPCDialOptions() []grpc.DialOption {
	opts := make([]grpc.DialOption, len(o.dialOpts))
	copy(opts, o.dialOpts)
	if !o.secure {
		opts = append(opts, grpc.WithInsecure())
	}
	return opts
}

// WithHealthy sets an option that causes client creation to fail if the
// service does not immediately send a "SERVING" health response. Otherwise
// creation may succeed where requests fail.
func WithHealthy() Option {
	return func(o *mfcOpts) error {
		o.requireHealthy = true
		return nil
	}
}

// WithWaitForBackend blocks the dialer until the backend is ready.
// Do not use for buffered connections, just for production.
func WithWaitForBackend() Option {
	return WithDial(grpc.WithBlock())
}

// WithMTLS creates an option for MTLS authentication, using a server CA file,
// a client cert PEM, and a client private key file.
func WithMTLS(caCertPath, clientCertPath, clientKeyPath string) Option {
	return func(o *mfcOpts) error {
		caBytes, err := ioutil.ReadFile(caCertPath)
		if err != nil {
			return errors.Wrap(err, "failed to load CA PEM bytes")
		}
		caPool, err := x509.SystemCertPool()
		if ok := caPool.AppendCertsFromPEM(caBytes); !ok {
			return errors.New("failed to add CA to pool")
		}

		clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
		if err != nil {
			return errors.Wrap(err, "failed to load Server cert")
		}

		config := &tls.Config{
			ClientAuth:   tls.RequireAndVerifyClientCert,
			Certificates: []tls.Certificate{clientCert},
			RootCAs:      caPool,
		}
		o.secure = true
		return WithDial(grpc.WithTransportCredentials(credentials.NewTLS(config)))(o)
	}
}

// WithDial sets a particular gRPC dial option when creating the client. For example,
// to use an insecure connection, you can do the following:
//
// 	client, err := NewClient(ctx, hostPort, WithDial(grpc.WithInsecure()))
//
// Note, the above is the default, so this is merely an example of something you *could* do.
func WithDial(opts ...grpc.DialOption) Option {
	return func(o *mfcOpts) error {
		o.dialOpts = append(o.dialOpts, opts...)
		return nil
	}
}

// PathTranslator is used to translate one path to another and back.
type PathTranslator interface {
	Translate(string) (string, error)
	Untranslate(string) (string, error)
}

type defaultTranslator struct{}

func (defaultTranslator) Translate(path string) (string, error) {
	return path, nil
}

func (defaultTranslator) Untranslate(path string) (string, error) {
	return path, nil
}

// DefaultPathTranslator provides no-op Translate and Untranslate methods.
var DefaultPathTranslator = defaultTranslator{}

// WithInputTranslator sets the input URI translator to be the given function.
// When present, this will translate any URI in a request proto by passing it
// through the provided translation function before sending the request.
// Operates on URIs in requests. Defaults to DefaultPathTranslator.
func WithInputTranslator(t PathTranslator) Option {
	return func(o *mfcOpts) error {
		o.inputTranslator = t
		return nil
	}
}

// WithOutputTranslator sets the output directory and response URI translation
// mechanism to the given function. Similar to WithInputTranslator, but
// operates on the output directory in requests, and on all URIs in responses.
func WithOutputTranslator(t PathTranslator) Option {
	return func(o *mfcOpts) error {
		o.outputTranslator = t
		return nil
	}
}

// WithFusionInputTranslator sets the input URI translator for fusion inputs
// (which are all analytic outputs, not the original source data). Works like
// WithInputTranslator, but affects only the analytic output portion of a
// fusion request.
func WithFusionInputTranslator(t PathTranslator) Option {
	return func(o *mfcOpts) error {
		o.fusionInputTranslator = t
		return nil
	}
}

// Client is a client library for speaking to MediFor services (e.g., analytic indicators) over gRPC.
type Client struct {
	conn     *grpc.ClientConn
	health   healthpb.HealthClient
	analytic pb.AnalyticClient

	inputTranslator  PathTranslator
	outputTranslator PathTranslator
}

// NewClient creates a new medifor client and dials the host.
func NewClient(ctx context.Context, hostPort string, opts ...Option) (*Client, error) {
	mo := &mfcOpts{
		inputTranslator:  DefaultPathTranslator,
		outputTranslator: DefaultPathTranslator,
	}
	for _, opt := range opts {
		if err := opt(mo); err != nil {
			return nil, errors.Wrap(err, "option error")
		}
	}
	conn, err := grpc.DialContext(ctx, hostPort, mo.GRPCDialOptions()...)
	if err != nil {
		return nil, errors.Wrap(err, "dial analytic")
	}
	c := &Client{
		conn:             conn,
		health:           healthpb.NewHealthClient(conn),
		analytic:         pb.NewAnalyticClient(conn),
		inputTranslator:  mo.inputTranslator,
		outputTranslator: mo.outputTranslator,
	}

	if mo.requireHealthy {
		resp, err := c.Health(ctx)
		if err != nil {
			return nil, errors.Wrap(err, "health check for creation")
		}
		if resp.Status != healthpb.HealthCheckResponse_SERVING {
			return nil, errors.Errorf("status SERVING required for client creation, but got %s", resp.Status)
		}
	}

	return c, nil
}

// Close closes the underlying connection to the host.
func (c *Client) Close() error {
	return c.conn.Close()
}

// CheckDetectionScore returns an error if a response score is not in [0, 1], otherwise nil.
// A nil detection object results in no error, since that would produce a default 0 score.
func CheckDetectionScore(detection *pb.Detection) error {
	if detection == nil {
		return nil
	}
	var score float64
	switch val := detection.GetResponse().(type) {
	case *pb.Detection_ImgManip:
		score = detection.GetImgManip().GetScore()
	case *pb.Detection_VidManip:
		score = detection.GetVidManip().GetScore()
	case *pb.Detection_ImgSplice:
		score = detection.GetImgSplice().GetLink().GetScore()
	case *pb.Detection_ImgCamMatch:
		score = detection.GetImgCamMatch().GetScore()
	default:
		return errors.Errorf("Unknown response type %T", val)
	}
	if score < 0 || score > 1 {
		return errors.Errorf("score %v not in [0, 1]", score)
	}
	return nil

}

// NewDetectionReq creates a detection object from a given request proto.
// It doesn't do anything special with the request (like set its ID). That
// should be done by the caller.
func NewDetectionReq(req Request) (*pb.Detection, error) {
	detection := new(pb.Detection)
	// Note that we fill in empty responses, too, so that JSON serialization has a chance at working.
	switch val := req.(type) {
	case *pb.ImageManipulationRequest:
		detection.Request = &pb.Detection_ImgManipReq{ImgManipReq: val}
		detection.Response = &pb.Detection_ImgManip{ImgManip: new(pb.ImageManipulation)}
	case *pb.VideoManipulationRequest:
		detection.Request = &pb.Detection_VidManipReq{VidManipReq: val}
		detection.Response = &pb.Detection_VidManip{VidManip: new(pb.VideoManipulation)}
	case *pb.ImageSpliceRequest:
		detection.Request = &pb.Detection_ImgSpliceReq{ImgSpliceReq: val}
		detection.Response = &pb.Detection_ImgSplice{ImgSplice: new(pb.ImageSplice)}
	case *pb.ImageCameraMatchRequest:
		detection.Request = &pb.Detection_ImgCamMatchReq{ImgCamMatchReq: val}
		detection.Response = &pb.Detection_ImgCamMatch{ImgCamMatch: new(pb.ImageCameraMatch)}
	case *pb.VideoCameraMatchRequest:
		detection.Request = &pb.Detection_VidCamMatchReq{VidCamMatchReq: val}
		detection.Response = &pb.Detection_VidCamMatch{VidCamMatch: new(pb.VideoCameraMatch)}
	case *pb.ImageCamerasRequest:
		detection.Request = &pb.Detection_ImgCamsReq{ImgCamsReq: val}
		detection.Response = &pb.Detection_ImgCams{ImgCams: new(pb.ImageCameras)}
	default:
		return nil, errors.Errorf("invalid request type %T", req)
	}
	return detection, nil
}

// ManipOpt is an option passed to image or video manipulation detection
// request creation functions for optional values.
type ManipOpt func(*manipConfig)

type manipConfig struct {
	outputPrefix string
	deviceID     string
}

// WithOutputPrefix sets the output prefix to the given value. If not called,
// the default is used, based on operating systsem temporary directory.
func WithOutputPrefix(out string) ManipOpt {
	return func(c *manipConfig) {
		c.outputPrefix = out
	}
}

// WithDeviceID sets the device ID on a manipulation request. Default is empty.
// Not all manipulation types will make use of this.
func WithDeviceID(id string) ManipOpt {
	return func(c *manipConfig) {
		c.deviceID = id
	}
}

func configManip(opts ...ManipOpt) *manipConfig {
	cfg := &manipConfig{
		outputPrefix: filepath.Join(os.TempDir(), fmt.Sprint(os.Getpid())),
	}
	for _, o := range opts {
		o(cfg)
	}
	return cfg
}

// NewImageManipulationRequest creates a new ImageManipulationRequest from the
// given image path. It determines the mime type from the file extension. It
// creates a unique request ID and appends it to the output directory prefix.
// If the output prefix is unspecified in the options, it is set to a location
// based on the OS temp directory.
func NewImageManipulationRequest(imgPath string, opts ...ManipOpt) *pb.ImageManipulationRequest {
	cfg := configManip(opts...)
	req := &pb.ImageManipulationRequest{
		OutDir: cfg.outputPrefix,
		Image: &pb.Resource{
			Uri:  imgPath,
			Type: fileutil.MimeTypeOrGeneric(imgPath),
		},
		HpDeviceId: cfg.deviceID,
	}
	if err := EnsureIDAndOutput(req); err != nil {
		log.Fatalf("Unexpected error setting RequestId and OutDir on request proto: %v", err)
	}
	return req
}

// NewVideoManipulationRequest creates a new VideoManipulationRequest from the
// given video path. Works similarly to NewImageManipulationRequest.
func NewVideoManipulationRequest(vidPath string, opts ...ManipOpt) *pb.VideoManipulationRequest {
	cfg := configManip(opts...)
	req := &pb.VideoManipulationRequest{
		OutDir: cfg.outputPrefix,
		Video: &pb.Resource{
			Uri:  vidPath,
			Type: fileutil.MimeTypeOrGeneric(vidPath),
		},
		HpDeviceId: cfg.deviceID,
	}
	if err := EnsureIDAndOutput(req); err != nil {
		log.Fatalf("Unexpected error setting RequestId and OutDir on request proto: %v", err)
	}
	return req
}

// NewImageSpliceRequest creates a new ImageSpliceRequest from the given probe
// and donor image paths. Works similarly to NewImageManipulationRequest.
func NewImageSpliceRequest(donorPath, probePath string, opts ...ManipOpt) *pb.ImageSpliceRequest {
	cfg := configManip(opts...)
	req := &pb.ImageSpliceRequest{
		OutDir: cfg.outputPrefix,
		ProbeImage: &pb.Resource{
			Uri:  probePath,
			Type: fileutil.MimeTypeOrGeneric(probePath),
		},
		DonorImage: &pb.Resource{
			Uri:  donorPath,
			Type: fileutil.MimeTypeOrGeneric(donorPath),
		},
	}
	if err := EnsureIDAndOutput(req); err != nil {
		log.Fatalf("Unexpected error setting RequestId and OutDir on request proto: %v", err)
	}
	return req
}

// NewImageCameraMatchRequest creates a new ImageCameraMatchRequest from the
// given probe and camera ID.
func NewImageCameraMatchRequest(probePath string, opts ...ManipOpt) *pb.ImageCameraMatchRequest {
	cfg := configManip(opts...)
	req := &pb.ImageCameraMatchRequest{
		OutDir: cfg.outputPrefix,
		Image: &pb.Resource{
			Uri:  probePath,
			Type: fileutil.MimeTypeOrGeneric(probePath),
		},
		CameraId: cfg.deviceID,
	}
	if err := EnsureIDAndOutput(req); err != nil {
		log.Fatalf("Unexpected error setting RequestId and OutDir on request proto: %v", err)
	}
	return req
}

// NewImageCamerasRequest creates a new ImageCamerasRequest from the
// given probe and camera ID.
func NewImageCamerasRequest(probePath string, opts ...ManipOpt) *pb.ImageCamerasRequest {
	cfg := configManip(opts...)
	req := &pb.ImageCamerasRequest{
		OutDir: cfg.outputPrefix,
		Image: &pb.Resource{
			Uri:  probePath,
			Type: fileutil.MimeTypeOrGeneric(probePath),
		},
	}
	if err := EnsureIDAndOutput(req); err != nil {
		log.Fatalf("Unexpected error setting RequestId and OutDir on request proto: %v", err)
	}
	return req
}

// Health sends a health check request and returns a response proto.
func (c *Client) Health(ctx context.Context) (*healthpb.HealthCheckResponse, error) {
	return c.health.Check(ctx, new(healthpb.HealthCheckRequest))
}

// DetectImageManipulation calls the corresponding endpoint, with file name translation, if registered.
// A request ID will be assigned if not present.
func (c *Client) DetectImageManipulation(ctx context.Context, req *pb.ImageManipulationRequest) (*pb.ImageManipulation, error) {
	// Note that we fill in empty responses, too, so that JSON serialization has a chance at working.
	empty := new(pb.ImageManipulation)
	if err := EnsureIDAndOutput(req); err != nil {
		return empty, errors.Wrap(err, "detect image manipulation")
	}
	req = proto.Clone(req).(*pb.ImageManipulationRequest)
	if err := protoutil.RewriteOutDir(req, c.outputTranslator.Translate); err != nil {
		return empty, errors.Wrap(err, "translate image out paths")
	}
	if err := protoutil.RewriteURIs(req, c.inputTranslator.Translate); err != nil {
		return empty, errors.Wrap(err, "translate image input paths")
	}
	resp, err := c.analytic.DetectImageManipulation(ctx, req)
	if err != nil {
		return empty, errors.Wrap(err, "rpc detect image manipulation")
	}
	if err := protoutil.RewriteURIs(resp, c.outputTranslator.Untranslate); err != nil {
		return empty, errors.Wrap(err, "image manip untranslate")
	}
	return resp, nil
}

// DetectVideoManipulation calls the corresponding endpoint, with file name translation, if registered.
// A request ID will be assigned if not present.
func (c *Client) DetectVideoManipulation(ctx context.Context, req *pb.VideoManipulationRequest) (*pb.VideoManipulation, error) {
	// Note that we fill in empty responses, too, so that JSON serialization has a chance at working.
	empty := new(pb.VideoManipulation)
	if err := EnsureIDAndOutput(req); err != nil {
		return empty, errors.Wrap(err, "detect video manipulation")
	}
	req = proto.Clone(req).(*pb.VideoManipulationRequest)
	if err := protoutil.RewriteOutDir(req, c.outputTranslator.Translate); err != nil {
		return empty, errors.Wrap(err, "translate video out paths")
	}
	if err := protoutil.RewriteURIs(req, c.inputTranslator.Translate); err != nil {
		return empty, errors.Wrap(err, "translate video input paths")
	}
	resp, err := c.analytic.DetectVideoManipulation(ctx, req)
	if err != nil {
		return empty, errors.Wrap(err, "rpc detect video manipulation")
	}
	if err := protoutil.RewriteURIs(resp, c.outputTranslator.Untranslate); err != nil {
		return empty, errors.Wrap(err, "video manip untranslate")
	}
	return resp, nil
}

// DetectImageSplice calls the corresponding endpoint, with file name translation, if registered.
// A request ID will be assigned if not present.
func (c *Client) DetectImageSplice(ctx context.Context, req *pb.ImageSpliceRequest) (*pb.ImageSplice, error) {
	// Note that we fill in empty responses, too, so that JSON serialization has a chance at working.
	empty := new(pb.ImageSplice)
	if err := EnsureIDAndOutput(req); err != nil {
		return empty, errors.Wrap(err, "detect image splice")
	}
	req = proto.Clone(req).(*pb.ImageSpliceRequest)
	if err := protoutil.RewriteOutDir(req, c.outputTranslator.Translate); err != nil {
		return empty, errors.Wrap(err, "translate splice out paths")
	}
	if err := protoutil.RewriteURIs(req, c.inputTranslator.Translate); err != nil {
		return empty, errors.Wrap(err, "translate splice input paths")
	}
	resp, err := c.analytic.DetectImageSplice(ctx, req)
	if err != nil {
		return empty, errors.Wrap(err, "rpc detect image splice")
	}
	if err := protoutil.RewriteURIs(resp, c.outputTranslator.Untranslate); err != nil {
		return empty, errors.Wrap(err, "splice untranslate")
	}
	return resp, nil
}

// DetectImageCameraMatch calls the corresponding endpoint, with file name translation, if registered.
// A request ID will be assigned if not present.
func (c *Client) DetectImageCameraMatch(ctx context.Context, req *pb.ImageCameraMatchRequest) (*pb.ImageCameraMatch, error) {
	// Note that we fill in empty responses, too, so that JSON serialization has a chance at working.
	empty := new(pb.ImageCameraMatch)
	if err := EnsureIDAndOutput(req); err != nil {
		return empty, errors.Wrap(err, "detect image camera match")
	}
	req = proto.Clone(req).(*pb.ImageCameraMatchRequest)
	if err := protoutil.RewriteOutDir(req, c.outputTranslator.Translate); err != nil {
		return empty, errors.Wrap(err, "translate img match out paths")
	}
	if err := protoutil.RewriteURIs(req, c.inputTranslator.Translate); err != nil {
		return empty, errors.Wrap(err, "translate img match input paths")
	}
	resp, err := c.analytic.DetectImageCameraMatch(ctx, req)
	if err != nil {
		return empty, errors.Wrap(err, "rpc image camera match")
	}
	if err := protoutil.RewriteURIs(resp, c.outputTranslator.Untranslate); err != nil {
		return empty, errors.Wrap(err, "img match untranslate")
	}
	return resp, nil
}

// DetectVideoCameraMatch calls the corresponding endpoint, with file name translation, if registered.
// A request ID will be assigned if not present.
func (c *Client) DetectVideoCameraMatch(ctx context.Context, req *pb.VideoCameraMatchRequest) (*pb.VideoCameraMatch, error) {
	// Note that we fill in empty responses, too, so that JSON serialization has a chance at working.
	empty := new(pb.VideoCameraMatch)
	if err := EnsureIDAndOutput(req); err != nil {
		return empty, errors.Wrap(err, "detect vid camera match")
	}
	req = proto.Clone(req).(*pb.VideoCameraMatchRequest)
	if err := protoutil.RewriteOutDir(req, c.outputTranslator.Translate); err != nil {
		return empty, errors.Wrap(err, "translate vid match out paths")
	}
	if err := protoutil.RewriteURIs(req, c.inputTranslator.Translate); err != nil {
		return empty, errors.Wrap(err, "translate vid match input paths")
	}
	resp, err := c.analytic.DetectVideoCameraMatch(ctx, req)
	if err != nil {
		return empty, errors.Wrap(err, "rpc vid camera match")
	}
	if err := protoutil.RewriteURIs(resp, c.outputTranslator.Untranslate); err != nil {
		return empty, errors.Wrap(err, "vid match untranslate")
	}
	return resp, nil
}

// DetectImageCameras calls the corresponding endpoint, with file name translation, if registered.
// A request ID will be assigned if not present.
func (c *Client) DetectImageCameras(ctx context.Context, req *pb.ImageCamerasRequest) (*pb.ImageCameras, error) {
	// Note that we fill in empty responses, too, so that JSON serialization has a chance at working.
	empty := new(pb.ImageCameras)
	if err := EnsureIDAndOutput(req); err != nil {
		return empty, errors.Wrap(err, "detect image camera match")
	}
	req = proto.Clone(req).(*pb.ImageCamerasRequest)
	if err := protoutil.RewriteOutDir(req, c.outputTranslator.Translate); err != nil {
		return empty, errors.Wrap(err, "translate img cams out paths")
	}
	if err := protoutil.RewriteURIs(req, c.inputTranslator.Translate); err != nil {
		return empty, errors.Wrap(err, "translate img cams input paths")
	}
	resp, err := c.analytic.DetectImageCameras(ctx, req)
	if err != nil {
		return empty, errors.Wrap(err, "rpc img cams")
	}
	if err := protoutil.RewriteURIs(resp, c.outputTranslator.Untranslate); err != nil {
		return empty, errors.Wrap(err, "img cams untranslate")
	}
	return resp, nil
}

// DetectReq creates a Detection from a given request, and calls Detect on it before returning it.
// It is valid to have a non-nil detection *and* a non-nil error. The detection, in that case, will
// contain the relevant endpoint error information in its status field, and the
// error will contain the call stack leading to it.
func (c *Client) DetectReq(ctx context.Context, req Request) (*pb.Detection, error) {
	detection, err := NewDetectionReq(req)
	if err != nil {
		return nil, errors.Wrap(err, "detect req create")
	}
	if err := c.Detect(ctx, detection); err != nil {
		// Note that an error in Detect still leaves us with a valid detection object,
		// so we return that, but we *don't* return an error, because a
		// "detection" error in status is valid.
		return detection, errors.Wrap(err, "detect req call")
	}
	return detection, nil
}

// Detect calls the endpoint corresponding to the request type in the given
// Detection, and fills in the response and status on reply. The request ID can
// be omitted. One will be assigned if not present.
func (c *Client) Detect(ctx context.Context, detection *pb.Detection) (err error) {
	detection.StartTimeMillis = time.Now().UnixNano() / 1e6
	defer func() {
		detection.Status = status.Convert(errors.Cause(err)).Proto()
		detection.EndTimeMillis = time.Now().UnixNano() / 1e6
	}()

	// Note that we expect these calls to return a valid, empty proto as the response even on error.
	switch val := detection.GetRequest().(type) {
	case *pb.Detection_ImgManipReq:
		resp, err := c.DetectImageManipulation(ctx, detection.GetImgManipReq())
		detection.Response = &pb.Detection_ImgManip{ImgManip: resp}
		return errors.Wrap(err, "detect img manip")
	case *pb.Detection_VidManipReq:
		resp, err := c.DetectVideoManipulation(ctx, detection.GetVidManipReq())
		detection.Response = &pb.Detection_VidManip{VidManip: resp}
		return errors.Wrap(err, "detect vid manip")
	case *pb.Detection_ImgSpliceReq:
		resp, err := c.DetectImageSplice(ctx, detection.GetImgSpliceReq())
		detection.Response = &pb.Detection_ImgSplice{ImgSplice: resp}
		return errors.Wrap(err, "detect img splice")
	case *pb.Detection_ImgCamMatchReq:
		resp, err := c.DetectImageCameraMatch(ctx, detection.GetImgCamMatchReq())
		detection.Response = &pb.Detection_ImgCamMatch{ImgCamMatch: resp}
		return errors.Wrap(err, "detect img camera match")
	case *pb.Detection_VidCamMatchReq:
		resp, err := c.DetectVideoCameraMatch(ctx, detection.GetVidCamMatchReq())
		detection.Response = &pb.Detection_VidCamMatch{VidCamMatch: resp}
		return errors.Wrap(err, "detect vid camera match")
	case *pb.Detection_ImgCamsReq:
		resp, err := c.DetectImageCameras(ctx, detection.GetImgCamsReq())
		detection.Response = &pb.Detection_ImgCams{ImgCams: resp}
		return errors.Wrap(err, "detect img cams")
	default:
		return errors.Errorf("unknown detection request type %T", val)
	}
}

// MultiClient is a client manager that handles multiple clients at once, allowing one-request-at-a-time mode against a set of clients.
type MultiClient struct {
	sync.Mutex

	size int

	clients chan *Client
	busy    sync.WaitGroup
}

// NewMultiClient creates a new multiclient, where every client uses the same options.
func NewMultiClient(ctx context.Context, hostPorts []string, opts ...Option) (client *MultiClient, err error) {
	c := &MultiClient{
		clients: make(chan *Client, len(hostPorts)),
		size:    len(hostPorts),
	}
	closeIfAnyErrors := func(c *Client) {
		if err != nil {
			c.Close()
		}
	}
	for _, hp := range hostPorts {
		client, err := NewClient(ctx, hp, opts...)
		if err != nil {
			return nil, errors.Wrapf(err, "client failure for %q", hp)
		}
		// Clean up previously-created connections if there is a problem.
		defer closeIfAnyErrors(client)
		c.clients <- client
	}
	return c, nil
}

// Len returns the total number of clients.
func (c *MultiClient) Len() int {
	return c.size
}

// get gets a single client if available. Blocks until one becomes available.
func (c *MultiClient) get(ctx context.Context) (*Client, error) {
	c.Lock()
	defer c.Unlock()

	select {
	case client, ok := <-c.clients:
		if !ok {
			return nil, errors.New("can't get clients - channel closed")
		}
		c.busy.Add(1)
		return client, nil
	case <-ctx.Done():
		return nil, errors.Wrap(ctx.Err(), "context canceled getting client")
	}
}

// put puts a single client back.
func (c *MultiClient) put(client *Client) {
	c.clients <- client
	c.busy.Done()
}

// CallWithClient calls the specific function with the next available client, if any.
func (c *MultiClient) CallWithClient(ctx context.Context, f func(context.Context, *Client) error) error {
	client, err := c.get(ctx)
	if err != nil {
		return errors.Wrap(err, "get client for call")
	}
	defer c.put(client)

	if err := f(ctx, client); err != nil {
		return errors.Wrap(err, "call with client")
	}
	return nil
}

// Close closes all underlying clients. Waits for all outstanding CallWithClient calls to finish.
func (c *MultiClient) Close() error {
	// Disallow any new starts, and ensure the loop will terminate below.
	c.Lock()
	c.busy.Wait()
	close(c.clients)
	c.Unlock()

	var err error
	for client := range c.clients {
		if cerr := client.Close(); err != nil {
			if err == nil {
				err = cerr
			}
		}
	}
	return errors.Wrap(err, "close multiclient")
}

// Request captures protos that look like they might be analytic requests because of particular fields.
type Request interface {
	proto.Message
	GetOutDir() string
	GetRequestId() string
}

// EnsureIDAndOutput checks that a request object contains a RequestID, and
// that its output directory ends with that ID. If either of those things is
// not true, it remedies that fact before sending the request.
func EnsureIDAndOutput(req Request) error {
	// Expect a RequestId field and an OutDir field.
	rID, err := protoutil.MessageStringField(req, "RequestId")
	if err != nil {
		return errors.Wrap(err, "get request_id")
	}
	out, err := protoutil.MessageStringField(req, "OutDir")
	if err != nil {
		return errors.Wrap(err, "get out_dir")
	}

	if rID == "" {
		rID = uuid.New().String()
		if err := protoutil.SetMessageStringField(req, "RequestId", rID); err != nil {
			return errors.Wrap(err, "set request_id")
		}
	}
	if filepath.Base(out) != rID {
		if out == "" {
			out = os.TempDir()
		}
		out = filepath.Join(out, rID)
		if err := protoutil.SetMessageStringField(req, "OutDir", out); err != nil {
			return errors.Wrap(err, "set out_dir")
		}
	}
	return nil
}
