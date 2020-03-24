// Command analyticproxy starts a gRPC proxy for analytics that intercepts
// requests, pulls files from S3 or similar into a file system visible to the
// analytic, then forwards on the request. When a response occurs, data is
// pulled from that file system and sent where it belongs, and files are
// cleaned up.
package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"path"
	"path/filepath"
	"time"

	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/mediaforensics/medifor/pkg/fileutil"
	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
	"github.com/mediaforensics/medifor/pkg/protoutil"
)

var (
	port    = flag.Int("port", 0, "Listening port")
	backend = flag.String("backend", ":50051", "Hostport of backend analytic implementation that this proxies to.")

	ldapUser = flag.String("ldap_user", "", "Username used to access S3, if needed.")
	ldapPass = flag.String("ldap_pass", "", "Password used to access S3, if needed.")

	localStage = flag.String("local_stage", "/tmp/proxy",
		"Directory for staging files (from the perspective of the proxy process).")
	backendStage = flag.String("backend_stage", "",
		"Directory where staged files are visible from the perspective of the backend. "+
			"Defaults to value of local_stage. If mount points are different, set this value.")
)

// analyticSvc control structure holding a reference to an analytic service
type analyticSvc struct {
	analytic pb.AnalyticClient
	health   healthpb.HealthClient
	stager   *FileStager
}

// GenericRPC message wrapper
type GenericRPC func(ctx context.Context, req proto.Message) (proto.Message, error)

// FileStager control structure for a file movement task
type FileStager struct {
	localStage   string
	backendStage string

	client *fileutil.FileUtil
}

// fileStagerMod credential modification object for FileStager
type fileStagerMod struct {
	user string
	pass string

	backendStage string
}

// FileStagerOption sets options on a file stager when created.
type FileStagerOption func(*fileStagerMod)

// WithS3Credentials sets the LDAP user and password used to access S3 when needed.
func WithS3Credentials(user, pass string) FileStagerOption {
	return func(m *fileStagerMod) {
		m.user = user
		m.pass = pass
	}
}

// WithBackendStage sets the optional backend stage directory. If not set,
// defaults to the same as the local stage (meaning that both the proxy
// and the backend have the same view of the staging file system, i.e., both
// access files using the same absolute paths).
func WithBackendStage(dir string) FileStagerOption {
	return func(m *fileStagerMod) {
		m.backendStage = dir
	}
}

// NewFileStager creates a file stager capable of copying files, including to/from S3.
// Set LDAP user and password using WithLDAP(user, pass) as one of the options.
func NewFileStager(localStage string, opts ...FileStagerOption) (*FileStager, error) {
	mod := &fileStagerMod{
		backendStage: localStage, // Default can be overridden by options.
	}
	for _, opt := range opts {
		opt(mod)
	}

	var fUtilOpts []fileutil.FileUtilOption
	if mod.user != "" {
		fUtilOpts = append(fUtilOpts, fileutil.WithS3Credentials(mod.user, mod.pass))
	}

	futil, err := fileutil.New(fUtilOpts...)
	if err != nil {
		return nil, fmt.Errorf("fileutil client error: %v", err)
	}
	return &FileStager{
		localStage:   localStage,
		backendStage: mod.backendStage,
		client:       futil,
	}, nil
}

// stageFile pulls bytes down from the URI, stashes them in destPath under a
// name derived from the sha1 sum, and returns the basename of the staged file.
func (s *FileStager) stageFile(ctx context.Context, destPath, srcURI string) (string, error) {
	// Find a place to stash things
	tmp, err := ioutil.TempDir(os.TempDir(), "stage")
	if err != nil {
		return "", fmt.Errorf("stage file create tmpdir: %v", err)
	}
	defer os.RemoveAll(tmp)

	// Read the file into tmp so we can hash it.
	base, ext := path.Base(srcURI), path.Ext(srcURI)
	tmpFName := filepath.Join(tmp, base)
	if err := s.client.PullToFile(ctx, tmpFName, srcURI); err != nil {
		return "", fmt.Errorf("stage file read into tmp: %v", err)
	}

	sum, err := fileutil.HashFile(tmpFName)
	if err != nil {
		return "", fmt.Errorf("stage file: %v", err)
	}

	// Copy to its ultimate destination with the hash in the base name.
	destBase := sum + ext
	destFName := filepath.Join(destPath, destBase)

	if err := fileutil.CopyFile(destFName, tmpFName); err != nil {
		return "", fmt.Errorf("stage file local copy: %v", err)
	}
	return destBase, nil
}

// unstageFile pulls bytes from local storage and sends them to the given URI
// prefix + local basename (hopefully intelligently). Returns the basename of the file
// pushed to the given URI prefix.
func (s *FileStager) unstageFile(ctx context.Context, destURIPrefix, localPath string) (string, error) {
	sum, err := fileutil.HashFile(localPath)
	if err != nil {
		return "", fmt.Errorf("unstage hash file %q -> %q: %v", localPath, destURIPrefix, err)
	}

	base := sum + path.Ext(localPath)
	uri := path.Join(destURIPrefix, base)

	if err := s.client.PushFromFile(ctx, uri, localPath); err != nil {
		return "", fmt.Errorf("unstage file %q -> %q: %v", localPath, destURIPrefix, err)
	}
	return base, nil
}

// callWithFileStaging calls the given generic RPC function with the provided
// request. It rewrites diretories referenced in out_dir and stages/unstages
// files referenced in request and response Resource URIs.
//
// The steps it follows are basically these:
// - Rewrite out_dir to be something that the backend can see.
// - Find all request resources, pull them down to a place the backend can see, rewrite accordingly
// - Call the backend with the rewritten request
// - Find all response resources, push them to where they ultimately need to go, rewrite accordingly
// - Return rewritten response
func (s *FileStager) callWithFileStaging(ctx context.Context, f GenericRPC, req proto.Message) (proto.Message, error) {
	// Requests will all have an ID and an output directory specified. The
	// output directory can be a URI prefix, e.g., for pushing to S3.
	reqID, err := protoutil.MessageStringField(req, "RequestId")
	if err != nil {
		return nil, fmt.Errorf("could not get RequestId: %v", err)
	}
	// outDir names the final destination after unstaging. This can be an S3
	// URL prefix or a directory somewhere else (e.g., an NFS mount for the
	// ultimate destination of the data).
	outDir, err := protoutil.MessageStringField(req, "OutDir")
	if err != nil {
		return nil, fmt.Errorf("could not get OutDir: %v", err)
	}

	// Directories are a bit crazy. We have
	// - the source of actual bytes (req.Image.URI)
	// - the location to stage them from the proxy's perspective (localInDir)
	// - the location of staged files from the analytic's perspetive (backendInDir)
	// - the location for outputting files from the analytic's perspetive (backendOutDir)
	// - the location of output files from the proxy's perspective (localOutDir)
	// - the place to finally put (unstage) output files: (req.OutDir)
	//
	// Thus,
	// - files come in from req.Image.URI into localInDir,
	// - are visible from backendInDir, then
	// - they go out to backendOutDir,
	// - are visible from localOutDir, then
	// - they go out to req.OutDir.
	localInDir := filepath.Join(s.localStage, "req", reqID)
	backendInDir := filepath.Join(s.backendStage, "req", reqID)
	localOutDir := filepath.Join(s.localStage, "out", reqID)
	backendOutDir := filepath.Join(s.backendStage, "out", reqID)

	newReq := proto.Clone(req)

	// Inform the backend to write to backendOutDir. These will be visible by
	// this process at localOutDir (shared volume, potentially different mount
	// paths).
	if err := protoutil.RewriteOutDir(newReq, func(string) (string, error) {
		return backendOutDir, nil
	}); err != nil {
		return nil, fmt.Errorf("output dir rewrite: %v", err)
	}

	// Remove the local input dir when done.

	// Stage all inputs to our local input stage (localInDir), and inform the
	// backend (via URI rewrite) that they are available from the backend input
	// stage (backendInDir).
	defer os.RemoveAll(localInDir)
	if err := protoutil.RewriteURIs(newReq, func(src string) (string, error) {
		// Stage a file from URI to localInDir.
		stageBase, err := s.stageFile(ctx, localInDir, src)
		if err != nil {
			return "", fmt.Errorf("input stage failed: %v", err)
		}
		// Communicate where it will be from the backend's perspective.
		return filepath.Join(backendInDir, stageBase), nil
	}); err != nil {
		return nil, fmt.Errorf("rewrite input URIs: %v", err)
	}

	// Ensure that the output directory exists. Will appear in backendOutDir
	// from the backend perspective.
	defer os.RemoveAll(localOutDir)
	os.MkdirAll(localOutDir, 0755)

	// Call the backend.
	resp, err := f(ctx, newReq)
	if err != nil {
		return nil, err // Pass through, since it is already an RPC status error.
	}
	newResp := proto.Clone(resp)

	// All outputs were written to backendOutDir, which from this process is
	// actually localOutDir.
	if err := protoutil.RewriteURIs(newResp, func(src string) (string, error) {
		// Get a file. The backend will have written it to backendOutDir, which
		// from this process is seen as localOutDir. We thus need to get the
		// src path relative to backendOutDir, then add it to localOutDir. From there
		// we can unstage it to the final location: outDir, communicating that
		// to the caller below.
		relSrc, err := filepath.Rel(backendOutDir, src)
		if err != nil {
			return "", fmt.Errorf("unstage get rel path for %q in %q: %v", src, backendOutDir, err)
		}
		// Unstage from our local directory.
		pushBase, err := s.unstageFile(ctx, outDir, filepath.Join(localOutDir, relSrc))
		if err != nil {
			return "", fmt.Errorf("output unstage failed: %v", err)
		}
		// Communicate to the caller
		// Rewrite response to reference the originally-requested output directory.
		return path.Join(outDir, pushBase), nil
	}); err != nil {
		return nil, fmt.Errorf("rewrite output URIs: %v", err)
	}

	return newResp, nil
}

// DetectImageManipulation implements the DetectImageManipulation service endpoint for
// analytics. Forwards to the backend after staging all necessary files and
// rewriting resources to point to their local locations. Returns response
// after unstaging all output files and rewriting resources to reflect ultimate
// destinations. Note that all endpoints do this wrapped call step.
func (s *analyticSvc) DetectImageManipulation(ctx context.Context, req *pb.ImageManipulationRequest) (*pb.ImageManipulation, error) {
	call := func(ctx context.Context, r proto.Message) (proto.Message, error) {
		resp, err := s.analytic.DetectImageManipulation(ctx, r.(*pb.ImageManipulationRequest))
		return resp, err
	}
	resp, err := s.stager.callWithFileStaging(ctx, call, req)
	return resp.(*pb.ImageManipulation), err
}

// DetectVideoManipulation implements the VideoManipulation service endpoint for analytics.
// See DetectImageManipulation for info on how files are staged.
func (s *analyticSvc) DetectVideoManipulation(ctx context.Context, req *pb.VideoManipulationRequest) (*pb.VideoManipulation, error) {
	call := func(ctx context.Context, r proto.Message) (proto.Message, error) {
		return s.analytic.DetectVideoManipulation(ctx, r.(*pb.VideoManipulationRequest))
	}
	resp, err := s.stager.callWithFileStaging(ctx, call, req)
	return resp.(*pb.VideoManipulation), err
}

// DetectImageSplice implements the ImageSplice service endpoint for analytics.
// See DetectImageManipulation for info on how files are staged.
func (s *analyticSvc) DetectImageSplice(ctx context.Context, req *pb.ImageSpliceRequest) (*pb.ImageSplice, error) {
	call := func(ctx context.Context, r proto.Message) (proto.Message, error) {
		return s.analytic.DetectImageSplice(ctx, r.(*pb.ImageSpliceRequest))
	}
	resp, err := s.stager.callWithFileStaging(ctx, call, req)
	return resp.(*pb.ImageSplice), err
}

// DetectImageCameraMatch implements the ImageCameraMatch service endpoint for analytics.
// See DetectImageManipulation for info on how files are staged.
func (s *analyticSvc) DetectImageCameraMatch(ctx context.Context, req *pb.ImageCameraMatchRequest) (*pb.ImageCameraMatch, error) {
	call := func(ctx context.Context, r proto.Message) (proto.Message, error) {
		return s.analytic.DetectImageCameraMatch(ctx, r.(*pb.ImageCameraMatchRequest))
	}
	resp, err := s.stager.callWithFileStaging(ctx, call, req)
	return resp.(*pb.ImageCameraMatch), err
}

// DetectVideoCameraMatch implements the VideoCameraMatch service endpoint for analytics.
// See DetectVideoManipulation for info on how files are staged.
func (s *analyticSvc) DetectVideoCameraMatch(ctx context.Context, req *pb.VideoCameraMatchRequest) (*pb.VideoCameraMatch, error) {
	call := func(ctx context.Context, r proto.Message) (proto.Message, error) {
		return s.analytic.DetectVideoCameraMatch(ctx, r.(*pb.VideoCameraMatchRequest))
	}
	resp, err := s.stager.callWithFileStaging(ctx, call, req)
	return resp.(*pb.VideoCameraMatch), err
}

// DetectImageCameras implements the ImageCameras service endpoint for analytics.
// See DetectImageManipulation for info on how files are staged.
func (s *analyticSvc) DetectImageCameras(ctx context.Context, req *pb.ImageCamerasRequest) (*pb.ImageCameras, error) {
	call := func(ctx context.Context, r proto.Message) (proto.Message, error) {
		return s.analytic.DetectImageCameras(ctx, r.(*pb.ImageCamerasRequest))
	}
	resp, err := s.stager.callWithFileStaging(ctx, call, req)
	return resp.(*pb.ImageCameras), err
}

// Kill causes this service to die on demand.
func (s *analyticSvc) Kill(ctx context.Context, _ *pb.Empty) (*pb.Empty, error) {
	log.Fatal("Asked to die")
	return nil, nil
}

// Check forwards health checks on to the backend.
func (s *analyticSvc) Check(ctx context.Context, req *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return s.health.Check(ctx, req)
}

// Watch is a currently unimplemented health endpoint.
func (s *analyticSvc) Watch(req *healthpb.HealthCheckRequest, healthServer healthpb.Health_WatchServer) error {
	ctx := context.Background()
	client, err := s.health.Watch(ctx, req)
	if err != nil {
		return err
	}
	for {
		resp, err := client.Recv()
		if err != nil {
			return err
		}
		if err := healthServer.Send(resp); err != nil {
			return err
		}
	}
}

func main() {
	flag.Parse()
	ctx := context.Background()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("Listen error: %v", err)
	}
	server := grpc.NewServer()
	var stagerOpts []FileStagerOption
	if *ldapUser != "" {
		stagerOpts = append(stagerOpts, WithS3Credentials(*ldapUser, *ldapPass))
	}
	if *backendStage != "" {
		stagerOpts = append(stagerOpts, WithBackendStage(*backendStage))
	}

	stager, err := NewFileStager(*localStage, stagerOpts...)
	if err != nil {
		log.Fatalf("Failed to create stager: %v", err)
	}

	dialCtx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, *backend, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("Dial backend %q failed: %v", *backend, err)
	}
	defer conn.Close()
	asvc := &analyticSvc{
		analytic: pb.NewAnalyticClient(conn),
		health:   healthpb.NewHealthClient(conn),
		stager:   stager,
	}
	pb.RegisterAnalyticServer(server, asvc)
	healthpb.RegisterHealthServer(server, asvc)

	log.Fatal(server.Serve(lis))
}
