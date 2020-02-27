// Package analyticservice abstracts creation of the necessary service elements
// away from analytics, allowing callbacks to be registered with simple APIs.
//
// Example:
//
// 	func main() {
// 		s, err := analyticservice.New("My Team", "My Analytic ID", analyticservice.WithDefaultMaxBatch(10))
// 		if err != nil {
// 			log.Fatalf("Failed to create medifor service: %v", err)
// 		}
//
// 		s.RegisterImageManipulation(func(ctx context.Context, req *pb.ImageManipulationRequest) (*pb.ManipulationResponse, error) {
// 			// Do the manipulation task and return the appropriate proto response here.
// 		})
//
// 		log.Fatal(s.Run(context.Background()))
// 	}
package analyticservice

import (
	"context"
	"fmt"
	"log"
	"net"

	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	pb "gitlab.mediforprogram.com/medifor/medifor-proto/pkg/mediforproto"
)

// ImageManipulationFunc defines a callback for analytics that detect general image manipulations.
type ImageManipulationFunc func(ctx context.Context, req *pb.ImageManipulationRequest) (*pb.ImageManipulation, error)

// ImageSpliceFunc defines a callback for analytics that detect image splices.
type ImageSpliceFunc func(ctx context.Context, req *pb.ImageSpliceRequest) (*pb.ImageSplice, error)

// VideoManipulationFunc defines a callback for analytics that detect video manipulations.
type VideoManipulationFunc func(ctx context.Context, req *pb.VideoManipulationRequest) (*pb.VideoManipulation, error)

// ImageCameraMatchFunc defines a callback for analytics that detect an image camera ID match.
type ImageCameraMatchFunc func(ctx context.Context, req *pb.ImageCameraMatchRequest) (*pb.ImageCameraMatch, error)

// VideoCameraMatchFunc defines a callback for analytics that detect an image camera ID match.
type VideoCameraMatchFunc func(ctx context.Context, req *pb.VideoCameraMatchRequest) (*pb.VideoCameraMatch, error)

// ImageCamerasFunc defines a callback for analytics that detect an image camera ID match.
type ImageCamerasFunc func(ctx context.Context, req *pb.ImageCamerasRequest) (*pb.ImageCameras, error)

// AnalyticService implements the analytic service proto endpoints.
type AnalyticService struct {
	Port int // Analytic service port, defaults to 50051

	listener  net.Listener
	server    *grpc.Server
	endpoints map[string]interface{} // Map names to functions.
}

// ServiceOption allows options to be set at the service level.
type ServiceOption func(*AnalyticService)

// WithPort creates an option that sets the local TCP port for this service.
func WithPort(port int) ServiceOption {
	return func(s *AnalyticService) {
		s.Port = port
	}
}

// WithListener specifies the network listener to use when running.
func WithListener(listener net.Listener) ServiceOption {
	return func(s *AnalyticService) {
		s.listener = listener
	}
}

// New creates a new AnalyticService that listens on the given port.
func New(opts ...ServiceOption) (*AnalyticService, error) {
	s := &AnalyticService{
		Port:      50051,
		endpoints: make(map[string]interface{}),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s, nil
}

// Stop stops the service cleanly.
func (s *AnalyticService) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}

// Run starts the service and blocks until it dies or the context is canceled.
func (s *AnalyticService) Run(ctx context.Context) error {
	if s.listener == nil {
		var err error
		s.listener, err = net.Listen("tcp", fmt.Sprintf(":%d", s.Port))
		if err != nil {
			return fmt.Errorf("medifor service analytic listen: %v", err)
		}
	}
	s.server = grpc.NewServer()
	hsvc := health.NewServer()
	pb.RegisterAnalyticServer(s.server, &analyticService{impl: s, healthSvc: hsvc})
	healthpb.RegisterHealthServer(s.server, hsvc)

	g, ctx := errgroup.WithContext(ctx)
	done := make(chan bool)
	g.Go(func() error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done: // clean shutdown
			return nil
		}
	})
	g.Go(func() error {
		log.Printf("Analytic service listening at %v", s.listener.Addr())
		if err := s.server.Serve(s.listener); err != nil {
			return err
		}
		log.Printf("Analytic service shutting down cleanly at %v", s.listener.Addr())
		// Cleanly shut down (make the context listener die without an error).
		close(done)
		return nil
	})
	return g.Wait()
}

func (s *AnalyticService) registerFunc(name string, f interface{}) error {
	if _, ok := s.endpoints[name]; ok {
		return fmt.Errorf("duplicate %s registration", name)
	}
	s.endpoints[name] = f
	return nil
}

// RegisterImageManipulation registers a function to handle ImageManipulation
// calls. Pass options to change the way the callback is invoked.
func (s *AnalyticService) RegisterImageManipulation(f ImageManipulationFunc) error {
	return s.registerFunc("ImageManipulation", f)
}

// RegisterImageSplice registers a function to handle ImageSplice calls.
func (s *AnalyticService) RegisterImageSplice(f ImageSpliceFunc) error {
	return s.registerFunc("ImageSplice", f)
}

// RegisterVideoManipulation registers a function to handle VideoManipulation
// calls.
func (s *AnalyticService) RegisterVideoManipulation(f VideoManipulationFunc) error {
	return s.registerFunc("VideoManipulation", f)
}

// RegisterImageCameraMatch registers a function to determine whether given camera ID is a match for an image.
func (s *AnalyticService) RegisterImageCameraMatch(f ImageCameraMatchFunc) error {
	return s.registerFunc("ImageCameraMatch", f)
}

// RegisterVideoCameraMatch registers a function to determine whether given camera ID is a match for an image.
func (s *AnalyticService) RegisterVideoCameraMatch(f VideoCameraMatchFunc) error {
	return s.registerFunc("VideoCameraMatch", f)
}

// RegisterImageCameras registers a function to determine whether given camera ID is a match for an image.
func (s *AnalyticService) RegisterImageCameras(f ImageCamerasFunc) error {
	return s.registerFunc("ImageCameras", f)
}

type analyticService struct {
	impl      *AnalyticService
	healthSvc *health.Server
}

func (s *analyticService) endpoint(kind string) (interface{}, error) {
	e, ok := s.impl.endpoints[kind]
	if !ok {
		return nil, status.Errorf(codes.Unimplemented, "no implementation for %s", kind)
	}
	return e, nil
}

// DetectImageManipulation calls a registered image manipulation detector if present.
func (s *analyticService) DetectImageManipulation(ctx context.Context, req *pb.ImageManipulationRequest) (*pb.ImageManipulation, error) {
	ep, err := s.endpoint("ImageManipulation")
	if err != nil {
		return nil, err
	}
	return ep.(ImageManipulationFunc)(ctx, req)
}

// DetectImageSplice calls a registered image splice detector if present.
func (s *analyticService) DetectImageSplice(ctx context.Context, req *pb.ImageSpliceRequest) (*pb.ImageSplice, error) {
	ep, err := s.endpoint("ImageSplice")
	if err != nil {
		return nil, err
	}

	return ep.(ImageSpliceFunc)(ctx, req)
}

// DetectVideoManipulation calls a registered video manipulation detector if present.
func (s *analyticService) DetectVideoManipulation(ctx context.Context, req *pb.VideoManipulationRequest) (*pb.VideoManipulation, error) {
	ep, err := s.endpoint("VideoManipulation")
	if err != nil {
		return nil, err
	}
	return ep.(VideoManipulationFunc)(ctx, req)
}

// DetectImageCameraMatch calls a registered image camera ID match detector if registered.
func (s *analyticService) DetectImageCameraMatch(ctx context.Context, req *pb.ImageCameraMatchRequest) (*pb.ImageCameraMatch, error) {
	ep, err := s.endpoint("ImageCameraMatch")
	if err != nil {
		return nil, err
	}
	return ep.(ImageCameraMatchFunc)(ctx, req)
}

// DetectVideoCameraMatch calls a registered image camera ID match detector if registered.
func (s *analyticService) DetectVideoCameraMatch(ctx context.Context, req *pb.VideoCameraMatchRequest) (*pb.VideoCameraMatch, error) {
	ep, err := s.endpoint("VideoCameraMatch")
	if err != nil {
		return nil, err
	}
	return ep.(VideoCameraMatchFunc)(ctx, req)
}

// DetectImageCameras calls a registered image camera ID match detector if registered.
func (s *analyticService) DetectImageCameras(ctx context.Context, req *pb.ImageCamerasRequest) (*pb.ImageCameras, error) {
	ep, err := s.endpoint("ImageCameras")
	if err != nil {
		return nil, err
	}
	return ep.(ImageCamerasFunc)(ctx, req)
}
