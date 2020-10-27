package analytic

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	pb "github.com/mediaforensics/medifor/pkg/mediforproto"
)

func TestFindResources(t *testing.T) {
	cases := []struct {
		name      string
		detection interface{}
		want      []*pb.Resource
	}{
		{
			name: "image manip",
			detection: &pb.Detection{
				Request: &pb.Detection_ImgManipReq{
					ImgManipReq: &pb.ImageManipulationRequest{
						OutDir: "outdir",
						Image: &pb.Resource{
							Uri:  "uri1.jpg",
							Type: "image/jpeg",
						},
					},
				},
			},
			want: []*pb.Resource{
				{Uri: "uri1.jpg", Type: "image/jpeg"},
			},
		},
		{
			name: "splice manip",
			detection: &pb.Detection{
				Request: &pb.Detection_ImgSpliceReq{
					ImgSpliceReq: &pb.ImageSpliceRequest{
						OutDir: "outdir",
						ProbeImage: &pb.Resource{
							Uri:  "uriprobe.png",
							Type: "image/png",
						},
						DonorImage: &pb.Resource{
							Uri:  "uridonor.png",
							Type: "image/png",
						},
					},
				},
			},
			want: []*pb.Resource{
				{Uri: "uriprobe.png", Type: "image/png"},
				{Uri: "uridonor.png", Type: "image/png"},
			},
		},
		{
			name: "video manip",
			detection: &pb.Detection{
				Request: &pb.Detection_VidManipReq{
					VidManipReq: &pb.VideoManipulationRequest{
						OutDir: "outdir",
						Video: &pb.Resource{
							Uri: "uri1.m4v",
						},
					},
				},
			},
			want: []*pb.Resource{
				{Uri: "uri1.m4v"},
			},
		},
		{
			name:      "empty",
			detection: &pb.Detection{},
			want:      nil,
		},
		{
			name: "request only",
			detection: &pb.Detection_ImgManipReq{
				ImgManipReq: &pb.ImageManipulationRequest{
					OutDir: "blah",
					Image: &pb.Resource{
						Uri: "something.jpg",
					},
				},
			},
			want: []*pb.Resource{{Uri: "something.jpg"}},
		},
	}

	for _, c := range cases {
		res, err := FindResources(c.detection)
		if err != nil {
			t.Fatalf("FindResources failed on %q: %v", c.name, err)
		}
		if diff := cmp.Diff(c.want, res); diff != "" {
			t.Errorf("FindResources unexpected diff (-want +got):\n%v", diff)
		}
	}
}
