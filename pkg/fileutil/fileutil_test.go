// Base test Noop test class for validating custom analytic implementation
// Note: the service interfaces will be tested using the cli and sample images
// provided by the gitlab.mediforprogram.com/analytic-wrapper/medifor-proto-utils repo
package fileutil

import (
	"testing"
)

func TestS3Split(t *testing.T) {
	cases := []struct {
		name       string
		s3path     string
		wantBucket string
		wantObject string
		wantError  bool
	}{
		{
			name:       "basic path",
			s3path:     "/bucket/object",
			wantBucket: "bucket",
			wantObject: "object",
		},
		{
			name:       "basic path with slash",
			s3path:     "/bucket/object/",
			wantBucket: "bucket",
			wantObject: "object/",
		},
		{
			name:       "virtual path",
			s3path:     "/bucket/virtual/path/to/object/",
			wantBucket: "bucket",
			wantObject: "virtual/path/to/object/",
		},
		{
			name:      "relative path error",
			s3path:    "bucket",
			wantError: true,
		},
	}

	for _, test := range cases {
		bucket, object, err := s3Split(test.s3path)
		if test.wantError {
			if err == nil {
				t.Errorf("Test %q expected an error, got bucket %q and object %q", test.name, bucket, object)
			}
			continue
		}

		if err != nil {
			t.Errorf("Test %q got unexpected error: %v", test.name, err)
			continue
		}

		if want, got := test.wantBucket, bucket; want != got {
			t.Errorf("Test %q want bucket %q, got %q", test.name, want, got)
		}

		if want, got := test.wantObject, object; want != got {
			t.Errorf("Test %q want object %q, got %q", test.name, want, got)
		}
	}
}

func TestTranslateSubPath(t *testing.T) {
	cases := []struct {
		name      string
		src       string
		dst       string
		path      string
		want      string
		wantError bool
	}{
		{
			name: "both absolute",
			src:  "/path/to/parent",
			dst:  "/dest",
			path: "/path/to/parent/some/dir/file",
			want: "/dest/some/dir/file",
		},
		{
			name: "both relative",
			src:  "floating/parent/dir",
			dst:  "other/dest",
			path: "floating/parent/dir/some/file",
			want: "other/dest/some/file",
		},
		{
			name: "src abs, dest rel",
			src:  "/abs/path/to/parent",
			dst:  "floating",
			path: "/abs/path/to/parent/file",
			want: "floating/file",
		},
		{
			name: "src rel, dest abs",
			src:  "floating/src",
			dst:  "/path/to/abs/loc",
			path: "floating/src/stuff",
			want: "/path/to/abs/loc/stuff",
		},
		{
			name:      "abs path not prefixed by abs src",
			src:       "/path/to/src",
			path:      "/other/location/of/things",
			wantError: true,
		},
		{
			name:      "abs path not prefixed by rel src",
			src:       "floating/src",
			path:      "/some/path",
			wantError: true,
		},
		{
			name:      "rel path abs src",
			src:       "/path/to/something",
			path:      "floating/location",
			wantError: true,
		},
	}

	for _, test := range cases {
		txpath, err := TranslateSubPath(test.src, test.dst, test.path)
		if test.wantError {
			if err == nil {
				t.Errorf("Test %q wanted error, got valid result %q", test.name, txpath)
			}
			continue
		}

		if err != nil {
			t.Errorf("Test %q got unexpected error: %v", test.name, err)
			continue
		}

		if want, got := test.want, txpath; want != got {
			t.Errorf("Test %q failure: want path %q, got %q", test.name, want, got)
		}
	}
}
