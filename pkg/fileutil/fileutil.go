// Package fileutil contains routines and clients for moving bytes around
// between object stores and files, or files and files.
package fileutil

import (
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"mime"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/h2non/filetype"
	minio "github.com/minio/minio-go"
	"github.com/pkg/errors"
)

func init() {
	mime.AddExtensionType(".mts", "video/avchd-stream")
}

var (
	schemeRE = regexp.MustCompile(`^\w+://`)
)

type rgwData struct {
	Version int    `json:"version"`
	Type    string `json:"type"`
	ID      string `json:"id"`
	Key     string `json:"key"`
}

type tokData struct {
	RGWToken rgwData `json:"RGW_TOKEN"`
}

func rgwToken(username, password string) (string, error) {
	d := &tokData{
		RGWToken: rgwData{
			Version: 1,
			Type:    "ldap",
			ID:      username,
			Key:     password,
		},
	}

	b, err := json.Marshal(d)
	if err != nil {
		return "", fmt.Errorf("marshal of RGWToken failed: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// s3Split accepts a URL of the form /bucket_name/path/to/obj and splits it into
// the bucket name and object path parts.
func s3Split(urlPath string) (bucket, obj string, err error) {
	if !path.IsAbs(urlPath) {
		return "", "", fmt.Errorf("s3 path must be absolute, got %q", urlPath)
	}
	parts := strings.Split(urlPath, "/")

	// Note that Amazon S3 bucket names are required to be valid DNS subdomains, so
	// we (can safely, definitely?) assume there won't be slashes in our bucket names.
	bucketName := parts[1] // parts[0] is empty with a leading slash, which is required.
	objectName := strings.Join(parts[2:], "/")

	return bucketName, objectName, nil
}

// IsVideo returns whether the given path looks like a video based on extension.
func IsVideo(path string) bool {
	tp := mime.TypeByExtension(filepath.Ext(path))
	return (strings.HasPrefix(tp, "video/") || strings.HasPrefix(tp, "application/"))
}

// MimeTypeOrGeneric produces a mime type from the path extension, else octet-stream.
func MimeTypeOrGeneric(path string) string {
	mt := mime.TypeByExtension(filepath.Ext(path))
	if mt == "" {
		return "application/octet-stream"
	}
	return mt
}

// HashFile returns the hex string of the SHA1 hash of the bytes in the named file.
func HashFile(fname string) (string, error) {
	f, err := os.Open(fname)
	if err != nil {
		return "", fmt.Errorf("hash file open %q: %v", fname, err)
	}
	defer f.Close()

	h := sha1.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("hash file %q: %v", fname, err)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

// TranslateSubPath finds the components of path that are under the source
// path, and appends them to the target path.
func TranslateSubPath(src, target, path string) (string, error) {
	srcParts := strings.Split(src, string([]rune{filepath.Separator}))
	pathParts := strings.Split(path, string([]rune{filepath.Separator}))

	for i, sp := range srcParts {
		if pathParts[i] != sp {
			return "", errors.Errorf("path translate failure: %q not prefixed by %q", path, src)
		}
	}

	resultParts := append([]string{target}, pathParts[len(srcParts):]...)
	return filepath.Clean(filepath.Join(resultParts...)), nil
}

// CopyFile copies bytes from the source filename to the dest filename. If they
// are determined to be the same file, it does nothing and returns a nil error.
// If the destination directory does not exist, it is created via os.MkdirAll
// with permissions 0775.
func CopyFile(dest, source string) error {
	// TODO: write to something .tmp, atomic rename at end.
	if strings.HasSuffix(dest, "/") {
		dest = filepath.Join(dest, filepath.Base(source))
	}

	si, err := os.Stat(source)
	if err != nil {
		return fmt.Errorf("copyFile source stat: %v", err)
	}
	if di, err := os.Stat(dest); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("copyFile dest stat: %v", err)
		}
		// Error because the file isn't there? No problem.
	} else {
		if os.SameFile(di, si) {
			return nil // nothing to do
		}
		return fmt.Errorf("copyFile dest exists: %v", err)
	}

	if err := os.MkdirAll(filepath.Dir(dest), 0775); err != nil {
		return fmt.Errorf("copyFile mkdirs: %v", err)
	}

	sf, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("copyFile source open: %v", err)
	}
	defer sf.Close()

	df, err := os.Create(dest)
	if err != nil {
		return fmt.Errorf("copyFile dest create: %v", err)
	}
	defer df.Close()

	if _, err := io.Copy(df, sf); err != nil {
		return fmt.Errorf("copyFile: %v", err)
	}
	return nil
}

// FileUtil provides operations for accessing files and object stores and
// performing common operations between them.
type FileUtil struct {
	rgwToken string
	// TODO: add a cache of minio clients to reuse, per host?
}

type fileUtilMod struct {
	s3User string
	s3Pass string
}

// FileUtilOption is an option that modifies behavior of the FileUtil when passed to New.
type FileUtilOption func(*fileUtilMod)

// WithS3Credentials creates a FileUtilOption that sets the S3 user and password for S3 API operations.
func WithS3Credentials(user, pass string) FileUtilOption {
	return func(m *fileUtilMod) {
		m.s3User = user
		m.s3Pass = pass
	}
}

// New produces a new FileUtil that can access an object store with the
// given credentials. This allows them to be omitted in object store URLs.
func New(opts ...FileUtilOption) (*FileUtil, error) {
	mod := new(fileUtilMod)
	for _, opt := range opts {
		opt(mod)
	}
	tok, err := rgwToken(mod.s3User, mod.s3Pass)
	if err != nil {
		return nil, fmt.Errorf("cannot create file util credentials: %v", err)
	}
	return &FileUtil{
		rgwToken: tok,
	}, nil
}

func (f *FileUtil) s3Get(ctx context.Context, dest string, s3src *url.URL) error {
	bucket, object, err := s3Split(s3src.Path)
	if err != nil {
		return fmt.Errorf("s3 get invalid path: %v", err)
	}
	client, err := minio.New(s3src.Host, f.rgwToken, "", false)
	if err != nil {
		return fmt.Errorf("s3 get client failure: %v", err)
	}
	if err := client.FGetObjectWithContext(ctx, bucket, object, dest, minio.GetObjectOptions{}); err != nil {
		return fmt.Errorf("s3 get: %v", err)
	}
	return nil
}

func (f *FileUtil) s3Put(ctx context.Context, s3dest *url.URL, src string) error {
	bucket, object, err := s3Split(s3dest.Path)
	if err != nil {
		return fmt.Errorf("s3 put invalid path: %v", err)
	}
	client, err := minio.New(s3dest.Host, f.rgwToken, "", false)
	if err != nil {
		return fmt.Errorf("s3 put client failure: %v", err)
	}
	if _, err := client.FPutObjectWithContext(ctx, bucket, object, src, minio.PutObjectOptions{}); err != nil {
		return fmt.Errorf("s3 put: %v", err)
	}
	return nil
}

// PullToFile reads the bytes given by the uri (can be a file name) and
// writes them to the file named by dest. The destination must be a complete
// file name, not a directory.
func (f *FileUtil) PullToFile(ctx context.Context, dest, uri string) error {
	if !schemeRE.MatchString(uri) {
		uri = "file://" + uri
	}
	u, err := url.Parse(uri)
	if err != nil {
		return fmt.Errorf("open invalid uri %q: %v", uri, err)
	}

	switch u.Scheme {
	case "file":
		return CopyFile(dest, path.Join(u.Host, u.Path))
	case "s3":
		return f.s3Get(ctx, dest, u)
	default:
		return fmt.Errorf("unknown open resource scheme %q", u.Scheme)
	}
}

// PushFromFile reads the byte at the source file and attempts to write them to
// the given URI. If the URI represents a file, it is treated as a file name.
// If it is a file name and ends in /, it is treated as a directory and the
// name will come from the source file name.
func (f *FileUtil) PushFromFile(ctx context.Context, uri, source string) error {
	if !schemeRE.MatchString(uri) {
		uri = "file://" + uri
	}
	u, err := url.Parse(uri)
	if err != nil {
		return fmt.Errorf("create resource invalid uri %q: %v", uri, err)
	}

	switch u.Scheme {
	case "file":
		return CopyFile(path.Join(u.Host, u.Path), source)
	case "s3":
		return f.s3Put(ctx, u, source)
	default:
		return fmt.Errorf("unknown open resource scheme %q", u.Scheme)
	}
}

// FileType returns the mime type string for a given file path, e.g., "image/jpeg" for a JPEG image.
// Returns "inode/directory" if the referenced path is a directory name.
func FileType(path string) (string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("stat path %q: %v", path, err)
	}
	if info.IsDir() {
		return "inode/directory", nil
	}
	ftype, err := filetype.MatchFile(path)
	if err != nil {
		return "", fmt.Errorf("file type detection for %q: %v", path, err)
	}
	return ftype.MIME.Value, nil
}

// FileTypes returns the mime type for every file specified. See FileType.
func FileTypes(paths []string) ([]string, error) {
	var types []string
	for _, p := range paths {
		ft, err := FileType(p)
		if err != nil {
			return nil, fmt.Errorf("file type for path: %v", err)
		}
		types = append(types, ft)
	}
	return types, nil
}

// Translator translates path names that are under a common source directory.
// The source is removed and replaced with the target.
type Translator struct {
	source string
	target string

	stage   Stager
	unstage Unstager
}

// TranslatorOption provides options to the file path translator.
type TranslatorOption func(*Translator)

// Stager is a function that gets a file from a source URI and pushes it to a destination (local) file path.
type Stager func(destPath, srcURI string) error

// Unstager is a function that gets a local file and pushes it ot a destination URI.
type Unstager func(destURI, srcPath string) error

// WithStage instructs the translator to not only translate paths, but to
// stage files during the translate processes. For example,
// it may be that an input translator should pull files from an HTTP service
// and store them on disk. Turning this on allows that to happen. Otherwise it
// merely does path translation.
//
// This does not change the behavior of Untranslate.
func WithStager(s Stager) TranslatorOption {
	return func(t *Translator) {
		t.stage = s
	}
}

// WithUnstage instructs the translator to copy files while untranslating,
// e.g., to push a file to an originating service. See WithStage for details.
//
// This does not change the behavior of Translate.
func WithUnstager(u Unstager) TranslatorOption {
	return func(t *Translator) {
		t.unstage = u
	}
}

// NewTranslator creates a path translator from the given source and target
// paths. The file name to be translated must be under the source tree for
// translation to work properly. This works best when both source and target
// paths are absolute or relative to the same parent.
func NewTranslator(source, target string, opts ...TranslatorOption) *Translator {
	t := &Translator{
		source: source,
		target: target,
	}
	for _, o := range opts {
		o(t)
	}
	return t
}

// Translate finds the source prefix and removes it, replacing it with the target value.
func (t *Translator) Translate(path string) (string, error) {
	if t.source == "" || t.target == "" || t.source == t.target {
		return path, nil
	}
	newPath, err := TranslateSubPath(t.source, t.target, path)
	if err != nil {
		return "", err
	}
	if t.stage != nil {
		if err := t.stage(newPath, path); err != nil {
			return "", fmt.Errorf("staging translated file %q -> %q: %v", path, newPath, err)
		}
	}
	return newPath, nil
}

// Untranslate removes the target prefix and replaces it with the source. It undoes the translate operation.
func (t *Translator) Untranslate(path string) (string, error) {
	if t.source == "" || t.target == "" || t.source == t.target {
		return path, nil
	}
	newPath, err := TranslateSubPath(t.target, t.source, path)
	if err != nil {
		return "", err
	}
	if t.unstage != nil {
		if err := t.unstage(newPath, path); err != nil {
			return "", fmt.Errorf("unstaging untranslated file %q -> %q: %v", path, newPath, err)
		}
	}
	return newPath, nil
}
