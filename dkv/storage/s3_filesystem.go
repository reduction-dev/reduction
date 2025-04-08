package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"reduction.dev/reduction/storage/objstore"
	"reduction.dev/reduction/util/ptr"
)

type S3FileSystem struct {
	client *objstore.S3StorageWithUsage
	bucket string
	prefix string
}

const s3Protocol = "s3://"

func NewS3FileSystem(client objstore.S3Service, bucket, prefix string) *S3FileSystem {
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	usageClient := objstore.NewS3StorageWithUsage(client)

	return &S3FileSystem{
		bucket: bucket,
		prefix: prefix,
		client: usageClient,
	}
}

func NewS3FileSystemFromURI(uri string) (*S3FileSystem, error) {
	bucket, prefix, err := parseS3URI(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid s3 URI: %s", uri)
	}

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS configuration: %w", err)
	}
	client := s3.NewFromConfig(cfg)

	return NewS3FileSystem(client, bucket, prefix), nil
}

func (fs *S3FileSystem) New(name string) File {
	if strings.HasPrefix(name, s3Protocol) {
		panic(fmt.Sprintf("creating a file with URI path (%s) not supported", name))
	}
	return &S3Object{
		bucket:   fs.bucket,
		key:      fs.prefix + name,
		name:     name,
		fs:       fs,
		buffer:   new(bytes.Buffer),
		fileMode: FILE_MODE_WRITE,
	}
}

func (fs *S3FileSystem) Open(name string) File {
	var bucket, key string
	if strings.HasPrefix(name, s3Protocol) {
		var err error
		bucket, key, err = parseS3URI(name)
		if err != nil {
			panic(fmt.Sprintf("invalid s3 URI: %s", name))
		}
	} else {
		key = fs.prefix + name
		bucket = fs.bucket
	}

	return &S3Object{
		bucket:   bucket,
		key:      key,
		name:     name,
		fs:       fs,
		fileMode: FILE_MODE_READ,
	}
}

func (fs *S3FileSystem) Copy(sourceURI string, destination string) error {
	if !strings.HasPrefix(sourceURI, s3Protocol) {
		return fmt.Errorf("s3 source URI must start with %s", s3Protocol)
	}

	_, err := fs.client.CopyObject(context.Background(), &s3.CopyObjectInput{
		CopySource: ptr.New(strings.TrimPrefix(sourceURI, s3Protocol)),
		Bucket:     &fs.bucket,
		Key:        ptr.New(fs.prefix + destination),
	})
	return err
}

func (fs *S3FileSystem) USDCost() string {
	return fs.client.TotalCost()
}

func parseS3URI(uri string) (string, string, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return "", "", fmt.Errorf("invalid s3 URI: %s", uri)
	}
	bucket := u.Host
	key := strings.TrimPrefix(u.Path, "/")
	return bucket, key, nil
}

var _ FileSystem = (*S3FileSystem)(nil)

type S3Object struct {
	bucket   string
	key      string
	name     string
	fs       *S3FileSystem
	buffer   *bytes.Buffer
	reader   *bytes.Reader
	size     int64
	fileMode FileMode
}

func (o *S3Object) Name() string {
	return o.name
}

func (o *S3Object) ReadAt(p []byte, off int64) (n int, err error) {
	// For now download the whole file if we don't have it
	if o.reader == nil {
		output, err := o.fs.client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: &o.bucket,
			Key:    &o.key,
		})
		if err != nil {
			if isNoSuchKeyErr(err) {
				return 0, fmt.Errorf("reading s3://%s/%s: %w", o.bucket, o.key, ErrNotFound)
			}
			return 0, err
		}

		data, err := io.ReadAll(output.Body)
		if err != nil {
			return 0, err
		}
		output.Body.Close()
		o.reader = bytes.NewReader(data)
	}

	return o.reader.ReadAt(p, off)
}

func (o *S3Object) Size() int64 {
	return o.size
}

func (o *S3Object) Save() error {
	if o.fileMode == FILE_MODE_READ {
		panic("tried to save a read only file")
	}
	o.fileMode = FILE_MODE_READ
	b := o.buffer.Bytes()
	_, err := o.fs.client.PutObject(context.Background(), &s3.PutObjectInput{
		Bucket: &o.bucket,
		Key:    &o.key,
		Body:   bytes.NewReader(b),
	})
	o.reader = bytes.NewReader(o.buffer.Bytes())
	return err
}

// Write data to a temporary buffer that can be flushed with `Sync()`.
func (o *S3Object) Write(p []byte) (n int, err error) {
	if o.fileMode == FILE_MODE_READ {
		panic("tried to write to a read only file")
	}
	n, err = o.buffer.Write(p)
	o.size += int64(n)
	return n, err
}

func (o *S3Object) Delete() error {
	if o.fileMode == FILE_MODE_WRITE {
		panic("tried to delete a file being written")
	}
	_, err := o.fs.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
		Bucket: &o.bucket,
		Key:    &o.key,
	})
	return err
}

func (o *S3Object) URI() string {
	return s3Protocol + filepath.Join(o.bucket, o.key)
}

func (o *S3Object) CreateDeleteFunc() func() error {
	fs := o.fs
	key := o.key
	return func() error {
		_, err := fs.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: &fs.bucket,
			Key:    &key,
		})
		return err
	}
}

func isNoSuchKeyErr(err error) bool {
	var notFoundErr *types.NoSuchKey
	return errors.As(err, &notFoundErr)
}

var _ File = (*S3Object)(nil)
