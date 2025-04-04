package storage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"reduction.dev/reduction/storage/objstore"
	"reduction.dev/reduction/util/ptr"
)

type S3FileSystem struct {
	client     objstore.S3Service
	bucketName string
	awsUsage   *S3Usage
}

func NewS3FileSystem(client objstore.S3Service, bucketName string) *S3FileSystem {
	return &S3FileSystem{
		bucketName: bucketName,
		client:     client,
		awsUsage:   &S3Usage{},
	}
}

func (fs *S3FileSystem) New(key string) File {
	return &S3Object{
		key:      key,
		fs:       fs,
		buffer:   new(bytes.Buffer),
		fileMode: FILE_MODE_WRITE,
	}
}

func (fs *S3FileSystem) Open(key string) File {
	return &S3Object{
		key:      key,
		fs:       fs,
		fileMode: FILE_MODE_READ,
	}
}

func (fs *S3FileSystem) Copy(sourceURI string, destination string) error {
	u, err := url.Parse(sourceURI)
	if err != nil {
		return err
	}

	_, err = fs.client.CopyObject(context.Background(), &s3.CopyObjectInput{
		Bucket:     &fs.bucketName,
		CopySource: ptr.New(filepath.Join(u.Host, u.Path)),
		Key:        ptr.New(destination),
	})
	fs.awsUsage.AddExpensiveRequest()
	return err
}

func (fs *S3FileSystem) USDCost() string {
	return fs.awsUsage.TotalCost()
}

var _ FileSystem = (*S3FileSystem)(nil)

type S3Object struct {
	key      string
	fs       *S3FileSystem
	buffer   *bytes.Buffer
	reader   *bytes.Reader
	size     int64
	fileMode FileMode
}

func (o *S3Object) Name() string {
	return o.key
}

func (o *S3Object) ReadAt(p []byte, off int64) (n int, err error) {
	// For now download the whole file if we don't have it
	if o.reader == nil {
		output, err := o.fs.client.GetObject(context.Background(), &s3.GetObjectInput{
			Bucket: &o.fs.bucketName,
			Key:    &o.key,
		})
		if err != nil {
			if isNoSuchKeyErr(err) {
				return 0, ErrNotFound
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
		Bucket: &o.fs.bucketName,
		Key:    &o.key,
		Body:   bytes.NewReader(b),
	})
	o.fs.awsUsage.AddExpensiveRequest()
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
		Bucket: &o.fs.bucketName,
		Key:    &o.key,
	})
	return err
}

func (o *S3Object) URI() string {
	return "s3://" + filepath.Join(o.fs.bucketName, o.key)
}

func (o *S3Object) CreateDeleteFunc() func() error {
	fs := o.fs
	key := o.key
	return func() error {
		_, err := fs.client.DeleteObject(context.Background(), &s3.DeleteObjectInput{
			Bucket: &fs.bucketName,
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
