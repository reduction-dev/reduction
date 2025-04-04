package s3fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"reduction.dev/reduction/storage"
	"reduction.dev/reduction/storage/objstore"
)

type S3Location struct {
	s3     objstore.S3Service
	bucket string
	prefix string
}

func NewS3Location(s3 objstore.S3Service, path string) *S3Location {
	// Remove s3:// prefix if present
	path = strings.TrimPrefix(path, "s3://")

	// Split into bucket and prefix
	parts := strings.SplitN(path, "/", 2)
	bucket := parts[0]
	prefix := ""
	if len(parts) > 1 {
		prefix = parts[1]
	}

	// Ensure the prefix ends with a slash. No one wants pathnames like
	// "prefixfile.txt".
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return &S3Location{
		s3:     s3,
		bucket: bucket,
		prefix: prefix,
	}
}

func (l *S3Location) Write(path string, data io.Reader) (uri string, err error) {
	key := resolveKey(l.prefix, path)
	_, err = l.s3.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: &l.bucket,
		Key:    &key,
		Body:   data,
	})
	if err != nil {
		return "", fmt.Errorf("failed to write object: %w", err)
	}

	// Return the full S3 URI for the written object
	return s3URI(l.bucket, key), nil
}

func (l *S3Location) Read(path string) ([]byte, error) {
	key := resolveKey(l.prefix, path)
	output, err := l.s3.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &l.bucket,
		Key:    &key,
	})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return nil, fmt.Errorf("failed reading key %s: %w", key, storage.ErrNotFound)
		}
		return nil, fmt.Errorf("failed to read object: %w", err)
	}

	defer output.Body.Close()
	return io.ReadAll(output.Body)
}

func (l *S3Location) Copy(sourceURI string, destination string) error {
	sourceKey := resolveKey(l.prefix, sourceURI)
	destKey := resolveKey(l.prefix, destination)

	copySource := l.bucket + "/" + sourceKey
	_, err := l.s3.CopyObject(context.TODO(), &s3.CopyObjectInput{
		CopySource: &copySource,
		Bucket:     &l.bucket,
		Key:        &destKey,
	})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return storage.ErrNotFound
		}
		return fmt.Errorf("failed to copy object: %w", err)
	}
	return nil
}

// List returns the result of a ListObjectsV2 call to S3. It doesn't paginate
// so it returns up to 1,000 objects.
func (l *S3Location) List() iter.Seq2[string, error] {
	output, err := l.s3.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: &l.bucket,
		Prefix: &l.prefix,
	})
	return func(yield func(string, error) bool) {
		if err != nil {
			yield("", err)
			return
		}

		// Convert object keys to URIs
		for _, obj := range output.Contents {
			if !yield(s3URI(l.bucket, *obj.Key), nil) {
				return
			}
		}
	}
}

func (l *S3Location) Remove(paths ...string) error {
	var compositeErr error
	for _, path := range paths {
		key := resolveKey(l.prefix, path)
		_, err := l.s3.DeleteObject(context.TODO(), &s3.DeleteObjectInput{
			Bucket: &l.bucket,
			Key:    &key,
		})
		if err != nil {
			errors.Join(compositeErr, fmt.Errorf("failed to delete object %s: %w", path, err))
		}
	}

	return compositeErr
}

func (l *S3Location) URI(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", errors.New("path cannot be empty")
	}

	key := resolveKey(l.prefix, path)
	_, err := l.s3.GetObject(context.TODO(), &s3.GetObjectInput{
		Bucket: &l.bucket,
		Key:    &key,
	})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return "", storage.ErrNotFound
		}
		return "", err
	}

	return s3URI(l.bucket, key), nil
}

var _ storage.FileStore = (*S3Location)(nil)

// resolveKey returns an absolute bucket key. The prefix is added to relative
// paths while URIs have the protocol and bucket name removed.
//
// example:
//
//	resolveKey("prefix/", "s3://bucket/path") => "bucket/path"
//	resolveKey("prefix/", "path") => "prefix/path"
func resolveKey(prefix string, path string) string {
	// Handle URIs
	if strings.HasPrefix(path, "s3://") {
		_, after, found := strings.Cut(strings.TrimPrefix(path, "s3://"), "/")
		if !found {
			return path
		}
		return after
	}

	// Handle regular paths by joining with prefix
	return prefix + path
}

func s3URI(bucket, key string) string {
	return fmt.Sprintf("s3://%s/%s", bucket, key)
}
