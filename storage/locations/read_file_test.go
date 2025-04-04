package locations_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/storage/locations"
	"reduction.dev/reduction/storage/objstore"
)

func TestReadLocalFile(t *testing.T) {
	// Create a temporary file with test content
	tempDir := t.TempDir()
	testContent := []byte("test file content")
	filePath := filepath.Join(tempDir, "test-read-local.txt")
	err := os.WriteFile(filePath, testContent, 0644)
	require.NoError(t, err, "prereq: writing test file should not return an error")

	// Test reading existing file
	content, err := locations.ReadLocalFile(filePath)
	assert.NoError(t, err, "reading existing file should not return an error")
	assert.Equal(t, testContent, content, "file content should match what was written")

	// Test reading non-existent file
	nonExistentPath := filepath.Join(tempDir, "nonexistent.txt")
	content, err = locations.ReadLocalFile(nonExistentPath)
	assert.ErrorIs(t, err, locations.ErrNotFound, "reading non-existent file should return ErrNotFound")
	assert.Nil(t, content, "content should be nil for non-existent file")
}

func TestReadS3File(t *testing.T) {
	// Setup memory S3 service for testing
	s3Service := objstore.NewMemoryS3Service()
	bucketName := "test-bucket"
	key := "test/file.txt"
	testContent := []byte("test s3 content")

	// Create a test file in the memory S3 service
	_, err := s3Service.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: &bucketName,
		Key:    &key,
		Body:   bytes.NewReader(testContent),
	})
	require.NoError(t, err, "putting test object should not return an error")

	// Test reading existing S3 file
	s3Path := "s3://" + bucketName + "/" + key
	content, err := locations.ReadS3File(s3Service, s3Path)
	assert.NoError(t, err, "reading existing S3 file should not return an error")
	assert.Equal(t, testContent, content, "S3 file content should match what was uploaded")

	// Test reading non-existent S3 file
	_, err = locations.ReadS3File(s3Service, "s3://"+bucketName+"/test/nonexistent.txt")
	assert.ErrorIs(t, err, locations.ErrNotFound, "reading non-existent S3 file should return an error")

	// Test invalid S3 path format
	invalidPath := "s3://invalid-format"
	_, err = locations.ReadS3File(s3Service, invalidPath)
	assert.Error(t, err, "reading with invalid S3 path format should return an error")
}
