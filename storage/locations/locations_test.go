package locations_test

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"reduction.dev/reduction/storage/locations"
	"reduction.dev/reduction/storage/objstore"
)

func TestNewLocation_S3Path(t *testing.T) {
	store, err := locations.New("s3://my-bucket/some/path")
	assert.NoError(t, err, "creating store with S3 path should not error")

	// Verify we got an S3 location back
	_, ok := store.(*locations.S3Location)
	assert.True(t, ok, "store should be an S3Location for s3:// paths")
}

func TestNewLocation_LocalPath(t *testing.T) {
	store, err := locations.New("/local/path")
	assert.NoError(t, err, "creating store with local path should not error")

	// Verify we got a local directory back
	_, ok := store.(*locations.LocalDirectory)
	assert.True(t, ok, "store should be a Directory for local paths")
}

func TestLocalDirectory(t *testing.T) {
	locationStoreSuite(t, func() locations.StorageLocation {
		return locations.NewLocalDirectory(t.TempDir())
	})
}

func TestS3Location(t *testing.T) {
	locationStoreSuite(t, func() locations.StorageLocation {
		loc, err := locations.NewS3Location(objstore.NewMemoryS3Service(), "s3://bucket/prefix")
		require.NoError(t, err, "creating S3 location should not return an error")
		return loc
	})
}

func locationStoreSuite(t *testing.T, newLoc func() locations.StorageLocation) {
	t.Run("WriteThenRead", func(t *testing.T) {
		loc := newLoc()

		// Write test data to a file
		testData := []byte("test data")
		path, err := loc.Write("test.txt", bytes.NewReader(testData))
		require.NoError(t, err, "write operation should not return an error")

		// Verify file exists and has correct content
		content, err := loc.Read(path)
		require.NoError(t, err, "should be able to read the created file")
		assert.Equal(t, testData, content, "file should contain the written data")
	})

	t.Run("ReadNonExistent", func(t *testing.T) {
		loc := newLoc()
		content, err := loc.Read("nonexistent.txt")
		assert.Error(t, err, "reading a non-existent file should return an error")
		assert.Nil(t, content, "content should be nil for non-existent file")
	})

	t.Run("Remove", func(t *testing.T) {
		loc := newLoc()

		// Create test files
		testData := []byte("test data")
		absPath1, _ := loc.Write("doomed1.txt", bytes.NewReader(testData))
		absPath2, _ := loc.Write("doomed2.txt", bytes.NewReader(testData))

		// Get one relative path
		relPath2 := filepath.Base(absPath2)

		// Remove one file with absolute path and one with relative path
		err := loc.Remove(absPath1, relPath2)
		assert.NoError(t, err, "removing existing files should not return an error")

		// Verify files no longer exist
		_, err1 := loc.Read(absPath1)
		_, err2 := loc.Read(absPath2)
		assert.ErrorIs(t, err1, locations.ErrNotFound, "first file should no longer exist")
		assert.ErrorIs(t, err2, locations.ErrNotFound, "second file should no longer exist")
	})

	t.Run("RemoveNonExistent", func(t *testing.T) {
		err := newLoc().Remove("nonexistent.txt")
		assert.NoError(t, err, "removing a non-existent file should not error")
	})

	t.Run("List", func(t *testing.T) {
		loc := newLoc()

		testFiles := []string{"file1.txt", "file2.txt", "nested/file3.txt"}
		for _, file := range testFiles {
			loc.Write(file, bytes.NewReader([]byte("test data")))
		}

		// List uris
		var uris []string
		for s, err := range loc.List() {
			assert.NoError(t, err, "list iterator should not return an error")
			uris = append(uris, s)
		}

		mustURI := func(path string) string {
			uri, err := loc.URI(path)
			assert.NoError(t, err, "getting URI for file should not return an error")
			return uri
		}

		// Verify all files are listed
		assert.Equal(t,
			[]string{
				mustURI("file1.txt"),
				mustURI("file2.txt"),
				mustURI("nested/file3.txt"),
			}, uris,
			"list should return all expected file paths")
	})

	t.Run("URINonExistent", func(t *testing.T) {
		_, err := newLoc().URI("nonexistent.txt")
		assert.ErrorIs(t, err, locations.ErrNotFound, "error should be ErrNotFound")
	})

	t.Run("Copy", func(t *testing.T) {
		loc := newLoc()

		// Create a source file
		testData := []byte("test data for copying")
		sourcePath, err := loc.Write("source.txt", bytes.NewReader(testData))
		require.NoError(t, err)

		// Copy the file
		destPath := "destination.txt"
		err = loc.Copy(sourcePath, destPath)
		require.NoError(t, err)

		// Read destination file with relative path and check the content.
		relativeContent, err := loc.Read(destPath)
		require.NoError(t, err)
		assert.Equal(t, testData, relativeContent, "content at relative path should match source")

		// Read destination file with URI path and check the content.
		destURI, err := loc.URI(destPath)
		require.NoError(t, err)
		uriContent, err := loc.Read(destURI)
		require.NoError(t, err)
		assert.Equal(t, testData, uriContent, "content at URI should match source")
	})

	t.Run("CopyNonExistent", func(t *testing.T) {
		err := newLoc().Copy("nonexistent.txt", "destination.txt")
		assert.ErrorIs(t, err, locations.ErrNotFound, "copying a non-existent file should return an error")
	})
}
