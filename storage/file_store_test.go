package storage_test

import (
	"bytes"
	"io/fs"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/storage"
	"reduction.dev/reduction/storage/localfs"
)

func TestLocalDirectory(t *testing.T) {
	fileStoreSuite(t, func() storage.FileStore {
		return localfs.NewDirectory(t.TempDir())
	})
}

func fileStoreSuite(t *testing.T, newDir func() storage.FileStore) {
	t.Run("WriteThenRead", func(t *testing.T) {
		dir := newDir()

		// Write test data to a file
		testData := []byte("test data")
		path, err := dir.Write("test.txt", bytes.NewReader(testData))
		require.NoError(t, err, "write operation should not return an error")

		// Verify file exists and has correct content
		content, err := dir.Read(path)
		require.NoError(t, err, "should be able to read the created file")
		assert.Equal(t, testData, content, "file should contain the written data")
	})

	t.Run("ReadNonExistent", func(t *testing.T) {
		dir := newDir()
		content, err := dir.Read("nonexistent.txt")
		assert.Error(t, err, "reading a non-existent file should return an error")
		assert.Nil(t, content, "content should be nil for non-existent file")
	})

	t.Run("Remove", func(t *testing.T) {
		dir := newDir()

		// Create test files
		testData := []byte("test data")
		absPath1, _ := dir.Write("doomed1.txt", bytes.NewReader(testData))
		absPath2, _ := dir.Write("doomed2.txt", bytes.NewReader(testData))

		// Get one relative path
		relPath2 := filepath.Base(absPath2)

		// Remove one file with absolute path and one with relative path
		err := dir.Remove(absPath1, relPath2)

		assert.NoError(t, err, "removing existing files should not return an error")

		// Verify files no longer exist
		_, err1 := dir.Read(absPath1)
		_, err2 := dir.Read(absPath2)
		pathErr := &fs.PathError{}
		assert.ErrorAs(t, err1, &pathErr, "first file should no longer exist")
		assert.ErrorAs(t, err2, &pathErr, "second file should no longer exist")
	})

	t.Run("RemoveNonExistent", func(t *testing.T) {
		// Try to remove a non-existent file
		err := newDir().Remove("nonexistent.txt")

		pathErr := &fs.PathError{}
		assert.ErrorAs(t, err, &pathErr, "removing a non-existent file should return an error")
	})

	t.Run("List", func(t *testing.T) {
		dir := newDir()

		testFiles := []string{"file1.txt", "file2.txt", "nested/file3.txt"}
		for _, file := range testFiles {
			dir.Write(file, bytes.NewReader([]byte("test data")))
		}

		// List uris
		var uris []string
		for s, err := range dir.List() {
			assert.NoError(t, err, "list iterator should not return an error")
			uris = append(uris, s)
		}

		mustURI := func(path string) string {
			uri, err := dir.URI(path)
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
		_, err := newDir().URI("nonexistent.txt")
		assert.ErrorIs(t, err, fs.ErrNotExist, "error should be fs.ErrNotExist")
	})

	t.Run("Copy", func(t *testing.T) {
		dir := newDir()

		// Create a source file
		testData := []byte("test data for copying")
		sourcePath, err := dir.Write("source.txt", bytes.NewReader(testData))
		require.NoError(t, err)

		// Copy the file
		destPath := "destination.txt"
		err = dir.Copy(sourcePath, destPath)
		require.NoError(t, err)

		// Read destination file with relative path and check the content.
		relativeContent, err := dir.Read(destPath)
		require.NoError(t, err)
		assert.Equal(t, testData, relativeContent, "content at relative path should match source")

		// Read destination file with URI path and check the content.
		destURI, err := dir.URI(destPath)
		require.NoError(t, err)
		uriContent, err := dir.Read(destURI)
		require.NoError(t, err)
		assert.Equal(t, testData, uriContent, "content at URI should match source")
	})
}
