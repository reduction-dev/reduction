package storage_test

import (
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/dkv/dkvtest"
	"reduction.dev/reduction/dkv/storage"
)

func TestLocalFilesystem(t *testing.T) {
	fs := storage.NewLocalFilesystem(t.TempDir())
	FileSystemSemanticsSuite(t, fs)
}

func TestMemoryFilesystem(t *testing.T) {
	fs := storage.NewMemoryFilesystem()
	FileSystemSemanticsSuite(t, fs)
}

func TestS3FileSystem(t *testing.T) {
	s3Service := dkvtest.NewMemoryS3Service()
	fs := storage.NewS3FileSystem(&s3Service, "bucket")
	FileSystemSemanticsSuite(t, fs)
}

func FileSystemSemanticsSuite(t *testing.T, fs storage.FileSystem) {
	t.Run("Creating new file", func(t *testing.T) {
		f := fs.New("file.txt")
		assert.Equal(t, "file.txt", f.Name())

		_, err := fs.Open("file.txt").ReadAt(nil, 0)
		assert.ErrorIs(t, err, storage.ErrNotFound, "files aren't created until saved")
	})

	t.Run("Writing to file", func(t *testing.T) {
		f := fs.New("file.txt")
		n, err := f.Write([]byte{1}) // Write a single byte
		assert.NoError(t, err)
		assert.Equal(t, 1, n)

		err = f.Save()
		assert.NoError(t, err)

		assert.Panics(t, func() { f.Write([]byte{1}) }, "writing is not allowed after save")
		assert.Panics(t, func() { f.Save() }, "save is not allowed after save")

		b := []byte{0}
		n, err = f.ReadAt(b, 0)
		assert.Equal(t, []byte{1}, b, "can read after save")
		assert.Equal(t, 1, n)
		assert.NoError(t, err)
	})

	t.Run("Opening an existing file", func(t *testing.T) {
		// Create a file with one byte
		f := fs.New("file.txt")
		_, err := f.Write([]byte{1})
		require.NoError(t, err)
		err = f.Save()
		require.NoError(t, err)

		f = fs.Open(f.Name())
		assert.Panics(t, func() { f.Write([]byte{1}) }, "writing is not allowed")
		assert.Panics(t, func() { f.Save() }, "save is not allowed")

		b := []byte{0}
		n, err := f.ReadAt(b, 0)
		assert.Equal(t, []byte{1}, b, "can read after opening")
		assert.Equal(t, 1, n, "read correct value from file")
		assert.NoError(t, err, "no error reading from file")

		b = []byte{0, 0}
		n, err = f.ReadAt(b, 0)
		assert.Equal(t, []byte{1, 0}, b, "reading beyond EOF reads available bytes")
		assert.Equal(t, 1, n)
		assert.ErrorIs(t, err, io.EOF, "reading beyond EOF returns EOF error")
	})

	t.Run("Copying", func(t *testing.T) {
		// Create a file with one byte
		f := fs.New("file.txt")
		_, err := f.Write([]byte{1})
		require.NoError(t, err)
		err = f.Save()
		require.NoError(t, err)

		// Copy file to new location
		err = fs.Copy(f.URI(), "file-copy.txt")
		require.NoError(t, err)

		// Read the contents of the copied file
		copy := fs.Open("file-copy.txt")
		b := []byte{0}
		n, err := copy.ReadAt(b, 0)
		assert.NoError(t, err)
		assert.Equal(t, n, 1)
		assert.Equal(t, []byte{1}, b, "read copy contents")
	})

	t.Run("Creating file with / in path", func(t *testing.T) {
		f := fs.New("dir/file.txt")
		n, err := f.Write([]byte{1})
		assert.NoError(t, err)
		assert.Equal(t, 1, n)

		err = f.Save()
		require.NoError(t, err)

		// Read file contents
		b := []byte{0}
		n, err = f.ReadAt(b, 0)
		assert.Equal(t, []byte{1}, b, "can read after sync")
		assert.Equal(t, 1, n)
		assert.NoError(t, err)
	})

	t.Run("Saving empty file", func(t *testing.T) {
		f := fs.New("file.txt")
		assert.NoError(t, f.Save(), "saving empty file is ok")

		f = fs.Open(f.Name())
		b := []byte{0}
		n, err := f.ReadAt(b, 0)
		assert.Equal(t, n, 0, "reads zero bytes")
		assert.ErrorIs(t, err, io.EOF, "read returns EOF")
	})
}
