package localfs

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"os"
	"os/exec"
	"path/filepath"

	"reduction.dev/reduction/storage"
)

type Directory struct {
	Path          string
	subscriptions []chan storage.FileEvent
}

func NewDirectory(path string) *Directory {
	return &Directory{
		Path:          path,
		subscriptions: make([]chan storage.FileEvent, 0),
	}
}

func NewInWorkingDirectory(path string) *Directory {
	wd, err := os.Getwd()
	if err != nil {
		panic(fmt.Errorf("failed getting working directory: %w", err))
	}
	return NewDirectory(wd + "/" + path)
}

func (d *Directory) Write(fname string, reader io.Reader) (string, error) {
	fullPath := filepath.Join(d.Path, fname)
	destDir := filepath.Dir(fullPath)

	err := os.MkdirAll(destDir, 0777)
	if err != nil {
		return "", fmt.Errorf("Directory.Write error creating directory %s: %w", d.Path, err)
	}

	targetFile, err := os.Create(fullPath)
	if err != nil {
		return "", fmt.Errorf("Directory.Write creating file %s: %w", fullPath, err)
	}

	_, err = io.Copy(targetFile, reader)
	if err != nil {
		return "", err
	}
	err = targetFile.Close()
	if err != nil {
		return "", err
	}

	for _, s := range d.subscriptions {
		s <- storage.FileEvent{
			Path: targetFile.Name(),
			Op:   storage.OpCreate,
		}
	}
	return targetFile.Name(), nil
}

// Read accepts both relative and absolute paths or URIs.
// If a relative path is provided, it will be resolved relative to the Directory's Path.
func (d *Directory) Read(filePath string) ([]byte, error) {
	fullFilePath := filePath
	if !filepath.IsAbs(filePath) {
		fullFilePath = filepath.Join(d.Path, filePath)
	}

	f, err := os.Open(fullFilePath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (d *Directory) Remove(paths ...string) error {
	for _, path := range paths {
		fullPath := path
		if !filepath.IsAbs(path) {
			fullPath = filepath.Join(d.Path, path)
		}

		err := os.Remove(fullPath)
		if err != nil {
			pathErr := &os.PathError{}
			if errors.As(err, &pathErr) {
				// Ignore path errors to match how S3 doesn't error if a file
				// doesn't exist.
				return nil
			}
			return fmt.Errorf("removing file %s: %w", fullPath, err)
		}

		// Notify subscribers about the file deletion
		for _, s := range d.subscriptions {
			s <- storage.FileEvent{
				Path: fullPath,
				Op:   storage.OpRemove,
			}
		}
	}

	return nil
}

func (d *Directory) List() iter.Seq2[string, error] {
	errStop := errors.New("walk-dir-stop")

	return func(yield func(string, error) bool) {
		err := filepath.WalkDir(d.Path, func(p string, d os.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return errStop
				}
				return err
			}
			if !d.IsDir() {
				if !yield(p, nil) {
					return errStop
				}
			}
			return nil
		})
		if err != nil {
			if errors.Is(err, errStop) {
				return
			}
			yield("", err)
		}
	}
}

func (d *Directory) URI(fname string) (string, error) {
	fullPath := d.Path + "/" + fname
	_, err := os.Stat(fullPath)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", storage.ErrNotFound
		}
		return "", err
	}
	return fullPath, nil
}

func (d *Directory) Copy(sourceURI string, destination string) error {
	destinationPath := destination
	if !filepath.IsAbs(destination) {
		destinationPath = filepath.Join(d.Path, destination)
	}

	// Check if the source file exists
	if _, err := os.Stat(sourceURI); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return storage.ErrNotFound
		}
		return fmt.Errorf("source file %s: %w", sourceURI, err)
	}

	// First make sure the destination path exists
	mkdir := exec.Command("mkdir", "-p", filepath.Dir(destinationPath))
	if _, err := mkdir.CombinedOutput(); err != nil {
		panic(err)
	}

	// Then do a copy
	cmd := exec.Command("cp", sourceURI, destinationPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("cp command %s: %w", out, err)
	}
	return nil
}

func (d *Directory) Subscribe() <-chan storage.FileEvent {
	ch := make(chan storage.FileEvent)
	d.subscriptions = append(d.subscriptions, ch)
	return ch
}

var _ storage.FileStore = (*Directory)(nil)
