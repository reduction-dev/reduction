package storage

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

type LocalFilesystem struct {
	Dir string
}

func NewLocalFilesystem(dir string) *LocalFilesystem {
	mkdir := exec.Command("mkdir", "-p", dir)
	if out, err := mkdir.CombinedOutput(); err != nil {
		panic(fmt.Sprintf("creating local filesystem: %s, %v", out, err))
	}

	return &LocalFilesystem{dir}
}

func (fs *LocalFilesystem) New(path string) File {
	if filepath.IsAbs(path) {
		panic(fmt.Sprintf("creating a file with absolute path (%s) not supported", path))
	}
	pathDir := filepath.Dir(path)
	return &DiskFile{
		name:     filepath.Base(path),
		dir:      filepath.Join(fs.Dir, pathDir),
		fileMode: FILE_MODE_WRITE,
	}
}

func (fs *LocalFilesystem) Open(path string) File {
	var dir string
	if filepath.IsAbs(path) {
		dir = filepath.Dir(path)
	} else {
		dir = filepath.Join(fs.Dir, filepath.Dir(path))
	}

	return &DiskFile{
		name:     filepath.Base(path),
		dir:      dir,
		fileMode: FILE_MODE_READ,
	}
}

func (fs *LocalFilesystem) List() []string {
	var fileNames []string
	err := filepath.WalkDir(fs.Dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		// Only include files (not their directories)
		if !d.IsDir() {
			fileNames = append(fileNames, d.Name())
		}
		return nil
	})

	if err != nil {
		panic(err)
	}
	return fileNames
}

func (fs *LocalFilesystem) Copy(sourceURI string, destination string) error {
	destinationPath := filepath.Join(fs.Dir, destination)

	// First make sure the destination path exists
	mkdir := exec.Command("mkdir", "-p", filepath.Dir(destinationPath))
	if _, err := mkdir.CombinedOutput(); err != nil {
		panic(err)
	}

	// Then do a clone copy
	cmd := exec.Command("cp", "-c", sourceURI, destinationPath)
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("cp command %s: %w", out, err)
	}

	return nil
}

var _ FileSystem = (*LocalFilesystem)(nil)

type DiskFile struct {
	osFile   *os.File
	name     string
	dir      string
	fileMode FileMode
	size     int64
}

func (d *DiskFile) ReadAt(b []byte, off int64) (n int, err error) {
	file, err := d.openFile()
	if err != nil {
		return 0, err
	}
	return file.ReadAt(b, off)
}

func (d *DiskFile) Save() error {
	if d.fileMode == FILE_MODE_READ {
		panic("tried to save a read only file")
	}
	file, err := d.tmpFile()
	if err != nil {
		return err
	}
	file.Sync()
	if err := os.Rename(file.Name(), filepath.Join(d.dir, d.name)); err != nil {
		return err
	}
	d.fileMode = FILE_MODE_READ
	return nil
}

// Lazily open the file
func (d *DiskFile) openFile() (*os.File, error) {
	if d.osFile != nil {
		return d.osFile, nil
	}
	f, err := os.Open(filepath.Join(d.dir, d.name))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	d.osFile = f
	return f, nil
}

// Lazily create the tmp file
func (d *DiskFile) tmpFile() (*os.File, error) {
	if d.osFile != nil {
		return d.osFile, nil
	}
	mkdir := exec.Command("mkdir", "-p", d.dir)
	if _, err := mkdir.CombinedOutput(); err != nil {
		panic(err)
	}
	f, err := os.CreateTemp(d.dir, d.name)
	if err != nil {
		return nil, err
	}
	d.osFile = f
	return f, nil
}

func (d *DiskFile) Write(b []byte) (n int, err error) {
	if d.fileMode == FILE_MODE_READ {
		panic("tried to write to a read only file")
	}
	file, err := d.tmpFile()
	if err != nil {
		return 0, err
	}
	n, err = file.Write(b)
	d.size += int64(n)
	return n, err
}

func (d *DiskFile) Delete() error {
	return os.Remove(d.osFile.Name())
}

func (d *DiskFile) Size() int64 {
	return d.size
}

func (d *DiskFile) Name() string {
	return d.name
}

func (d *DiskFile) URI() string {
	localPath := filepath.Join(d.dir, d.name)
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		panic(fmt.Sprintf("failed to get absolute path to %s: %v", localPath, err))
	}
	return absPath
}

var _ File = (*DiskFile)(nil)
