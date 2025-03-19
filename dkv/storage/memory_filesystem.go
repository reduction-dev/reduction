package storage

import (
	"bytes"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"reduction.dev/reduction/util/ds"
)

type MemoryFilesystem struct {
	files      *ds.LockingMap[string, *MemoryFile] // map[string]*MemoryFile
	workingDir string
}

const memoryProtocol = "memory://"

func NewMemoryFilesystem() *MemoryFilesystem {
	return &MemoryFilesystem{
		files:      ds.NewLockingMap[string, *MemoryFile](),
		workingDir: "/",
	}
}

func (fs *MemoryFilesystem) New(path string) File {
	return &MemoryFile{
		path:     fs.normalizePath(path),
		fs:       fs,
		writer:   &bytes.Buffer{},
		fileMode: FILE_MODE_WRITE,
		mu:       &sync.RWMutex{},
	}
}

func (fs *MemoryFilesystem) Open(path string) File {
	return &MemoryFile{
		path:     fs.normalizePath(path),
		fs:       fs,
		fileMode: FILE_MODE_READ,
		mu:       &sync.RWMutex{},
	}
}

func (fs *MemoryFilesystem) Copy(source string, destination string) error {
	source = fs.normalizePath(source)
	f, ok := fs.files.Get(source)
	if !ok {
		return fmt.Errorf("missing source file %s", source)
	}

	destination = fs.normalizePath(destination)
	fs.files.Put(destination, f)
	return nil
}

func (fs *MemoryFilesystem) List() []string {
	// Allocate capacity, but don't rely on files size in case
	// files change while we are iterating.
	paths := make([]string, 0, fs.files.Size())
	for path := range fs.files.All() {
		if strings.HasPrefix(path, fs.workingDir) {
			relativePath, err := filepath.Rel(fs.workingDir, path)
			if err != nil {
				panic(err)
			}
			paths = append(paths, relativePath)
		}
	}
	slices.Sort(paths)
	return paths
}

func (fs *MemoryFilesystem) Exists(path string) bool {
	_, ok := fs.files.Get(fs.normalizePath(path))
	return ok
}

func (fs *MemoryFilesystem) normalizePath(path string) string {
	path = strings.TrimPrefix(path, memoryProtocol)
	if !filepath.IsAbs(path) {
		path = filepath.Join(fs.workingDir, path)
	}
	return path
}

func (fs *MemoryFilesystem) WithWorkingDir(path string) *MemoryFilesystem {
	newFS := NewMemoryFilesystem()
	newFS.files = fs.files
	if filepath.IsAbs(path) {
		newFS.workingDir = path
	} else {
		newFS.workingDir = filepath.Join(fs.workingDir, path)
	}
	return newFS
}

var _ FileSystem = (*MemoryFilesystem)(nil)

type MemoryFile struct {
	path     string
	fs       *MemoryFilesystem
	buf      []byte
	mu       *sync.RWMutex
	writer   *bytes.Buffer
	reader   *bytes.Reader
	size     int64
	fileMode FileMode
	didLoad  bool
}

func (m *MemoryFile) ReadAt(p []byte, off int64) (n int, err error) {
	m.mu.Lock()
	if !m.didLoad {
		file, ok := m.fs.files.Get(m.path)
		if !ok {
			m.mu.Unlock()
			return 0, fmt.Errorf("no memory file named %s: %w", m.path, ErrNotFound)
		}
		m.buf = file.buf
		m.reader = bytes.NewReader(m.buf)
		m.didLoad = true
	}
	m.mu.Unlock()

	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.reader.ReadAt(p, off)
}

func (m *MemoryFile) Write(p []byte) (n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.fileMode == FILE_MODE_READ {
		panic("tried to write to a read only file")
	}
	n, err = m.writer.Write(p)
	m.size += int64(n)
	return n, err
}

func (m *MemoryFile) Delete() error {
	if m.fileMode == FILE_MODE_WRITE {
		panic("tried to delete a file being written")
	}
	m.fs.files.Delete(m.path)
	return nil
}

func (m *MemoryFile) Save() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fileMode == FILE_MODE_READ {
		panic("tried to save a read only file")
	}
	m.fileMode = FILE_MODE_READ
	m.buf = m.writer.Bytes()
	m.fs.files.Put(m.path, m)
	m.reader = bytes.NewReader(m.buf)
	return nil
}

func (m *MemoryFile) Size() int64 {
	return m.size
}

func (m *MemoryFile) Name() string {
	return filepath.Base(m.path)
}

func (m *MemoryFile) URI() string {
	return memoryProtocol + m.path
}

func (m *MemoryFile) CreateDeleteFunc() func() error {
	fs := m.fs
	path := m.path
	return func() error {
		fs.files.Delete(path)
		return nil
	}
}

var _ File = (*MemoryFile)(nil)
