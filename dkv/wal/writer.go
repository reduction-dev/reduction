package wal

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"reduction.dev/reduction/dkv/fields"
	"reduction.dev/reduction/dkv/storage"
)

type Writer struct {
	file          storage.File     // The working file
	id            int              // The file number which increases by 1 each time the log is rotated
	activeBuffer  *bufferSegment   // The buffer to write to
	sealedBuffers []*bufferSegment // Buffers pending truncation
	latestSeqNum  uint64           // Latest written seq number recorded in segments during Cut
	maxSize       uint64           // Maximum size of the log file
	sealed        atomic.Bool      // Whether the writer is immutable
	mu            sync.Mutex       // Protects sealedBuffers for concurrent operations on unsealed writers
}

// NewWriter creates a new WAL writer.
//
// Concurrency caller contract:
// Called synchronously by the same goroutine:
//  1. Put/Delete - writing entries
//  2. Cut - sync phase of memtable rotation
//  3. Rotate - sync checkpoint phase
//  4. Handle - sync checkpoint phase
//
// Called asynchronously:
// 1. Truncate - async rotating memtables phase
// 2. Save - async checkpointing phase
func NewWriter(fs storage.FileSystem, id int, maxSize uint64) *Writer {
	return &Writer{
		file:          fs.New(FileName(id)),
		id:            id,
		activeBuffer:  &bufferSegment{},
		sealedBuffers: []*bufferSegment{},
		maxSize:       maxSize,
	}
}

/* Start non-concurrent operations */

func (w *Writer) Put(key []byte, value []byte, seqNum uint64) (full bool) {
	if w.sealed.Load() {
		panic("cannot write to a sealed writer")
	}

	buf := w.activeBuffer
	fields.MustWriteUint64(buf, seqNum)
	fields.MustWriteVarBytes(buf, key)
	fields.MustWriteTombstone(buf, false)
	fields.MustWriteVarBytes(buf, value)
	w.latestSeqNum = seqNum

	return uint64(len(buf.buf)) >= w.maxSize
}

func (w *Writer) Delete(key []byte, seqNum uint64) (full bool) {
	if w.sealed.Load() {
		panic("cannot write to a sealed writer")
	}

	buf := w.activeBuffer
	fields.MustWriteUint64(buf, seqNum)
	fields.MustWriteVarBytes(buf, key)
	fields.MustWriteTombstone(buf, true)
	w.latestSeqNum = seqNum

	return uint64(len(buf.buf)) >= w.maxSize
}

// Rotate returns a new writer for the next log file. The current writer is
// considered sealed and must not be modified after this call.
func (w *Writer) Rotate(fs storage.FileSystem) *Writer {
	if w.sealed.Load() {
		panic("cannot rotate a sealed writer")
	}

	// Add mutex lock to synchronize with Truncate()
	w.mu.Lock()
	defer w.mu.Unlock()

	w.sealed.Store(true)
	nextLog := NewWriter(fs, w.id+1, w.maxSize)
	nextLog.sealedBuffers = make([]*bufferSegment, len(w.sealedBuffers)+1)
	nextLog.maxSize = w.maxSize

	// Include all data from previous buffers
	for i, b := range w.sealedBuffers {
		nextLog.sealedBuffers[i] = &bufferSegment{buf: b.buf}
	}
	nextLog.sealedBuffers[len(w.sealedBuffers)] = &bufferSegment{buf: w.activeBuffer.buf}

	// And initialize a new active buffer
	nextLog.activeBuffer = &bufferSegment{}
	return nextLog
}

// Record the current internal buffer offset along with the provided sequence
// number. This offset will be used to truncate the log when memtables are
// written to sstables and make it to the current set of DB levels.
func (w *Writer) Cut() {
	if w.sealed.Load() {
		panic("cannot cut a sealed writer")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.activeBuffer.latestSeqNum = w.latestSeqNum
	w.sealedBuffers = append(w.sealedBuffers, w.activeBuffer)
	w.activeBuffer = &bufferSegment{}
}

func (w *Writer) Handle(after uint64) Handle {
	return Handle{
		ID:    w.id,
		file:  w.file,
		After: after,
	}
}

/* Start concurrent operations */

// Truncate up to and including the given sequence number
func (w *Writer) Truncate(seqNum uint64) {
	if w.sealed.Load() {
		panic("cannot truncate a sealed writer")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	truncateIndex := -1
	for i, buf := range w.sealedBuffers {
		if buf.latestSeqNum > seqNum {
			truncateIndex = i
			break
		}
	}

	// If every buffer is earlier than the target seqNum, clear all buffers.
	if truncateIndex == -1 {
		w.sealedBuffers = nil
		return
	}

	// Otherwise, truncate the buffers up to the target seqNum.
	w.sealedBuffers = w.sealedBuffers[truncateIndex:]
}

// Save the log to durable storage. WALs are immutable when they are saved.
func (w *Writer) Save() error {
	if !w.sealed.Load() {
		panic("writer must be sealed with Rotate() before saving")
	}

	// Write all sealed buffers to the file
	for _, buf := range w.sealedBuffers {
		if _, err := io.Copy(w.file, buf); err != nil {
			return err
		}
	}
	// Write the active buffer to the file
	if _, err := io.Copy(w.file, w.activeBuffer); err != nil {
		return err
	}
	return w.file.Save()
}

func FileName(id int) string {
	return fmt.Sprintf("%06d.wal", id)
}

// bufferSegment implements io.Reader, io.Writer and, unlike bytes.Buffer
// provides access to the underlying []byte so that a new bufferSegment can an
// existing bufferSegment's data.
type bufferSegment struct {
	buf          []byte
	latestSeqNum uint64
	readOffset   int // Keep track of read offset for io.Reader.Read.
}

// Write implements io.Writer
func (b *bufferSegment) Write(p []byte) (int, error) {
	b.buf = append(b.buf, p...)
	return len(p), nil
}

// Read implements io.Reader
func (b *bufferSegment) Read(p []byte) (n int, err error) {
	if len(b.buf) <= b.readOffset {
		return 0, io.EOF
	}
	n = copy(p, b.buf[b.readOffset:])
	b.readOffset += n
	return n, nil
}
