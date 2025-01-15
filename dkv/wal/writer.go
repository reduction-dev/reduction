package wal

import (
	"fmt"
	"io"

	"reduction.dev/reduction/dkv/fields"
	"reduction.dev/reduction/dkv/storage"
)

type Writer struct {
	file          storage.File     // The working file
	id            int              // The file number which increases by 1 each time the log is rotated
	activeBuffer  *bufferSegment   // The buffer to write to
	sealedBuffers []*bufferSegment // Buffers pending truncation
	latestSeqNum  uint64           // Latest written seq number recorded in segments during Cut
}

func NewWriter(fs storage.FileSystem, id int) *Writer {
	return &Writer{
		file:          fs.New(FileName(id)),
		id:            id,
		activeBuffer:  &bufferSegment{},
		sealedBuffers: []*bufferSegment{},
	}
}

func (w *Writer) Put(key []byte, value []byte, seqNum uint64) {
	buf := w.activeBuffer
	_ = fields.MustWriteUint64(buf, seqNum)
	_ = fields.MustWriteVarBytes(buf, key)
	_ = fields.MustWriteTombstone(buf, false)
	_ = fields.MustWriteVarBytes(buf, value)
	w.latestSeqNum = seqNum
}

func (w *Writer) Delete(key []byte, seqNum uint64) {
	buf := w.activeBuffer
	_ = fields.MustWriteUint64(buf, seqNum)
	_ = fields.MustWriteVarBytes(buf, key)
	_ = fields.MustWriteTombstone(buf, true)
	w.latestSeqNum = seqNum
}

func (w *Writer) Rotate(fs storage.FileSystem) *Writer {
	nextLog := NewWriter(fs, w.id+1)
	nextLog.sealedBuffers = make([]*bufferSegment, len(w.sealedBuffers)+1)

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
	w.activeBuffer.latestSeqNum = w.latestSeqNum
	w.sealedBuffers = append(w.sealedBuffers, w.activeBuffer)
	w.activeBuffer = &bufferSegment{}
}

// Truncate up to and including the given sequence number
func (w *Writer) Truncate(seqNum uint64) {
	for i, buf := range w.sealedBuffers {
		if buf.latestSeqNum > seqNum {
			// Truncate the obsolete cuts
			w.sealedBuffers = w.sealedBuffers[i:]
			return
		}
	}
}

// Save the log to durable storage
func (w *Writer) Save() error {
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

func (w *Writer) Handle(after uint64) Handle {
	return Handle{
		ID:    w.id,
		file:  w.file,
		After: after,
	}
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
