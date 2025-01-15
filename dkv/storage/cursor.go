package storage

import (
	"fmt"
	"io"
	"runtime/debug"
)

type Cursor struct {
	File       io.ReaderAt
	offset     int64
	debug      bool
	startBound uint64
	endBound   uint64
}

func NewBoundedCursor(reader io.ReaderAt, start, end uint64) *Cursor {
	return &Cursor{File: reader, startBound: start, endBound: end}
}

func (c *Cursor) Move(offset int64) {
	if c.endBound != 0 {
		if offset < int64(c.startBound) {
			panic(fmt.Sprintf("cursor move to %d before start bound %d", offset, c.startBound))
		}

		if offset > int64(c.endBound) {
			panic(fmt.Sprintf("cursor move to %d after end bound %d", offset, c.endBound))
		}
	}

	c.offset = offset
}

// Read implements io.Reader.
func (c *Cursor) Read(p []byte) (n int, err error) {
	if c.endBound != 0 {
		if c.offset >= int64(c.endBound) {
			return 0, io.EOF
		}
		// Limit the amount to read to avoid going over the bounds
		if max := c.endBound - uint64(c.offset); uint64(len(p)) > max {
			p = p[0:max]
		}
	}

	n, err = c.File.ReadAt(p, c.offset)
	if err != nil {
		if c.debug {
			return n, fmt.Errorf("cursor read: %w, trace: %s", err, debug.Stack())
		}
		return n, err
	}
	c.offset += int64(n)
	return n, nil
}

func (c *Cursor) Offset() int64 { return c.offset }

// Set a debug flag to create stack traces with errors
func (c *Cursor) Debug() { c.debug = true }

var _ io.Reader = (*Cursor)(nil)
