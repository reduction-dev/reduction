package storage

import "io"

func ReadAll(file File) ([]byte, error) {
	cursor := &Cursor{File: file}
	return io.ReadAll(cursor)
}
