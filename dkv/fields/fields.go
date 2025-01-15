package fields

import (
	"encoding/binary"
	"io"
)

// VarBytes

func writeVarBytes(w io.Writer, data []byte) (n int, err error) {
	// Write key length
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, uint32(len(data)))
	if n, err = w.Write(b); err != nil {
		return n, err
	}

	// Write key value
	n2, err := w.Write([]byte(data))
	n += n2
	return
}

func MustWriteVarBytes(w io.Writer, key []byte) int {
	n, err := writeVarBytes(w, key)
	if err != nil {
		panic(err)
	}
	return n
}

func ReadVarBytes(r io.Reader) ([]byte, error) {
	// Read key length
	lenData := make([]byte, 4)
	if _, err := io.ReadFull(r, lenData); err != nil {
		return nil, err
	}

	// Read value
	valueData := make([]byte, binary.LittleEndian.Uint32(lenData))
	if _, err := io.ReadFull(r, valueData); err != nil {
		return nil, err
	}

	return valueData, nil
}

func SkipVarBytes(r io.Reader) error {
	// Read value length
	lenData := make([]byte, 4)
	if _, err := io.ReadFull(r, lenData); err != nil {
		return err
	}

	// Skip over the value
	_, err := io.CopyN(io.Discard, r, int64(binary.LittleEndian.Uint32(lenData)))
	return err
}

// Tombstone

func writeTombstone(w io.Writer, deleted bool) (int, error) {
	b := make([]byte, 1)
	if deleted {
		b[0] = 1
	}
	return w.Write(b)
}

func MustWriteTombstone(w io.Writer, deleted bool) int {
	n, err := writeTombstone(w, deleted)
	if err != nil {
		panic(err)
	}
	return n
}

func ReadTombstone(r io.Reader) (bool, error) {
	marker := make([]byte, 1)
	if _, err := r.Read(marker); err != nil {
		return false, err
	}
	return marker[0] == byte(1), nil
}

func SkipTombstone(r io.Reader) error {
	_, err := io.CopyN(io.Discard, r, 1)
	return err
}

// Uint64

func writeUint64(w io.Writer, v uint64) (int, error) {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return w.Write(b)
}

func MustWriteUint64(w io.Writer, n uint64) int {
	written, err := writeUint64(w, n)
	if err != nil {
		panic(err)
	}
	return written
}

func ReadUint64(r io.Reader) (uint64, error) {
	b := make([]byte, 8)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(b), nil
}

func SkipUint64(r io.Reader) error {
	_, err := io.CopyN(io.Discard, r, 8)
	return err
}

// Uint32

func writeUint32(w io.Writer, v uint32) (int, error) {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return w.Write(b)
}

func MustWriteUint32(w io.Writer, v uint32) int {
	n, err := writeUint32(w, v)
	if err != nil {
		panic(err)
	}
	return n
}

func ReadUint32(r io.Reader) (uint32, error) {
	b := make([]byte, 4)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b), nil
}
