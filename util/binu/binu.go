package binu

import (
	"encoding/binary"
	"time"

	"reduction.dev/reduction/util/sliceu"
)

// IntBytesList create a list of binary encoded uint64 values.
func IntBytesList(nums ...uint64) [][]byte {
	return sliceu.Map(nums, func(n uint64) []byte {
		return IntBytes(n)
	})
}

// IntBytes creates a []byte representation of a uint64 value.
func IntBytes(num uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, num)
	return buf
}

func PutTimeBytes(b []byte, t time.Time) int {
	binary.BigEndian.PutUint64(b, uint64(t.UnixNano()))
	return 8
}

func TimeFromBytes(b []byte) time.Time {
	nanoSeconds := binary.BigEndian.Uint64(b)
	return time.Unix(0, int64(nanoSeconds))
}

func MustAppend(buf []byte, order binary.ByteOrder, data any) []byte {
	ret, err := binary.Append(buf, order, data)
	if err != nil {
		panic(err)
	}
	return ret
}
