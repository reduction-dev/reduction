package sst

import (
	"bytes"
	"io"
	"math"
	"slices"

	"reduction.dev/reduction/dkv/fields"
)

// Write a value to the search index every nth entry, starting with the first
// entry.
const searchIndexSpacing = 16

type SearchIndex struct {
	offsets      []uint32
	itemsWritten int
}

// Every n items, write an entry to the index
func (si *SearchIndex) IndexOffset(offset int64) {
	if si.itemsWritten%searchIndexSpacing == 0 {
		si.offsets = append(si.offsets, uint32(offset))
	}
	si.itemsWritten++
}

func (si *SearchIndex) Search(targetKey []byte, readKey func(offset int64) ([]byte, error)) (start, end int64, err error) {
	// If we have no index, do the entire scan.
	if len(si.offsets) == 0 {
		return 0, math.MaxInt64, nil
	}

	// Compare the offsets in the searchIndex given a targetKey.
	var searchErr error
	cmp := func(offset uint32, targetKey []byte) int {
		key, err := readKey(int64(offset))
		if err != nil {
			searchErr = err
			return 0 // Claim "found" to stop the search.
		}

		return bytes.Compare(key, targetKey)
	}
	foundIndex, isExact := slices.BinarySearchFunc(si.offsets, targetKey, cmp)
	if searchErr != nil {
		return 0, 0, searchErr
	}

	// BinarySearch returns the _greater_ index when we don't find an exact match
	// so subtract one here to start searching from the earlier index.
	if !isExact {
		foundIndex--
	}

	// Use a scan from this point to look for the key
	startOffset := int64(si.offsets[foundIndex])

	var endOffset int64
	if foundIndex == len(si.offsets)-1 {
		endOffset = math.MaxInt64
	} else {
		endOffset = int64(si.offsets[foundIndex+1])
	}

	return startOffset, endOffset, nil
}

func (si *SearchIndex) Encode(w io.Writer) int {
	// Write the number of offsets
	n := fields.MustWriteUint32(w, uint32(len(si.offsets)))

	// Write each offset
	for _, o := range si.offsets {
		n += fields.MustWriteUint32(w, o)
	}
	return n
}

func SearchIndexDecode(r io.Reader) *SearchIndex {
	// Read the number of offsets
	offsetCount, err := fields.ReadUint32(r)
	if err != nil {
		panic(err)
	}

	si := &SearchIndex{}

	// Read each offset
	si.offsets = make([]uint32, offsetCount)
	for i := range si.offsets {
		offset, err := fields.ReadUint32(r)
		if err != nil {
			panic(err)
		}
		si.offsets[i] = offset
	}

	return si
}
