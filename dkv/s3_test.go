package dkv_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/dkv"
	"reduction.dev/reduction/dkv/dkvtest"
	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/storage/objstore"
)

func TestS3_PutAndGet(t *testing.T) {
	s3Service := objstore.NewMemoryS3Service()
	db := dkv.Open(dkv.DBOptions{FileSystem: storage.NewS3FileSystem(s3Service, "bucket")}, nil)

	// Putting a key and successfully getting it.
	db.Put([]byte("k1"), []byte("v1"))
	v, err := db.Get([]byte("k1"))
	require.NoError(t, err)
	dkvtest.EntryEqual(t, dkvtest.NewKVEntry("k1", "v1"), v)

	// Trying to get a key that isn't there.
	_, err = db.Get([]byte("k2"))
	assert.ErrorIs(t, err, kv.ErrNotFound)

	_, err = db.Checkpoint(1)()
	require.NoError(t, err)
	require.NoError(t, db.WaitOnTasks())
}
