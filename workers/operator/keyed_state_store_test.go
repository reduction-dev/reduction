package operator_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction-protocol/handlerpb"
	"reduction.dev/reduction/dkv"
	"reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/partitioning"
	"reduction.dev/reduction/workers/operator"
)

func TestKeyedStateStore_StoreAndRetrieveStateEntry(t *testing.T) {
	db := dkv.Open(dkv.DBOptions{
		FileSystem: storage.NewMemoryFilesystem(),
	}, nil)
	keySpace := partitioning.NewKeySpace(256, 8)
	store := operator.NewKeyedStateStore(db, keySpace)

	err := store.ApplyMutations([]byte("subject-key"), []*handlerpb.StateMutationNamespace{{
		Namespace: "namespace",
		Mutations: []*handlerpb.StateMutation{{
			Mutation: &handlerpb.StateMutation_Put{
				Put: &handlerpb.PutMutation{
					Key:   []byte("key"),
					Value: []byte("value"),
				},
			},
		}},
	}})
	require.NoError(t, err)

	stateNamespaces, err := store.GetState([]byte("subject-key"))
	require.NoError(t, err)

	assert.Equal(t, stateNamespaces, []*handlerpb.StateEntryNamespace{{
		Namespace: "namespace",
		Entries: []*handlerpb.StateEntry{{
			Key:   []byte("key"),
			Value: []byte("value"),
		}},
	}})
}
