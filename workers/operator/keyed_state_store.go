package operator

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"reduction.dev/reduction-handler/handlerpb"
	"reduction.dev/reduction/dkv"
	"reduction.dev/reduction/partitioning"
)

// KeyedStateStore uses a DKV database to store and retrieve user-provided state
// entries from the handler function. It is an adapter responsible for encoding
// and decoding composite keys for DKV.
type KeyedStateStore struct {
	db       *dkv.DB
	keySpace *partitioning.KeySpace
}

func NewKeyedStateStore(db *dkv.DB, keySpace *partitioning.KeySpace) *KeyedStateStore {
	return &KeyedStateStore{db: db, keySpace: keySpace}
}

// GetState gets all the state entries for a given subject key.
func (s *KeyedStateStore) GetState(key []byte) ([]*handlerpb.StateEntryNamespace, error) {
	var ret []*handlerpb.StateEntryNamespace
	var currentItem *handlerpb.StateEntryNamespace

	var scanErr error
	for entry := range s.db.ScanPrefix(s.encodeSubjectKey(key), &scanErr) {
		ns, keyData := s.decodeKey(entry.Key())
		pbEntry := &handlerpb.StateEntry{Key: keyData, Value: entry.Value()}

		// Start a new state item if we're on the first iteration or have a new key
		// state item id. This takes advantage of the fact that the DKV scan results
		// are ordered by key.
		if currentItem == nil || currentItem.Namespace != string(ns) {
			currentItem = &handlerpb.StateEntryNamespace{Namespace: string(ns)}
			ret = append(ret, currentItem)
		}
		currentItem.Entries = append(currentItem.Entries, pbEntry)
	}

	if scanErr != nil {
		return nil, fmt.Errorf("GetState for key %q: %w", string(key), scanErr)
	}

	return ret, nil
}

func (s *KeyedStateStore) ApplyMutations(subjectKey []byte, mutations []*handlerpb.StateMutationNamespace) error {
	for _, namespace := range mutations {
		for _, mutation := range namespace.Mutations {
			switch mutation.GetMutation().(type) {
			case *handlerpb.StateMutation_Delete:
				mut := mutation.GetDelete()
				s.db.Delete(s.encodeDBKey(subjectKey, namespace.Namespace, mut.Key))
			case *handlerpb.StateMutation_Put:
				mut := mutation.GetPut()
				s.db.Put(s.encodeDBKey(subjectKey, namespace.Namespace, mut.Key), mut.Value)
			default:
				panic(fmt.Sprintf("BUG: invalid state mutation type %+v", mutation.Mutation))
			}
		}
	}

	return nil
}

// Create a composite key for DKV storage like
//
//	<key-group><schema><subject-key><namespace><data>
//
// Schema is 0x00, subject-key and namespace are length encoded and data is the
// data bytes as-is.
func (s *KeyedStateStore) encodeDBKey(subjectKey []byte, namespace string, data []byte) []byte {
	// 2 key group bytes + 1 schema byte + 4 subject key length bytes + subject key + 1 namespace length byte + namespace + data
	buf := make([]byte, 2+1+4+len(subjectKey)+1+len(namespace)+len(data))
	offset := 0

	// Key group prefix
	kg := s.keySpace.KeyGroup(subjectKey)
	kg.PutBytes(buf)
	offset += 2

	// Schema byte for KeyedStateStore
	buf[offset] = 0x00
	offset += 1

	// Subject key length
	binary.BigEndian.PutUint32(buf[offset:], uint32(len(subjectKey)))
	offset += 4

	// Subject key content
	offset += copy(buf[offset:], subjectKey)

	// Namespace length
	buf[offset] = uint8(len(namespace))
	offset += 1

	// Namespace content
	offset += copy(buf[offset:], namespace)

	// Data content
	copy(buf[offset:], data)
	return buf
}

// Length encode the subject key to query DKV
func (s *KeyedStateStore) encodeSubjectKey(subjectKey []byte) []byte {
	// 2 key group bytes + 1 schema byte + 4 subject key length bytes + subjectKey content
	buf := make([]byte, 2+1+4+len(subjectKey))

	// Write key group prefix
	kg := s.keySpace.KeyGroup(subjectKey)
	kg.PutBytes(buf)

	// Write remaining key components
	buf[2] = 0x00                                                 // Schema byte for KeyedStateStore
	binary.BigEndian.PutUint32(buf[3:7], uint32(len(subjectKey))) // Subject key length
	copy(buf[7:], subjectKey)                                     // Subject key content
	return buf
}

// Given a DB key created with encodeDBKey, return the namespace and data portions of the key.
// The subject key doesn't need to be returned because the caller already knows the value.
func (s *KeyedStateStore) decodeKey(compositeKey []byte) (namespace, data []byte) {
	r := bytes.NewReader(compositeKey[3:]) // Skip key group and schema bytes

	// Read subject key length and skip over the value
	var subjectKeyLen uint32
	if err := binary.Read(r, binary.BigEndian, &subjectKeyLen); err != nil {
		panic(err)
	}
	if _, err := r.Seek(int64(subjectKeyLen), io.SeekCurrent); err != nil {
		panic(err)
	}

	// Read the namespace value
	var namespaceLen uint8
	if err := binary.Read(r, binary.BigEndian, &namespaceLen); err != nil {
		panic(err)
	}
	namespace = make([]byte, namespaceLen)
	if err := binary.Read(r, binary.BigEndian, &namespace); err != nil {
		panic(err)
	}

	// Read remaining data
	data, err := io.ReadAll(r)
	if err != nil {
		panic(err)
	}

	return namespace, data
}
