package kv

// DataOwnership defines the contract for determining if a keys and tables
// belongs to a particular instance of DKV.
type DataOwnership interface {
	// OwnsKey determines if this instance owns the specified key
	OwnsKey(key []byte) bool

	// ExclusivelyOwnsTable determines if this instance exclusively owns the
	// given table with the specified start and end keys.
	ExclusivelyOwnsTable(uri string, startKey, endKey []byte) (bool, error)
}

// AllDataOwnership is a useful default for some tests scenarios.
type AllDataOwnership struct{}

func (*AllDataOwnership) OwnsKey(key []byte) bool {
	return true
}

func (*AllDataOwnership) ExclusivelyOwnsTable(uri string, startKey, endKey []byte) (bool, error) {
	return true, nil
}

var _ DataOwnership = &AllDataOwnership{}
