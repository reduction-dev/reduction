package partitioning

type Partition interface {
	OwnsKey(key []byte) bool
}

type AllKeysPartition struct {
}

func (a *AllKeysPartition) OwnsKey(key []byte) bool {
	return true
}
