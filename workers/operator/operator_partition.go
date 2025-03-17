package operator

import (
	"context"

	"reduction.dev/reduction/partitioning"
	"reduction.dev/reduction/proto"
)

type OperatorPartition struct {
	keyGroupRange partitioning.KeyGroupRange
	neighbors     []neighborParition
}

func newOperatorPartition(keyGroupRange partitioning.KeyGroupRange, neighbors []neighborParition) *OperatorPartition {
	return &OperatorPartition{
		keyGroupRange: keyGroupRange,
		neighbors:     neighbors,
	}
}

func (o *OperatorPartition) OwnsKey(key []byte) bool {
	keyGroup := partitioning.KeyGroupFromBytes(key[:2])
	return o.keyGroupRange.IncludesKeyGroup(keyGroup)
}

var _ partitioning.Partition = &OperatorPartition{}

type neighborParition struct {
	keyGroupRange partitioning.KeyGroupRange
	operator      proto.Operator
}

func (o *neighborParition) NeedsTable(ctx context.Context, filePath string, startKey []byte, endKey []byte) (bool, error) {
	// Derive the key group range from the start and end keys of a table.
	tableKeyGroupRange := partitioning.KeyGroupRange{
		Start: int(partitioning.KeyGroupFromBytes(startKey[:2])),
		End:   int(partitioning.KeyGroupFromBytes(endKey[:2])) + 1,
	}

	// If the neighboring operator's key group range doesn't intersect with the
	// table's key range then we don't need to ask if it needs the table.
	if !o.keyGroupRange.Overlaps(tableKeyGroupRange) {
		return false, nil
	}

	return o.operator.NeedsTable(ctx, filePath)
}
