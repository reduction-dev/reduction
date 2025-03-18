package operator

import (
	"context"
	"errors"

	"reduction.dev/reduction/dkv/kv"
	"reduction.dev/reduction/partitioning"
	"reduction.dev/reduction/proto"
)

type OperatorPartition struct {
	keyGroupRange partitioning.KeyGroupRange
	neighbors     []neighborPartition
}

func newOperatorPartition(keyGroupRange partitioning.KeyGroupRange, neighbors []neighborPartition) *OperatorPartition {
	return &OperatorPartition{
		keyGroupRange: keyGroupRange,
		neighbors:     neighbors,
	}
}

func (o *OperatorPartition) OwnsKey(key []byte) bool {
	keyGroup := partitioning.KeyGroupFromBytes(key[:2])
	return o.keyGroupRange.IncludesKeyGroup(keyGroup)
}

func (o *OperatorPartition) ExclusivelyOwnsTable(uri string, startKey []byte, endKey []byte) (bool, error) {
	// If this partition owns the entire key range then there's no need to check
	// other operators.
	otherRange := partitioning.KeyGroupRangeFromBytes(startKey[:2], endKey[:2])
	if o.keyGroupRange.Contains(otherRange) {
		return true, nil
	}

	// Setup context to race the NeedsTable calls.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Collect the results in a channel
	type result struct {
		needsTable bool
		err        error
	}
	results := make(chan result, len(o.neighbors))
	for _, neighbor := range o.neighbors {
		go func() {
			needsTable, err := neighbor.NeedsTable(ctx, uri, startKey, endKey)
			results <- result{needsTable, err}
			if needsTable {
				cancel() // Can cancel the other NeedsTable calls
			}
		}()
	}

	// Check the results
	var err error
	neighborNeedsTable := false
	for range o.neighbors {
		result := <-results
		if result.needsTable {
			neighborNeedsTable = true
			break
		}
		if result.err != nil {
			err = errors.Join(err, result.err)
			continue
		}
	}

	return !neighborNeedsTable, err
}

var _ kv.DataOwnership = &OperatorPartition{}

type neighborPartition struct {
	keyGroupRange partitioning.KeyGroupRange
	operator      proto.Operator
}

func (o *neighborPartition) NeedsTable(ctx context.Context, filePath string, startKey []byte, endKey []byte) (bool, error) {
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
