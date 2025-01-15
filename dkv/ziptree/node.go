package ziptree

type Node struct {
	Key   []byte
	Value []byte
	Meta  any
	rank  uint32
	left  *Node
	right *Node
}

func NewKVEntry(key []byte, value []byte) *Node {
	return &Node{
		Key:   key,
		Value: value,
	}
}

func NewNode(key, value []byte, meta any) *Node {
	return &Node{
		Key:   key,
		Value: value,
		Meta:  meta,
	}
}
