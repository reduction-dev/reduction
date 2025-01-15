package ziptree

import (
	"bytes"
	"fmt"
	"iter"
	"math/rand/v2"
)

type ZipTree struct {
	root *Node
}

func New() *ZipTree {
	return &ZipTree{}
}

// Insert is the original zip tree insert algorithm from https://arxiv.org/pdf/1806.06726.
func (t *ZipTree) insert(node *Node) error {
	node.rank = rand.Uint32()
	key := node.Key
	var prev *Node
	cur := t.root

	// Find the node to be replaced
	for cur != nil &&
		(node.rank < cur.rank || (node.rank == cur.rank && bytes.Compare(key, cur.Key) == 1)) {
		prev = cur
		if bytes.Compare(key, cur.Key) == -1 {
			cur = cur.left
		} else {
			cur = cur.right
		}
	}

	// Insert the new node as a child in its ancestor or set it to root.
	if cur == t.root {
		t.root = node
	} else if bytes.Compare(key, prev.Key) == -1 {
		prev.left = node
	} else {
		prev.right = node
	}

	if cur == nil {
		node.left = nil
		node.right = nil
		return nil
	}

	// Decide where to place the node being replaced relative to the new node.
	if bytes.Compare(key, cur.Key) == -1 {
		node.right = cur
	} else {
		node.left = cur
	}

	// Follow the remaining search path for node, "unzipping".
	prev = node
	for cur != nil {
		fix := prev
		if bytes.Compare(cur.Key, key) == -1 {
			for cur != nil && bytes.Compare(cur.Key, key) <= 0 {
				prev = cur
				cur = cur.right
			}
		} else {
			for cur != nil && bytes.Compare(cur.Key, key) >= 0 {
				prev = cur
				cur = cur.left
			}
		}

		if bytes.Compare(fix.Key, key) == 1 || (fix == node && bytes.Compare(prev.Key, key) == 1) {
			fix.left = cur
		} else {
			fix.right = cur
		}
	}

	return nil
}

// Put either replaces or inserts a new value into the tree. Returns the
// replaced value or nil if the operation was an insert.
func (t *ZipTree) Put(node *Node) (replaced *Node) {
	key := node.Key
	var prev *Node
	cur := t.root

	for cur != nil {
		// If we found the key, stop
		keyCmp := bytes.Compare(key, cur.Key)
		if keyCmp == 0 {
			break
		}

		// Otherwise choose a branch to go down
		prev = cur
		if keyCmp == -1 {
			cur = cur.left
		} else {
			cur = cur.right
		}
	}

	// If we can't find the node, insert it
	if cur == nil {
		t.insert(node)
		return nil
	}

	// Replace the node in the tree
	node.left = cur.left
	node.right = cur.right
	node.rank = cur.rank
	if cur == t.root {
		t.root = node
	} else {
		if prev.right == cur {
			prev.right = node
		} else {
			prev.left = node
		}
	}
	return cur
}

func (t *ZipTree) Get(key []byte) (*Node, bool) {
	cur := t.root
	for cur != nil {
		if bytes.Compare(key, cur.Key) == 1 {
			cur = cur.right
		} else if bytes.Compare(key, cur.Key) == -1 {
			cur = cur.left
		} else {
			return cur, true
		}
	}
	return nil, false
}

func (t *ZipTree) AscendPrefix(prefix []byte) iter.Seq[*Node] {
	return func(yield func(*Node) bool) {
		var stack []*Node

		cur := t.root

		// Traverse the tree to find the node with the given value
		for cur != nil {
			if bytes.Equal(cur.Key, prefix) {
				// Add the exact match to the stack to look for greater items later and
				// stop searching.
				stack = append(stack, cur)
				break
			} else if bytes.Compare(prefix, cur.Key) == -1 {
				// Retain nodes that are greater than the prefix
				// to evaluate later.
				stack = append(stack, cur)
				cur = cur.left
			} else {
				cur = cur.right
			}
		}

		// Add all nodes greater than the prefix to the result
		for len(stack) > 0 {
			// Pop the last element from the stack
			cur = stack[len(stack)-1]
			stack = stack[:len(stack)-1]

			if bytes.HasPrefix(cur.Key, prefix) {
				// Yield the current node as a result
				if !yield(cur) {
					return
				}
			} else {
				// If we found a node without the prefix, stop.
				return
			}

			// Traverse the right subtree for more greater nodes
			cur = cur.right
			for cur != nil {
				stack = append(stack, cur)
				cur = cur.left
			}
		}
	}
}

func (t *ZipTree) Print() {
	print(t.root, "", false)
}

// Print the tree in a human-readable way
func print(node *Node, prefix string, isLeft bool) {
	if node != nil {
		// Print the current node
		fmt.Printf("%s", prefix)
		if isLeft {
			fmt.Printf("├── ")
		} else {
			fmt.Printf("└── ")
		}
		fmt.Printf("Key: %s, Value: %v, Rank: %d\n", node.Key, node.Value, node.rank)

		// Print the left and right children
		newPrefix := prefix
		if isLeft {
			newPrefix += "│   "
		} else {
			newPrefix += "    "
		}

		print(node.left, newPrefix, true)
		print(node.right, newPrefix, false)
	}
}
