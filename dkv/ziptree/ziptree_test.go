package ziptree_test

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/dkv/ziptree"
)

func TestPutAndGetValues(t *testing.T) {
	tree := ziptree.New()
	tree.Put(ziptree.NewKVEntry([]byte("a"), []byte{1}))
	tree.Put(ziptree.NewKVEntry([]byte("b"), []byte{2}))
	tree.Put(ziptree.NewKVEntry([]byte("c"), []byte{2}))

	node, ok := tree.Get([]byte("a"))
	assert.True(t, ok)
	assert.Equal(t, node.Value, []byte{1})
}

func TestPutOverwritingNodes(t *testing.T) {
	tree := ziptree.New()
	tree.Put(ziptree.NewKVEntry([]byte("a"), []byte{1}))
	tree.Put(ziptree.NewKVEntry([]byte("b"), []byte{2}))
	tree.Put(ziptree.NewKVEntry([]byte("c"), []byte{3}))
	tree.Put(ziptree.NewKVEntry([]byte("d"), []byte{3}))
	tree.Put(ziptree.NewKVEntry([]byte("a"), []byte{2}))
	tree.Put(ziptree.NewKVEntry([]byte("a"), []byte{3}))
	tree.Put(ziptree.NewKVEntry([]byte("d"), []byte{1}))
	tree.Print()

	node, ok := tree.Get([]byte("a"))
	assert.True(t, ok)
	assert.Equal(t, []byte{3}, node.Value)

	node, ok = tree.Get([]byte("b"))
	assert.True(t, ok)
	assert.Equal(t, []byte{2}, node.Value)

	node, ok = tree.Get([]byte("d"))
	assert.True(t, ok)
	assert.Equal(t, []byte{1}, node.Value)
}

func TestPutAndAscentPrefix_NoExactKeyMatch(t *testing.T) {
	tree := ziptree.New()
	tree.Put(ziptree.NewKVEntry([]byte("aa"), []byte{1}))
	tree.Put(ziptree.NewKVEntry([]byte("ba"), []byte{2}))
	tree.Put(ziptree.NewKVEntry([]byte("bb"), []byte{3}))
	tree.Put(ziptree.NewKVEntry([]byte("bc"), []byte{4}))
	tree.Put(ziptree.NewKVEntry([]byte("ca"), []byte{5}))

	var values [][]byte
	for node := range tree.AscendPrefix([]byte("b")) {
		values = append(values, node.Value)
	}
	assert.Equal(t, [][]byte{{2}, {3}, {4}}, values)
}

func TestPutAndAscentPrefix_WithExactMatch(t *testing.T) {
	tree := ziptree.New()
	tree.Put(ziptree.NewKVEntry([]byte("a"), []byte{1}))
	tree.Put(ziptree.NewKVEntry([]byte("b"), []byte{2}))
	tree.Put(ziptree.NewKVEntry([]byte("ba"), []byte{3}))
	tree.Put(ziptree.NewKVEntry([]byte("bb"), []byte{4}))
	tree.Put(ziptree.NewKVEntry([]byte("c"), []byte{5}))

	var values [][]byte
	for node := range tree.AscendPrefix([]byte("b")) {
		values = append(values, node.Value)
	}
	assert.Equal(t, [][]byte{{2}, {3}, {4}}, values)
}

// Standard benchmark to compare to other skip lists.
func BenchmarkReadWrite(b *testing.B) {
	value := []byte("value")
	for i := 0; i <= 10; i++ {
		readFrac := float32(i) / 10.0
		b.Run(fmt.Sprintf("frac_%d", i*10), func(b *testing.B) {
			b.ResetTimer()
			var count int
			b.RunParallel(func(pb *testing.PB) {
				rng := rand.New(rand.NewSource(time.Now().UnixNano()))
				zt := ziptree.New()

				for pb.Next() {
					if rng.Float32() < readFrac {
						for _ = range zt.AscendPrefix(randomKey(rng)) {
							count++
						}
					} else {
						zt.Put(ziptree.NewKVEntry(randomKey(rng), value))
					}
				}
			})
		})
	}
}

func randomKey(rng *rand.Rand) []byte {
	b := make([]byte, 8)
	key := rng.Uint32()
	key2 := rng.Uint32()
	binary.LittleEndian.PutUint32(b, key)
	binary.LittleEndian.PutUint32(b[4:], key2)
	return b
}
