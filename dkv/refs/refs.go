// Reference counting utility.
//
// Concepts:
//   - When a function creates a struct, it returns it with a ref count of 1 and
//     the caller implicitly has ownership of the struct.
//   - The caller only needs to increment the reference if sharing the struct
//     with another owner (e.g. the second level to use a table).
//   - When a function borrows a struct it doesn't modify the reference count
//     and the caller retains ownership.
package refs

import (
	"log"
	"runtime/debug"
	"sync/atomic"
)

type RefCount struct {
	count           atomic.Int64
	wasDereferenced atomic.Bool
	debugLabel      string
	debugID         int
}

func NewRefCount() *RefCount {
	rc := &RefCount{}
	rc.Hold()
	return rc
}

var debugID = 0

func NewDebuggingRefCount(label string) *RefCount {
	rc := &RefCount{
		debugLabel: label,
		debugID:    debugID,
	}
	debugID++
	rc.Hold()
	return rc
}

func (rc *RefCount) Hold() (retained bool) {
	count := rc.count.Add(1)
	if rc.debugLabel != "" {
		log.Printf("Hold %s - %d, count: %d", rc.debugLabel, rc.debugID, count)
		debug.PrintStack()
	}
	if rc.wasDereferenced.Load() {
		panic("adding ref count to item that wasDereferenced")
	}
	if count == 1 {
		return true
	}
	return false
}

func (rc *RefCount) Drop() (released bool) {
	result := rc.count.Add(-1)
	if rc.debugLabel != "" {
		log.Printf("Drop %s - %d, count: %d", rc.debugLabel, rc.debugID, result)
		debug.PrintStack()
	}
	if result < 0 {
		panic("ref count is below 0")
	}
	if result == 0 {
		rc.wasDereferenced.Swap(true)
		return true
	}
	return false
}

type DropRefer interface {
	DropRef() error
}

// Return a callback that binds the value of a struct. This avoids defer
// surprises where the binding changes after an inline function is declared.
func DropFunc(dr DropRefer) func() error {
	return func() error {
		return dr.DropRef()
	}
}
