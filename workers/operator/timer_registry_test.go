package operator_test

import (
	"iter"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"reduction.dev/reduction/dkv"
	"reduction.dev/reduction/dkv/storage"
	"reduction.dev/reduction/partitioning"
	"reduction.dev/reduction/proto/workerpb"
	"reduction.dev/reduction/util/size"
	"reduction.dev/reduction/workers/operator"

	"github.com/stretchr/testify/assert"
)

func TestInvokingCallbacksAfterAdvanceTime(t *testing.T) {
	e := operator.NewTimerRegistry(newTimerStore(), nil)
	e.SetTimer([]byte("key-1"), time.Unix(2, 0))
	callCount := 0
	inc := func(it iter.Seq2[[]byte, time.Time]) {
		for range it {
			callCount++
		}
	}

	inc(e.AdvanceWatermark("task", &workerpb.Watermark{Timestamp: &timestamppb.Timestamp{Seconds: 1}}))
	assert.Equal(t, 0, callCount)

	// Watermark at or later than timer
	inc(e.AdvanceWatermark("task", &workerpb.Watermark{Timestamp: &timestamppb.Timestamp{Seconds: 2}}))
	assert.Equal(t, 1, callCount)
	inc(e.AdvanceWatermark("task", &workerpb.Watermark{Timestamp: &timestamppb.Timestamp{Seconds: 3}}))
	assert.Equal(t, 1, callCount)
}

func TestCoalescingTimers(t *testing.T) {
	e := operator.NewTimerRegistry(newTimerStore(), nil)

	// Timers should be coalesced to one timer
	e.SetTimer([]byte("key-1"), time.Unix(2, 0))
	e.SetTimer([]byte("key-1"), time.Unix(2, 0))

	// Timers should be coalesced to one timer
	e.SetTimer([]byte("key-2"), time.Unix(2, 0))
	e.SetTimer([]byte("key-2"), time.Unix(2, 0))

	calls := make(map[string]int)
	inc := func(it iter.Seq2[[]byte, time.Time]) {
		for k := range it {
			calls[string(k)]++
		}
	}

	// Watermark behind all timers
	inc(e.AdvanceWatermark("task", &workerpb.Watermark{Timestamp: &timestamppb.Timestamp{Seconds: 1}}))
	assert.Equal(t, map[string]int{}, calls)

	inc(e.AdvanceWatermark("task", &workerpb.Watermark{Timestamp: &timestamppb.Timestamp{Seconds: 2}}))
	assert.Equal(t, map[string]int{
		"key-1": 1,
		"key-2": 1,
	}, calls)

	// Watermark past timers
	inc(e.AdvanceWatermark("task", &workerpb.Watermark{Timestamp: &timestamppb.Timestamp{Seconds: 3}}))
	assert.Equal(t, map[string]int{
		"key-1": 1,
		"key-2": 1,
	}, calls)
}

func TestTimeAdvancesAfterAllUpstreamsReachWatermark(t *testing.T) {
	e := operator.NewTimerRegistry(newTimerStore(), []string{"task-1", "task-2"})
	e.SetTimer([]byte("key-1"), time.Unix(2, 0))
	callCount := 0
	inc := func(it iter.Seq2[[]byte, time.Time]) {
		for range it {
			callCount++
		}
	}

	// Advancing one timer is not enough to invoke callback
	inc(e.AdvanceWatermark("task-1", &workerpb.Watermark{Timestamp: &timestamppb.Timestamp{Seconds: 2}}))
	assert.Equal(t, 0, callCount)

	// Second task must advance up to timer value as well
	inc(e.AdvanceWatermark("task-2", &workerpb.Watermark{Timestamp: &timestamppb.Timestamp{Seconds: 1}}))
	assert.Equal(t, 0, callCount)
	inc(e.AdvanceWatermark("task-2", &workerpb.Watermark{Timestamp: &timestamppb.Timestamp{Seconds: 2}}))
	assert.Equal(t, 1, callCount)
}

func TestSettingTimerOnOrBeforeWatermarkIsNoop(t *testing.T) {
	e := operator.NewTimerRegistry(newTimerStore(), nil)
	callCount := 0
	inc := func(it iter.Seq2[[]byte, time.Time]) {
		for range it {
			callCount++
		}
	}

	inc(e.AdvanceWatermark("task", &workerpb.Watermark{Timestamp: &timestamppb.Timestamp{Seconds: 1}}))
	assert.Equal(t, 0, callCount)

	e.SetTimer([]byte("key-1"), time.Unix(1, 0))

	inc(e.AdvanceWatermark("task", &workerpb.Watermark{Timestamp: &timestamppb.Timestamp{Seconds: 2}}))
	assert.Equal(t, 0, callCount)
}

func newTimerStore() *operator.TimerStore {
	db := dkv.Open(dkv.DBOptions{
		FileSystem: storage.NewMemoryFilesystem(),
	}, nil)
	return operator.NewTimerStore(db, partitioning.NewKeySpace(256, 8), partitioning.KeyGroupRange{Start: 0, End: 255}, size.MB)
}
