package clocks

import "time"

type Timer interface {
	Set(d time.Duration, do func())
}

type FakeTimer struct {
	do func()
}

func (t *FakeTimer) Set(d time.Duration, do func()) {
	t.do = do
}

func (t *FakeTimer) Trigger() {
	t.do()
}

type SystemTimer struct {
	timer *time.Timer
}

func (t *SystemTimer) Set(d time.Duration, do func()) {
	if t.timer != nil {
		t.timer.Stop()
	}
	t.timer = time.AfterFunc(d, do)
}
