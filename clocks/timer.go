package clocks

import "time"

type Timer interface {
	Set(d time.Duration, do func())
	Stop()
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

func (t *FakeTimer) Stop() {
	t.do = nil
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

func (t *SystemTimer) Stop() {
	if t.timer != nil {
		t.timer.Stop()
	}
}
