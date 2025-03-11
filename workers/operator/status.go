package operator

import (
	"fmt"
	"sync/atomic"
)

// status represents the current state of an Operator's lifecycle
type status uint32

const (
	// StatusInit indicates the operator has started but not registered yet
	StatusInit status = iota

	// StatusRegistered indicates the operator has successfully registered with the job
	StatusRegistered

	// StatusLoading indicates the operator is loading data from storage
	StatusLoading

	// StatusReady indicates the operator is ready to process events
	StatusReady
)

// String returns a human-readable representation of the Status
func (s status) String() string {
	switch s {
	case StatusInit:
		return "Init"
	case StatusRegistered:
		return "Registered"
	case StatusLoading:
		return "Loading"
	case StatusReady:
		return "Ready"
	default:
		return fmt.Sprintf("Unknown(%d)", s)
	}
}

// operatorStatus manages atomic status transitions for an Operator
type operatorStatus struct {
	status atomic.Uint32
}

func newOperatorStatus() *operatorStatus {
	s := &operatorStatus{}
	s.status.Store(uint32(StatusInit))
	return s
}

// DidRegister attempts to transition from Init to DidRegister
func (s *operatorStatus) DidRegister() {
	s.status.CompareAndSwap(uint32(StatusInit), uint32(StatusRegistered))
}

// LoadingStarted attempts to transition to Loading status
func (s *operatorStatus) LoadingStarted() {
	s.status.Store(uint32(StatusLoading))
}

// DidLoad attempts to transition from Loading to Ready
// Returns an error if not in Loading state
func (s *operatorStatus) DidLoad() error {
	if !s.status.CompareAndSwap(uint32(StatusLoading), uint32(StatusReady)) {
		return fmt.Errorf("cannot complete loading: current status is %s, expected %s",
			status(s.status.Load()), StatusLoading)
	}
	return nil
}

// IsReady returns true if the operator is in the Ready state
func (s *operatorStatus) IsReady() bool {
	return status(s.status.Load()) == StatusReady
}

// String returns the string representation of the current status
func (s *operatorStatus) String() string {
	return status(s.status.Load()).String()
}
