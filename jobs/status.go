package jobs

import (
	"fmt"
	"sync/atomic"
)

// status represents the current state of a Job's lifecycle
type status uint32

const (
	// StatusInit indicates the job has been initialized but not yet attempted to start
	StatusInit status = iota

	// StatusPaused indicates the job is not running and waiting for resources
	StatusPaused

	// StatusAssemblyStarting indicates the job is in the process of starting
	StatusAssemblyStarting

	// StatusRunning indicates the job is fully operational
	StatusRunning
)

// String returns a human-readable representation of the Status
func (s status) String() string {
	switch s {
	case StatusInit:
		return "Init"
	case StatusPaused:
		return "Paused"
	case StatusAssemblyStarting:
		return "Starting"
	case StatusRunning:
		return "Running"
	default:
		return fmt.Sprintf("Unknown(%d)", s)
	}
}

// jobStatus manages atomic status transitions for a Job
type jobStatus struct {
	status atomic.Uint32
}

func newJobStatus() *jobStatus {
	s := &jobStatus{}
	s.status.Store(uint32(StatusInit))
	return s
}

func (s *jobStatus) Value() status {
	return status(s.status.Load())
}

func (s *jobStatus) Set(value status) {
	s.status.Store(uint32(value))
}

// String returns the string representation of the current status
func (s *jobStatus) String() string {
	return status(s.status.Load()).String()
}
