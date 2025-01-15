package operator

import (
	"slices"

	"reduction.dev/reduction/util/sliceu"
)

type upstreams struct {
	all    []string
	active []string
}

func newUpstreams(ids []string) *upstreams {
	return &upstreams{
		all:    slices.Clone(ids),
		active: slices.Clone(ids),
	}
}

func (u *upstreams) deactivate(id string) {
	u.active = sliceu.Without(u.active, id)
}

func (u *upstreams) hasActive() bool {
	return len(u.active) > 0
}
