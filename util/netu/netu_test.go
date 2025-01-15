package netu_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reduction.dev/reduction/util/netu"
)

func TestResolveAddr(t *testing.T) {
	cases := [][]string{
		{":80", "http://0.0.0.0"},
		{":443", "https://0.0.0.0"},
		{":12345", "http://0.0.0.0:12345"},
		{"http://0.0.0.0", "http://0.0.0.0"},
		{"http://0.0.0.0:12345", "http://0.0.0.0:12345"},
		{"localhost:123", "http://127.0.0.1:123"},
	}

	for _, c := range cases {
		t.Run(c[0], func(t *testing.T) {
			url, err := netu.ResolveAddr(c[0])
			require.NoError(t, err)
			assert.Equal(t, c[1], url)
		})
	}
}
