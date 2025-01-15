package murmur_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/util/murmur"
)

func TestMurmurHash(t *testing.T) {
	cases := []struct {
		key      string
		expected int
		actual   int
	}{
		{key: "abcdefg", expected: 2285673222},
		{key: "", expected: 0},
		{key: "123456", expected: 3210799800},
		{key: "a1", expected: 882153338},
	}

	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			assert.Equal(t, c.expected, int(murmur.Hash([]byte(c.key), 0)))
		})
	}
}
