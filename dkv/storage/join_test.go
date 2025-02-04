package storage_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/dkv/storage"
)

func TestJoin(t *testing.T) {
	assert.Equal(t, "file:///a/b/c", storage.Join("file://", "/a", "b", "c"), "schema with absolute path arg")
	assert.Equal(t, "file://a/b/c", storage.Join("file://", "a", "b", "c"), "schema with relative path arg")
	assert.Equal(t, "schema:///a/b/c", storage.Join("schema:///a", "b", "c"), "base with schema and absolute path")
	assert.Equal(t, "schema:///a/b/c", storage.Join("schema:////a", "b", "c"), "base with too many slashes")
	assert.Equal(t, "a/b/c", storage.Join("a", "b", "c"), "simple relative path")
	assert.Equal(t, "a/b/c", storage.Join("a", "/b/", "/c"), "relative path with slashes in args")
	assert.Equal(t, "/a/b/c", storage.Join("/a", "b", "c"), "absolute path")
	assert.Equal(t, "/a/b/c", storage.Join("/a", "b/", "/c/"), "absolute path with slashes in args")
	assert.Equal(t, "/a/b/c", storage.Join("/a", "/b//c"), "absolute path with double slash in arg")

	assert.Equal(t, "base", storage.Join("base"), "degenerate: single arg")
}
