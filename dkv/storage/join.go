package storage

import (
	"path"
	"strings"
)

// Join URIs together with "/". Go's joining utilities remove repeated slashes
// and don't work with schemes like "file://" or "memory://". The first argument
// of Join can include a scheme.
func Join(base string, paths ...string) string {
	if len(paths) == 0 {
		return base
	}

	// Extract scheme if present
	var scheme string
	if idx := strings.Index(base, "://"); idx != -1 {
		scheme, base = base[:idx+3], base[idx+3:]
	}

	// Join remaining paths
	result := path.Join(base, path.Join(paths...))
	if scheme == "" {
		return result
	}
	return scheme + result
}
