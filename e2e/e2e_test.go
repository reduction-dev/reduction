package e2e_test

import (
	"log/slog"
	"os"
	"testing"

	"reduction.dev/reduction/logging"
)

func TestMain(m *testing.M) {
	// Set a logger that handles instanceID prefixes
	slog.SetDefault(slog.New(logging.NewTextHandler()))

	os.Exit(m.Run())
}
