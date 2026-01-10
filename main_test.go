package ssproc

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// Set test environment flag for reduced memory usage in worker pool
	os.Setenv("GO_TEST_ENV", "TRUE")
	os.Exit(m.Run())
}
