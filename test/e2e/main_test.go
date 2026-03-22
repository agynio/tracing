//go:build e2e

package e2e_test

import (
	"os"
	"strings"
	"testing"
)

var tracingAddress = envOrDefault("TRACING_ADDRESS", "tracing:50051")

func envOrDefault(key, fallback string) string {
	value, ok := os.LookupEnv(key)
	if !ok {
		return fallback
	}
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
