package jsontemplate_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/config/jsontemplate"
)

func TestNewParams(t *testing.T) {
	params := jsontemplate.NewParams()
	assert.NotNil(t, params, "NewParams should return a non-nil pointer")
}

func TestParamsSetGet(t *testing.T) {
	params := jsontemplate.NewParams()

	// Test setting and getting a value
	params.Set("key1", "value1")
	value, exists := params.Get("key1")
	assert.True(t, exists, "Get should return true for existing key")
	assert.Equal(t, "value1", value, "Get should return the correct value")

	// Test getting a non-existent key
	value, exists = params.Get("nonexistent")
	assert.False(t, exists, "Get should return false for non-existent key")
	assert.Equal(t, "", value, "Get should return empty string for non-existent key")

	// Test overwriting a value
	params.Set("key1", "newvalue")
	value, exists = params.Get("key1")
	assert.True(t, exists, "Get should return true for existing key")
	assert.Equal(t, "newvalue", value, "Get should return the updated value")
}

func TestParamsEnvironmentFallback(t *testing.T) {
	params := jsontemplate.NewParams()

	// Set environment variable
	os.Setenv("REDUCTION_PARAM_testkey", "testvalue")
	defer os.Unsetenv("REDUCTION_PARAM_testkey")

	// Test environment fallback
	value, exists := params.Get("testkey")
	assert.True(t, exists, "Get should return true for key with env var")
	assert.Equal(t, "testvalue", value, "Get should return env var value")

	// Test params map precedence over environment variables
	params.Set("testkey", "mapvalue")
	value, exists = params.Get("testkey")
	assert.True(t, exists, "Get should return true for key in map")
	assert.Equal(t, "mapvalue", value, "Value from params map should take precedence over env var")
}
