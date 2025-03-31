// filepath: /Users/mitch/projects/reduction/reduction/connectors/kinesis/config_test.go
package kinesis_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"reduction.dev/reduction/connectors/kinesis"
)

func TestSourceConfigValidate(t *testing.T) {
	validConfig := kinesis.SourceConfig{
		StreamARN: "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
		Endpoint:  "https://kinesis.us-east-1.amazonaws.com",
	}
	assert.NoError(t, validConfig.Validate(), "valid config")

	configMissingStreamARN := kinesis.SourceConfig{
		StreamARN: "",
		Endpoint:  "https://kinesis.us-east-1.amazonaws.com",
	}
	assert.ErrorContains(t, configMissingStreamARN.Validate(), "kinesis Source stream ARN is required")

	configWithInvalidEndpoint := kinesis.SourceConfig{
		StreamARN: "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
		Endpoint:  "://invalid-url",
	}
	assert.ErrorContains(t, configWithInvalidEndpoint.Validate(), "invalid kinesis Source endpoint")

	configWithInvalidARN := kinesis.SourceConfig{
		StreamARN: "invalid-arn",
		Endpoint:  "https://kinesis.us-east-1.amazonaws.com",
	}
	assert.ErrorContains(t, configWithInvalidARN.Validate(), "invalid StreamARN for kinesis source")
}
