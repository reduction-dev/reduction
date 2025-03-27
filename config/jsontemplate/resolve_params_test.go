package jsontemplate_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"reduction.dev/reduction/config/jsontemplate"
)

func TestResolveIntegerParameter(t *testing.T) {
	params := jsontemplate.NewParams()
	params.Set("INTEGER_PARAM", "5")

	result, err := jsontemplate.Resolve(
		[]byte(`{ "integerField": { "$param": "INTEGER_PARAM" } }`),
		createTestMessage(),
		params,
	)
	require.NoError(t, err)

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(result, &parsed))

	// JSON numbers are parsed as float64 by the json package
	assert.Equal(t, float64(5), parsed["integerField"], "integer parameter should be converted to float")
}

func TestResolveBooleanParameter(t *testing.T) {
	params := jsontemplate.NewParams()
	params.Set("BOOLEAN_PARAM", "true")
	result, err := jsontemplate.Resolve(
		[]byte(`{"booleanField": { "$param": "BOOLEAN_PARAM" }}`),
		createTestMessage(),
		params,
	)
	require.NoError(t, err)

	var parsed map[string]interface{}
	require.NoError(t, json.Unmarshal(result, &parsed))
	assert.Equal(t, true, parsed["booleanField"], "boolean parameter should be converted to boolean")
}

func TestResolveStringParameter(t *testing.T) {
	params := jsontemplate.NewParams()
	params.Set("STRING_PARAM", "string-value")
	result, err := jsontemplate.Resolve(
		[]byte(`{"stringField": { "$param": "STRING_PARAM" }}`),
		createTestMessage(),
		params,
	)
	require.NoError(t, err)

	var parsed map[string]interface{}
	require.NoError(t, json.Unmarshal(result, &parsed))
	assert.Equal(t, "string-value", parsed["stringField"], "string parameter should remain a string")
}

func TestResolveNestedParameters(t *testing.T) {
	params := jsontemplate.NewParams()
	params.Set("NESTED_INTEGER_PARAM", "3")
	params.Set("NESTED_BOOLEAN_PARAM", "true")
	result, err := jsontemplate.Resolve(
		[]byte(`{
			"objectField": {
				"nestedIntegerField": { "$param": "NESTED_INTEGER_PARAM" },
				"nestedBooleanField": { "$param": "NESTED_BOOLEAN_PARAM" }
			}
		}`),
		createTestMessage(),
		params,
	)
	require.NoError(t, err)

	var parsed map[string]any
	require.NoError(t, json.Unmarshal(result, &parsed))
	nestedObject := parsed["objectField"].(map[string]any)
	assert.Equal(t, float64(3), nestedObject["nestedIntegerField"], "nested integer parameter should be converted to number")
	assert.Equal(t, true, nestedObject["nestedBooleanField"], "nested boolean parameter should be converted to boolean")
}

func TestResolveArrayParameter(t *testing.T) {
	params := jsontemplate.NewParams()
	params.Set("STRING_ARRAY_PARAM", "value1,value2,value3")
	_, err := jsontemplate.Resolve(
		[]byte(`{"stringArray": { "$param": "STRING_ARRAY_PARAM" }}`),
		createTestMessage(),
		params,
	)
	require.ErrorContains(t, err, "cannot use $param for repeated (array) field")
}

func TestResolveMissingParameter(t *testing.T) {
	_, err := jsontemplate.Resolve(
		[]byte(`{"integerField": { "$param": "MISSING_PARAM" }}`),
		createTestMessage(),
		jsontemplate.NewParams(),
	)
	assert.Error(t, err, "should return an error when parameter is missing")
	assert.Contains(t, err.Error(), "MISSING_PARAM", "error message should mention the missing parameter")
}

func TestResolveInvalidJSON(t *testing.T) {
	_, err := jsontemplate.Resolve(
		[]byte(`{ invalid json }`),
		createTestMessage(),
		jsontemplate.NewParams(),
	)
	assert.Error(t, err, "should return an error for invalid JSON")
}

func TestResolveInvalidParamType(t *testing.T) {
	_, err := jsontemplate.Resolve(
		[]byte(`{"integerField": { "$param": 123 }}`),
		createTestMessage(),
		jsontemplate.NewParams(),
	)
	assert.Error(t, err, "should return an error when param name is not a string")
}

// createTestMessage creates a proto message with multiple field types for testing parameter resolution
func createTestMessage() proto.Message {
	// Create FileDescriptorSet will all the field types we want to test
	fileDesc := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test_message.proto"),
		Syntax:  proto.String("proto3"),
		Package: proto.String("test"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("ParamsTestMessage"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:     proto.String("integer_field"),
						Number:   proto.Int32(1),
						Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
						JsonName: proto.String("integerField"),
					},
					{
						Name:     proto.String("boolean_field"),
						Number:   proto.Int32(2),
						Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum(),
						JsonName: proto.String("booleanField"),
					},
					{
						Name:     proto.String("string_field"),
						Number:   proto.Int32(3),
						Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
						JsonName: proto.String("stringField"),
					},
					{
						Name:     proto.String("string_array"),
						Number:   proto.Int32(4),
						Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
						JsonName: proto.String("stringArray"),
					},
					{
						Name:     proto.String("object_field"),
						Number:   proto.Int32(5),
						Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
						TypeName: proto.String(".test.NestedObject"),
						JsonName: proto.String("objectField"),
					},
				},
			},
			{
				Name: proto.String("NestedObject"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:     proto.String("nested_integer_field"),
						Number:   proto.Int32(1),
						Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
						JsonName: proto.String("nestedIntegerField"),
					},
					{
						Name:     proto.String("nested_boolean_field"),
						Number:   proto.Int32(2),
						Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum(),
						JsonName: proto.String("nestedBooleanField"),
					},
				},
			},
		},
	}

	// Convert descriptor proto to a FileDescriptor
	files := new(protoregistry.Files)
	descs, err := protodesc.NewFiles(&descriptorpb.FileDescriptorSet{
		File: []*descriptorpb.FileDescriptorProto{fileDesc},
	})
	if err != nil {
		panic("failed to create test message descriptors: " + err.Error())
	}
	descs.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		files.RegisterFile(fd)
		return true
	})

	// Find the message descriptor
	desc, err := descs.FindDescriptorByName(protoreflect.FullName("test.ParamsTestMessage"))
	if err != nil {
		panic("failed to find test message descriptor: " + err.Error())
	}

	return dynamicpb.NewMessage(desc.(protoreflect.MessageDescriptor))
}
