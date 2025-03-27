package jsontemplate

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Resolve replaces all `{ "$param": "param_name" }` references in the JSON with
// values from the params map and returns new JSON data. Param values are always
// provided as strings and then converted to specific JSON types according to
// the protobuf definition.
func Resolve(data []byte, protoMsg proto.Message, params map[string]string) ([]byte, error) {
	var jsonObj any
	if err := json.Unmarshal(data, &jsonObj); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	desc := protoMsg.ProtoReflect().Descriptor()
	processedObj, err := processNode(jsonObj, params, desc, "")
	if err != nil {
		return nil, fmt.Errorf("parameter resolution failed: %w", err)
	}

	return json.Marshal(processedObj)
}

// processNode traverses the JSON structure, replacing parameter references
func processNode(node any, params map[string]string, desc protoreflect.MessageDescriptor, path string) (any, error) {
	switch nodeValue := node.(type) {

	// JSON object
	case map[string]any:
		// Check if this is a $param reference node
		if paramName, isParam := nodeValue["$param"]; isParam && len(nodeValue) == 1 {
			// Make sure the name value is a string
			paramNameStr, isNameString := paramName.(string)
			if !isNameString {
				return nil, fmt.Errorf("param name must be a string")
			}

			// Lookup the param value
			paramValue, exists := params[paramNameStr]
			if !exists {
				return nil, fmt.Errorf("missing parameter %q", paramNameStr)
			}

			// Find the field type based on the current path
			fieldDesc, err := getProtoFieldType(desc, path)
			if err != nil {
				return nil, err
			}

			// No functionality to substitute params for repeated fields
			if fieldDesc.Cardinality() == protoreflect.Repeated {
				return nil, fmt.Errorf("cannot use $param for repeated (array) field %q", path)
			}

			// Convert the string value to the appropriate type
			return toJSONType(paramValue, fieldDesc.Kind())
		}

		// Regular object (no $param), process each field
		result := make(map[string]any)
		for k, v := range nodeValue {
			// Update the path for nested traversal
			childPath := path
			if path == "" {
				childPath = k
			} else {
				childPath = path + "." + k
			}

			// Process the child node
			processed, err := processNode(v, params, desc, childPath)
			if err != nil {
				return nil, err
			}
			result[k] = processed
		}
		return result, nil

	// JSON array, process each item
	case []any:
		result := make([]any, len(nodeValue))
		for i, item := range nodeValue {
			processed, err := processNode(item, params, desc, path+"["+strconv.Itoa(i)+"]")
			if err != nil {
				return nil, err
			}
			result[i] = processed
		}
		return result, nil

	// Primitive value, done
	default:
		return nodeValue, nil
	}
}

// getProtoFieldType determines the protobuf field type at a given JSON path
func getProtoFieldType(desc protoreflect.MessageDescriptor, path string) (protoreflect.FieldDescriptor, error) {
	if path == "" {
		return nil, fmt.Errorf("BUG: empty path for type resolution")
	}

	parts := strings.Split(path, ".")
	currentDesc := desc

	for i, fieldName := range parts {
		// Handle array notation: extract the field name without index
		if idx := strings.Index(fieldName, "["); idx >= 0 {
			fieldName = fieldName[:idx]
		}

		field := currentDesc.Fields().ByJSONName(fieldName)
		if field == nil {
			return nil, fmt.Errorf("field %s not found in path %s", fieldName, path)
		}

		if i == len(parts)-1 {
			// Last part of the path, return its type and cardinality
			return field, nil
		} else if field.Kind() == protoreflect.MessageKind {
			// Move to nested message for further traversal
			currentDesc = field.Message()
		} else {
			return nil, fmt.Errorf("cannot traverse non-message field %s in path %s", fieldName, path)
		}
	}

	return nil, fmt.Errorf("failed to resolve type for path %s", path)
}

// toJSONType converts a string value to a specific proto type
func toJSONType(value string, fieldType protoreflect.Kind) (any, error) {
	switch fieldType {
	case protoreflect.Int32Kind, protoreflect.Int64Kind,
		protoreflect.Uint32Kind, protoreflect.Uint64Kind,
		protoreflect.Sint32Kind, protoreflect.Sint64Kind,
		protoreflect.Fixed32Kind, protoreflect.Fixed64Kind,
		protoreflect.Sfixed32Kind, protoreflect.Sfixed64Kind:
		return strconv.ParseInt(value, 10, 64)
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return strconv.ParseFloat(value, 64)
	case protoreflect.BoolKind:
		return strconv.ParseBool(value)
	case protoreflect.StringKind:
		return value, nil
	default:
		return nil, fmt.Errorf("unsupported field type: %v", fieldType)
	}
}
