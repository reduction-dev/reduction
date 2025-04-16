package confvars

import (
	"strings"

	"reduction.dev/reduction-protocol/jobconfigpb"
)

// StringValue creates a StringValue with the given value
func StringValue(value string) *jobconfigpb.StringVar {
	return &jobconfigpb.StringVar{
		Kind: &jobconfigpb.StringVar_Value{
			Value: value,
		},
	}
}

// IntValue creates an Int32Value with the given value
func IntValue(value int) *jobconfigpb.Int32Var {
	return &jobconfigpb.Int32Var{
		Kind: &jobconfigpb.Int32Var_Value{
			Value: int32(value),
		},
	}
}

func StringListValue(values []string) *jobconfigpb.StringVar {
	return StringValue(strings.Join(values, ","))
}
