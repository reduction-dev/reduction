package jsontemplate

import "os"

// Params hold eagerly loaded parameters for JSON templates and falls back to
// environment variables.
type Params struct {
	params map[string]string
}

func NewParams() *Params {
	return &Params{
		params: make(map[string]string),
	}
}

func (pl *Params) Set(key, value string) {
	pl.params[key] = value
}

// Get retrieves key's value from the params map, falling back to an
// environment variable.
func (pl *Params) Get(key string) (string, bool) {
	value, exists := pl.params[key]
	if exists {
		return value, true
	}

	value = os.Getenv("REDUCTION_PARAM_" + key)
	if value != "" {
		return value, true
	}

	return "", false
}
