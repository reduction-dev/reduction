package kinesisfake

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/smithy-go"
)

var errCodeToStatusCode = map[string]int{
	"ResourceNotFoundException":              http.StatusNotFound,
	"ExpiredIteratorException":               http.StatusBadRequest,
	"ProvisionedThroughputExceededException": http.StatusBadRequest,
	"AccessDeniedException":                  http.StatusForbidden,
	"InvalidArgumentException":               http.StatusBadRequest,
}

func handleError(w http.ResponseWriter, err error) {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		statusCode := errCodeToStatusCode[apiErr.ErrorCode()]
		if statusCode == 0 {
			statusCode = http.StatusInternalServerError
		}
		w.WriteHeader(statusCode)
		fmt.Fprintf(w, "{ \"__type\": \"%s\", \"message\": \"%s\" }", apiErr.ErrorCode(), apiErr.ErrorMessage())
	}

	w.WriteHeader(http.StatusInternalServerError)
	fmt.Fprintf(w, "{ \"error\": \"Internal Server Error: %v\" }", err)
}

type KinesisError interface {
	error
	StatusCode() int
	AWSExceptionCode() string
}
