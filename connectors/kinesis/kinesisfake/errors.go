package kinesisfake

import (
	"fmt"
	"net/http"
)

func handleError(w http.ResponseWriter, err error) {
	if kerr, ok := err.(KinesisError); ok {
		w.WriteHeader(kerr.StatusCode())
		fmt.Fprintf(w, "{ \"__type\": \"%s\", \"message\": \"%s\" }", kerr.AWSExceptionCode(), kerr.Error())
		return
	}
	w.WriteHeader(http.StatusInternalServerError)
	fmt.Fprintf(w, "{ \"error\": \"Internal Server Error: %v\" }", err)
}

type KinesisError interface {
	error
	StatusCode() int
	AWSExceptionCode() string
}

/* UnsupportedOperationError */

type UnsupportedOperationError struct {
	Operation string
}

func (e *UnsupportedOperationError) Error() string {
	return fmt.Sprintf("Operation '%s' not supported", e.Operation)
}

func (e *UnsupportedOperationError) StatusCode() int {
	return http.StatusNotImplemented
}

/* ResourceNotFoundException */

type ResourceNotFoundException struct {
	message string
}

func (e *ResourceNotFoundException) Error() string {
	if e.message != "" {
		return e.message
	}
	return "Resource not found"
}

func (e *ResourceNotFoundException) StatusCode() int {
	return http.StatusBadRequest
}

func (e *ResourceNotFoundException) AWSExceptionCode() string {
	return "ResourceNotFoundException"
}

/* ExpiredIteratorException */

type ExpiredIteratorException struct {
	message string
}

func (e *ExpiredIteratorException) Error() string {
	if e.message != "" {
		return e.message
	}
	return "Iterator expired"
}

func (e *ExpiredIteratorException) StatusCode() int {
	return http.StatusBadRequest
}

func (e *ExpiredIteratorException) AWSExceptionCode() string {
	return "ExpiredIteratorException"
}
