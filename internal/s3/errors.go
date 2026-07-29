package s3

import (
	"encoding/xml"
	"net/http"
)

// APIError is an S3 error in the shape clients expect. Every SDK
// parses this document, so getting the code right matters more than
// the message: a client retries on SlowDown and gives up on
// AccessDenied, and it decides which by reading Code.
type APIError struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId,omitempty"`

	status int `xml:"-"`
}

// Error implements the error interface.
func (e *APIError) Error() string { return e.Code + ": " + e.Message }

// Status returns the HTTP status to send with this error.
func (e *APIError) Status() int {
	if e.status == 0 {
		return http.StatusInternalServerError
	}
	return e.status
}

func errorf(status int, code, message string) *APIError {
	return &APIError{Code: code, Message: message, status: status}
}

// The errors tana can produce. Codes and statuses follow the S3 API
// reference; anything tana does not implement answers NotImplemented
// rather than pretending, so a client fails loudly instead of assuming
// a silent success.
var (
	errNoSuchBucket = func() *APIError {
		return errorf(http.StatusNotFound, "NoSuchBucket", "The specified bucket does not exist")
	}
	errNoSuchKey = func() *APIError {
		return errorf(http.StatusNotFound, "NoSuchKey", "The specified key does not exist")
	}
	errNoSuchUpload = func() *APIError {
		return errorf(http.StatusNotFound, "NoSuchUpload", "The specified multipart upload does not exist")
	}
	errAccessDenied = func() *APIError {
		return errorf(http.StatusForbidden, "AccessDenied", "Access Denied")
	}
	errInvalidAccessKeyID = func() *APIError {
		return errorf(http.StatusForbidden, "InvalidAccessKeyId", "The access key ID does not exist in our records")
	}
	errSignatureMismatch = func() *APIError {
		return errorf(http.StatusForbidden, "SignatureDoesNotMatch",
			"The request signature we calculated does not match the signature you provided")
	}
	errMissingAuth = func() *APIError {
		return errorf(http.StatusForbidden, "MissingSecurityHeader", "The request is missing its authentication header")
	}
	errRequestExpired = func() *APIError {
		return errorf(http.StatusForbidden, "RequestTimeTooSkewed",
			"The difference between the request time and the server's time is too large")
	}
	errInvalidRequest = func(msg string) *APIError {
		return errorf(http.StatusBadRequest, "InvalidRequest", msg)
	}
	errInvalidArgument = func(msg string) *APIError {
		return errorf(http.StatusBadRequest, "InvalidArgument", msg)
	}
	errMalformedXML = func() *APIError {
		return errorf(http.StatusBadRequest, "MalformedXML", "The XML you provided was not well-formed")
	}
	errInvalidDigest = func() *APIError {
		return errorf(http.StatusBadRequest, "BadDigest",
			"The content sent did not match the checksum the client declared")
	}
	errInvalidPart = func() *APIError {
		return errorf(http.StatusBadRequest, "InvalidPart",
			"One or more of the requested parts was not found or its entity tag did not match")
	}
	errEntityTooLarge = func() *APIError {
		return errorf(http.StatusBadRequest, "EntityTooLarge", "The upload exceeds the maximum allowed object size")
	}
	errInvalidRange = func() *APIError {
		return errorf(http.StatusRequestedRangeNotSatisfiable, "InvalidRange",
			"The requested range is not satisfiable")
	}
	errNotImplemented = func(what string) *APIError {
		return errorf(http.StatusNotImplemented, "NotImplemented",
			"tana does not implement "+what+": it is sized for WordPress media, not for general S3")
	}
	errInternal = func() *APIError {
		return errorf(http.StatusInternalServerError, "InternalError",
			"We encountered an internal error. Please try again.")
	}
)
