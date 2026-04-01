// Package dynamodb provides a DynamoDB resource client for dkmigo.
package dynamodb

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/Antonipo/go-dkmio"
)

// ----- public error types -----

// MissingKeyError is returned when a required primary key attribute is absent.
type MissingKeyError struct{ Attr string }

func (e *MissingKeyError) Error() string {
	return fmt.Sprintf("dkmigo/dynamodb: missing required key attribute %q", e.Attr)
}

// ConditionError is returned when a DynamoDB conditional expression fails.
type ConditionError struct{ cause error }

func (e *ConditionError) Error() string { return "dkmigo/dynamodb: condition check failed" }
func (e *ConditionError) Unwrap() error { return e.cause }

// TableNotFoundError is returned when the target DynamoDB table does not exist.
type TableNotFoundError struct{ Table string }

func (e *TableNotFoundError) Error() string {
	return fmt.Sprintf("dkmigo/dynamodb: table %q not found", e.Table)
}

// ValidationError is returned for local validation failures or AWS-level
// validation errors (e.g. missing key, unsupported type, bad expression).
type ValidationError struct {
	msg   string
	cause error
}

func (e *ValidationError) Error() string {
	if e.cause != nil {
		return "dkmigo/dynamodb: validation error: " + e.cause.Error()
	}
	return "dkmigo/dynamodb: validation error: " + e.msg
}
func (e *ValidationError) Unwrap() error { return e.cause }

// ThrottlingError is returned when DynamoDB throttles the request.
type ThrottlingError struct{ cause error }

func (e *ThrottlingError) Error() string { return "dkmigo/dynamodb: request throttled" }
func (e *ThrottlingError) Unwrap() error { return e.cause }

// TransactionError is returned when a DynamoDB transaction is cancelled.
type TransactionError struct {
	Reasons []string
	cause   error
}

func (e *TransactionError) Error() string {
	return fmt.Sprintf("dkmigo/dynamodb: transaction cancelled (%d reasons)", len(e.Reasons))
}
func (e *TransactionError) Unwrap() error { return e.cause }

// InvalidProjectionError is returned when an attribute requested via Select
// is not part of an index's projection.
type InvalidProjectionError struct {
	Attr  string
	Index string
}

func (e *InvalidProjectionError) Error() string {
	return fmt.Sprintf("dkmigo/dynamodb: attribute %q not projected in index %q", e.Attr, e.Index)
}

// CollectionSizeError is returned when an LSI item collection exceeds 10 GB.
type CollectionSizeError struct{ cause error }

func (e *CollectionSizeError) Error() string {
	return "dkmigo/dynamodb: item collection size limit exceeded"
}
func (e *CollectionSizeError) Unwrap() error { return e.cause }

// ----- error classification -----

// mapError converts AWS SDK errors to typed dkmigo errors.
// Returns the mapped error and whether it is an infrastructure error
// (should trip the circuit breaker).
func mapError(err error) (mapped error, isInfra bool) {
	if err == nil {
		return nil, false
	}

	var condFailed *types.ConditionalCheckFailedException
	if errors.As(err, &condFailed) {
		return &ConditionError{cause: err}, false
	}

	var notFound *types.ResourceNotFoundException
	if errors.As(err, &notFound) {
		return &TableNotFoundError{}, false
	}

	var tableNotFound *types.TableNotFoundException
	if errors.As(err, &tableNotFound) {
		return &TableNotFoundError{}, false
	}

	var throughput *types.ProvisionedThroughputExceededException
	if errors.As(err, &throughput) {
		return &ThrottlingError{cause: err}, true
	}

	var requestLimit *types.RequestLimitExceeded
	if errors.As(err, &requestLimit) {
		return &ThrottlingError{cause: err}, true
	}

	var txCancel *types.TransactionCanceledException
	if errors.As(err, &txCancel) {
		reasons := make([]string, len(txCancel.CancellationReasons))
		for i, r := range txCancel.CancellationReasons {
			if r.Message != nil {
				reasons[i] = *r.Message
			}
		}
		return &TransactionError{Reasons: reasons, cause: err}, false
	}

	var txConflict *types.TransactionConflictException
	if errors.As(err, &txConflict) {
		return &TransactionError{cause: err}, false
	}

	var collectionSize *types.ItemCollectionSizeLimitExceededException
	if errors.As(err, &collectionSize) {
		return &CollectionSizeError{cause: err}, false
	}

	var internalErr *types.InternalServerError
	if errors.As(err, &internalErr) {
		return err, true
	}

	// dkmigo root-level errors (circuit breaker) pass through as-is.
	var circuitOpen *dkmigo.CircuitOpenError
	if errors.As(err, &circuitOpen) {
		return err, false
	}

	// Unknown errors are treated as infrastructure errors.
	return err, true
}

// isInfrastructureError returns true if err should count towards the
// circuit breaker failure threshold.
func isInfrastructureError(err error) bool {
	_, isInfra := mapError(err)
	return isInfra
}
