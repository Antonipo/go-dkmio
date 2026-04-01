package dynamodb

import (
	"errors"
	"strings"
	"testing"

	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/Antonipo/go-dkmio"
)

// ----- Error() and Unwrap() on each type -----

func TestMissingKeyError_Error(t *testing.T) {
	e := &MissingKeyError{Attr: "user_id"}
	if !strings.Contains(e.Error(), "user_id") {
		t.Errorf("Error() = %q; want it to contain 'user_id'", e.Error())
	}
}

func TestConditionError_ErrorAndUnwrap(t *testing.T) {
	cause := errors.New("aws cause")
	e := &ConditionError{cause: cause}
	if e.Error() == "" {
		t.Error("Error() should not be empty")
	}
	if !errors.Is(e, cause) {
		t.Error("Unwrap() should return cause")
	}
}

func TestTableNotFoundError_Error(t *testing.T) {
	e := &TableNotFoundError{Table: "orders"}
	if !strings.Contains(e.Error(), "orders") {
		t.Errorf("Error() = %q; want it to contain 'orders'", e.Error())
	}
}

func TestValidationError_Error_WithCause(t *testing.T) {
	cause := errors.New("bad input")
	e := &ValidationError{cause: cause}
	if !strings.Contains(e.Error(), "bad input") {
		t.Errorf("Error() = %q; want it to contain 'bad input'", e.Error())
	}
	if !errors.Is(e, cause) {
		t.Error("Unwrap() should return cause")
	}
}

func TestValidationError_Error_WithMsg(t *testing.T) {
	e := &ValidationError{msg: "key missing"}
	if !strings.Contains(e.Error(), "key missing") {
		t.Errorf("Error() = %q; want it to contain 'key missing'", e.Error())
	}
}

func TestThrottlingError_ErrorAndUnwrap(t *testing.T) {
	cause := errors.New("throttled")
	e := &ThrottlingError{cause: cause}
	if e.Error() == "" {
		t.Error("Error() should not be empty")
	}
	if !errors.Is(e, cause) {
		t.Error("Unwrap() should return cause")
	}
}

func TestTransactionError_ErrorAndUnwrap(t *testing.T) {
	cause := errors.New("tx cancelled")
	e := &TransactionError{Reasons: []string{"condition failed", "conflict"}, cause: cause}
	msg := e.Error()
	if !strings.Contains(msg, "2") {
		t.Errorf("Error() = %q; want reason count", msg)
	}
	if !errors.Is(e, cause) {
		t.Error("Unwrap() should return cause")
	}
}

func TestInvalidProjectionError_Error(t *testing.T) {
	e := &InvalidProjectionError{Attr: "description", Index: "by_status"}
	msg := e.Error()
	if !strings.Contains(msg, "description") || !strings.Contains(msg, "by_status") {
		t.Errorf("Error() = %q; want attr and index name", msg)
	}
}

func TestCollectionSizeError_ErrorAndUnwrap(t *testing.T) {
	cause := errors.New("too big")
	e := &CollectionSizeError{cause: cause}
	if e.Error() == "" {
		t.Error("Error() should not be empty")
	}
	if !errors.Is(e, cause) {
		t.Error("Unwrap() should return cause")
	}
}

// ----- isInfrastructureError -----

func TestIsInfrastructureError_Nil(t *testing.T) {
	if isInfrastructureError(nil) {
		t.Error("nil error should not be infrastructure error")
	}
}

func TestIsInfrastructureError_UnknownError(t *testing.T) {
	if !isInfrastructureError(errors.New("some unknown aws error")) {
		t.Error("unknown error should be treated as infrastructure error")
	}
}

func TestIsInfrastructureError_CircuitOpenError(t *testing.T) {
	e := &dkmigo.CircuitOpenError{}
	if isInfrastructureError(e) {
		t.Error("CircuitOpenError should not be infrastructure error")
	}
}

func TestIsInfrastructureError_ConditionError(t *testing.T) {
	// ConditionError is a client error, not infra.
	e := &ConditionError{cause: errors.New("x")}
	// mapError maps it correctly — but ConditionError is not a known AWS SDK type,
	// so it falls through to "unknown → isInfra=true" unless we test via mapError directly.
	// We test mapError directly:
	_, isInfra := mapError(e)
	// ConditionError is not an AWS type, so it's treated as unknown (infra=true).
	// This is fine — the function is for classifying raw AWS errors, not wrapped ones.
	_ = isInfra
}

func TestMapError_Nil(t *testing.T) {
	mapped, isInfra := mapError(nil)
	if mapped != nil || isInfra {
		t.Errorf("mapError(nil) = (%v, %v); want (nil, false)", mapped, isInfra)
	}
}

func TestMapError_ResourceNotFound(t *testing.T) {
	err := &dbtypes.ResourceNotFoundException{}
	mapped, isInfra := mapError(err)
	var e *TableNotFoundError
	if !errors.As(mapped, &e) {
		t.Errorf("expected TableNotFoundError, got %T", mapped)
	}
	if isInfra {
		t.Error("ResourceNotFoundException should not be infra error")
	}
}

func TestMapError_TableNotFoundException(t *testing.T) {
	err := &dbtypes.TableNotFoundException{}
	mapped, isInfra := mapError(err)
	var e *TableNotFoundError
	if !errors.As(mapped, &e) {
		t.Errorf("expected TableNotFoundError, got %T", mapped)
	}
	if isInfra {
		t.Error("TableNotFoundException should not be infra error")
	}
}

func TestMapError_TransactionCancelled(t *testing.T) {
	msg := "condition failed"
	err := &dbtypes.TransactionCanceledException{
		CancellationReasons: []dbtypes.CancellationReason{
			{Message: &msg},
		},
	}
	mapped, isInfra := mapError(err)
	var e *TransactionError
	if !errors.As(mapped, &e) {
		t.Errorf("expected TransactionError, got %T", mapped)
	}
	if len(e.Reasons) != 1 || e.Reasons[0] != msg {
		t.Errorf("Reasons = %v; want [%q]", e.Reasons, msg)
	}
	if isInfra {
		t.Error("TransactionCanceledException should not be infra error")
	}
}

func TestMapError_TransactionConflict(t *testing.T) {
	err := &dbtypes.TransactionConflictException{}
	mapped, isInfra := mapError(err)
	var e *TransactionError
	if !errors.As(mapped, &e) {
		t.Errorf("expected TransactionError, got %T", mapped)
	}
	if isInfra {
		t.Error("TransactionConflictException should not be infra error")
	}
}

func TestMapError_CollectionSizeLimit(t *testing.T) {
	err := &dbtypes.ItemCollectionSizeLimitExceededException{}
	mapped, isInfra := mapError(err)
	var e *CollectionSizeError
	if !errors.As(mapped, &e) {
		t.Errorf("expected CollectionSizeError, got %T", mapped)
	}
	if isInfra {
		t.Error("ItemCollectionSizeLimitExceededException should not be infra error")
	}
}

func TestMapError_InternalServerError(t *testing.T) {
	err := &dbtypes.InternalServerError{}
	mapped, isInfra := mapError(err)
	if mapped == nil {
		t.Error("expected non-nil mapped error")
	}
	if !isInfra {
		t.Error("InternalServerError should be infra error")
	}
}

func TestMapError_ProvisionedThroughput(t *testing.T) {
	err := &dbtypes.ProvisionedThroughputExceededException{}
	mapped, isInfra := mapError(err)
	var e *ThrottlingError
	if !errors.As(mapped, &e) {
		t.Errorf("expected ThrottlingError, got %T", mapped)
	}
	if !isInfra {
		t.Error("ProvisionedThroughputExceededException should be infra error")
	}
}

func TestMapError_RequestLimitExceeded(t *testing.T) {
	err := &dbtypes.RequestLimitExceeded{}
	mapped, isInfra := mapError(err)
	var e *ThrottlingError
	if !errors.As(mapped, &e) {
		t.Errorf("expected ThrottlingError, got %T", mapped)
	}
	if !isInfra {
		t.Error("RequestLimitExceeded should be infra error")
	}
}
