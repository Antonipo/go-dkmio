package dkmigo

// DkmioError is the base error type for all dkmigo errors.
// Use errors.As to match specific subtypes.
type DkmioError struct {
	msg   string
	cause error
}

func (e *DkmioError) Error() string {
	if e.cause != nil {
		return e.msg + ": " + e.cause.Error()
	}
	return e.msg
}

func (e *DkmioError) Unwrap() error { return e.cause }

// CircuitOpenError is returned when the circuit breaker is in OPEN state
// and the call is rejected without reaching AWS.
// Resource packages (dynamodb, s3…) check for this type to avoid counting
// circuit-open rejections as infrastructure failures.
type CircuitOpenError struct {
	DkmioError
}

func newCircuitOpenError() *CircuitOpenError {
	return &CircuitOpenError{DkmioError{msg: "circuit breaker is open"}}
}
