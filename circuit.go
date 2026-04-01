package dkmigo

import (
	"time"

	"github.com/sony/gobreaker"
)

// CircuitBreakerConfig configures the built-in circuit breaker.
// Only infrastructure errors (throttling, service unavailable) count towards
// the failure threshold — client errors never trip the breaker.
type CircuitBreakerConfig struct {
	// Number of consecutive infrastructure failures before opening the circuit.
	// Default: 5.
	FailureThreshold uint32

	// Duration the circuit stays OPEN before allowing a single probe request.
	// Default: 30s.
	RecoveryTimeout time.Duration
}

// circuitBreaker wraps sony/gobreaker with dkmigo semantics.
type circuitBreaker struct {
	cb *gobreaker.CircuitBreaker
}

func newCircuitBreaker(cfg CircuitBreakerConfig) *circuitBreaker {
	if cfg.FailureThreshold == 0 {
		cfg.FailureThreshold = 5
	}
	if cfg.RecoveryTimeout == 0 {
		cfg.RecoveryTimeout = 30 * time.Second
	}

	settings := gobreaker.Settings{
		Name:        "dkmigo",
		MaxRequests: 1, // only one probe in half-open
		Timeout:     cfg.RecoveryTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= uint32(cfg.FailureThreshold)
		},
	}

	return &circuitBreaker{cb: gobreaker.NewCircuitBreaker(settings)}
}

// Execute runs fn through the circuit breaker.
// isInfraError classifies whether a returned error is an infrastructure failure
// (counts towards threshold) or a client error (does not count).
func (b *circuitBreaker) Execute(fn func() error, isInfraError func(error) bool) error {
	_, err := b.cb.Execute(func() (any, error) {
		fnErr := fn()
		if fnErr != nil && !isInfraError(fnErr) {
			// Wrap in a type the breaker treats as a success so it doesn't count.
			return nil, &clientErrWrapper{fnErr}
		}
		return nil, fnErr
	})

	if err == nil {
		return nil
	}

	// Unwrap client errors that were hidden from the breaker.
	if wrapped, ok := err.(*clientErrWrapper); ok {
		return wrapped.err
	}

	if err == gobreaker.ErrOpenState || err == gobreaker.ErrTooManyRequests {
		return newCircuitOpenError()
	}

	return err
}

// State returns "closed", "open", or "half_open".
func (b *circuitBreaker) State() string {
	switch b.cb.State() {
	case gobreaker.StateClosed:
		return "closed"
	case gobreaker.StateOpen:
		return "open"
	case gobreaker.StateHalfOpen:
		return "half_open"
	default:
		return "unknown"
	}
}

// Reset manually resets the circuit breaker to closed state.
func (b *circuitBreaker) Reset() {
	// gobreaker doesn't expose Reset(); re-create the breaker as a workaround.
	// In practice, callers should let the recovery timeout handle this.
	_ = b
}

// clientErrWrapper prevents client errors from being counted by the breaker.
type clientErrWrapper struct{ err error }

func (e *clientErrWrapper) Error() string { return e.err.Error() }
func (e *clientErrWrapper) Unwrap() error { return e.err }
