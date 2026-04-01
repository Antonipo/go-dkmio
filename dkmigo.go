// Package dkmigo is a lightweight Object-Key Mapper for AWS DynamoDB and other
// AWS resources. It provides a fluent, type-safe API built on top of the AWS
// SDK v2, with automatic expression building, reserved-word escaping, pagination,
// and circuit-breaker protection.
//
// Usage:
//
//	cfg := dkmigo.Config{Region: "us-east-1"}
//	root, err := dkmigo.New(cfg)
//
//	dynamo, err := dynamodb.New(root)
//	orders := dynamodb.Table[Order]{Name: "orders"}.Bind(dynamo)
//
//	item, err := orders.Get(ctx, "u1", "o1")
package dkmigo

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// Config holds the root configuration for the dkmigo client.
// Resource-specific clients (dynamodb, s3, sqs…) are created from a Client.
type Config struct {
	// AWS region (e.g. "us-east-1"). If empty, falls back to AWS_REGION env var.
	Region string

	// EndpointURL overrides the AWS service endpoint.
	// Useful for local development with DynamoDB Local: "http://localhost:8000"
	EndpointURL string

	// Static credentials — prefer IAM roles / env vars in production.
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string

	// CircuitBreaker configures the built-in circuit breaker.
	// Set to nil to disable it entirely.
	CircuitBreaker *CircuitBreakerConfig
}

// Client is the root dkmigo client. Create exactly one per application and
// pass it to resource-specific constructors (e.g. dynamodb.New(root)).
// Client is safe for concurrent use.
type Client struct {
	cfg    Config
	awsCfg aws.Config
	cb     *circuitBreaker // nil when disabled
}

// New creates a root Client from cfg.
// AWS credentials are resolved in order: explicit static creds → env vars →
// shared credentials file → IAM role.
func New(cfg Config) (*Client, error) {
	opts := []func(*awsconfig.LoadOptions) error{}

	if cfg.Region != "" {
		opts = append(opts, awsconfig.WithRegion(cfg.Region))
	}

	if cfg.AccessKeyID != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(
				cfg.AccessKeyID,
				cfg.SecretAccessKey,
				cfg.SessionToken,
			),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, err
	}

	c := &Client{
		cfg:    cfg,
		awsCfg: awsCfg,
	}

	if cfg.CircuitBreaker != nil {
		c.cb = newCircuitBreaker(*cfg.CircuitBreaker)
	}

	return c, nil
}

// AWSConfig returns the underlying aws.Config.
// Resource-specific clients use this to create their AWS service clients.
func (c *Client) AWSConfig() aws.Config {
	return c.awsCfg
}

// EndpointURL returns the custom endpoint URL, or empty string if not set.
func (c *Client) EndpointURL() string {
	return c.cfg.EndpointURL
}

// CircuitBreakerState returns the current circuit breaker state:
// "closed", "open", or "half_open". Returns "disabled" if none is configured.
func (c *Client) CircuitBreakerState() string {
	if c.cb == nil {
		return "disabled"
	}
	return c.cb.State()
}

// CircuitBreakerReset manually resets the circuit breaker to closed state.
func (c *Client) CircuitBreakerReset() {
	if c.cb != nil {
		c.cb.Reset()
	}
}

// Execute runs fn through the circuit breaker if one is configured.
// isInfraError should return true for transient infrastructure errors
// (throttling, unavailable) that should count towards the failure threshold.
// Client errors (bad input, condition failures) must not trip the breaker.
//
// This method is intended for use by resource-specific packages (dynamodb, s3…)
// and should not be called directly by application code.
func (c *Client) Execute(fn func() error, isInfraError func(error) bool) error {
	if c.cb == nil {
		return fn()
	}
	return c.cb.Execute(fn, isInfraError)
}
