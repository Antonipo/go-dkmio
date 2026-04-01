package dynamodb

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/Antonipo/go-dkmio"
)

// dynamoSvc is the subset of the AWS DynamoDB API used by this package.
// Defining it as an interface allows tests to inject a mock without needing
// a real AWS connection.
type dynamoSvc interface {
	GetItem(ctx context.Context, input *awsdynamodb.GetItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.GetItemOutput, error)
	PutItem(ctx context.Context, input *awsdynamodb.PutItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.PutItemOutput, error)
	UpdateItem(ctx context.Context, input *awsdynamodb.UpdateItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.UpdateItemOutput, error)
	DeleteItem(ctx context.Context, input *awsdynamodb.DeleteItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.DeleteItemOutput, error)
	Query(ctx context.Context, input *awsdynamodb.QueryInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error)
	Scan(ctx context.Context, input *awsdynamodb.ScanInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.ScanOutput, error)
	BatchGetItem(ctx context.Context, input *awsdynamodb.BatchGetItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchGetItemOutput, error)
	BatchWriteItem(ctx context.Context, input *awsdynamodb.BatchWriteItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchWriteItemOutput, error)
	TransactWriteItems(ctx context.Context, input *awsdynamodb.TransactWriteItemsInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactWriteItemsOutput, error)
	TransactGetItems(ctx context.Context, input *awsdynamodb.TransactGetItemsInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactGetItemsOutput, error)
}

// Client is the DynamoDB resource client.
// Create one via New and pass it to Table descriptors with .Bind(client).
// Client is safe for concurrent use.
type Client struct {
	root *dkmigo.Client
	svc  dynamoSvc
}

// New creates a DynamoDB Client from a root dkmigo.Client.
func New(root *dkmigo.Client) (*Client, error) {
	opts := []func(*awsdynamodb.Options){}

	if ep := root.EndpointURL(); ep != "" {
		opts = append(opts, func(o *awsdynamodb.Options) {
			o.BaseEndpoint = aws.String(ep)
		})
	}

	svc := awsdynamodb.NewFromConfig(root.AWSConfig(), opts...)
	return &Client{root: root, svc: svc}, nil
}

// newWithSvc creates a Client with an injected service — for use in tests only.
func newWithSvc(root *dkmigo.Client, svc dynamoSvc) *Client {
	return &Client{root: root, svc: svc}
}

// service returns the underlying DynamoDB service client.
func (c *Client) service() dynamoSvc { return c.svc }

// execute runs fn with circuit-breaker protection from the root client.
func (c *Client) execute(_ context.Context, fn func() error) error {
	return c.root.Execute(fn, isInfrastructureError)
}
