package dynamodb

// helpers_test.go — shared test fixtures and helpers.

import (
	"context"
	"encoding/json"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/Antonipo/go-dkmio"
)

// ----- Shared test types -----

type testOrder struct {
	UserID    string  `json:"user_id" dkmio:"pk"`
	OrderID   string  `json:"order_id" dkmio:"sk"`
	Total     float64 `json:"total"`
	Status    string  `json:"status" dkmio:"gsi:gsi-status-date:pk"`
	CreatedAt string  `json:"created_at" dkmio:"gsi:gsi-status-date:sk"`
}

type testUser struct {
	UserID string `json:"user_id" dkmio:"pk"`
	Name   string `json:"name"`
}

// ----- Shared table descriptors -----

var testOrdersDesc = Table[testOrder]{
	Name: "orders",
	Indexes: IndexMap{
		"by_status": IndexDef{
			Name:       "gsi-status-date",
			Projection: ProjectionIncludeAttrs("total", "created_at"),
		},
	},
}

var testUsersDesc = Table[testUser]{Name: "users"}

// ----- Helpers -----

// newTestClient builds a *Client backed by a mockDynamo.
// It uses a no-op root dkmigo.Client so circuit breaker is disabled.
func newTestClient(mock *mockDynamo) *Client {
	root, _ := dkmigo.New(dkmigo.Config{
		Region:         "us-east-1",
		CircuitBreaker: nil, // disabled — we test errors directly
	})
	return newWithSvc(root, mock)
}

// bindOrders returns a bound *Table[testOrder] backed by mock.
func bindOrders(mock *mockDynamo) *Table[testOrder] {
	return testOrdersDesc.Bind(newTestClient(mock))
}

// bindUsers returns a bound *Table[testUser] backed by mock.
func bindUsers(mock *mockDynamo) *Table[testUser] {
	return testUsersDesc.Bind(newTestClient(mock))
}

// marshalOrder marshals a testOrder to a DynamoDB attribute map using json tags
// as attribute names (consistent with how the library marshals items).
func marshalOrder(o testOrder) map[string]dbtypes.AttributeValue {
	return mustMarshalToAV(o)
}

// marshalUser marshals a testUser to a DynamoDB attribute map using json tags.
func marshalUser(u testUser) map[string]dbtypes.AttributeValue {
	return mustMarshalToAV(u)
}

// mustMarshalToAV converts a struct to DynamoDB attributes via JSON round-trip
// so that json tags are used as attribute names.
func mustMarshalToAV(v any) map[string]dbtypes.AttributeValue {
	b, _ := json.Marshal(v)
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	av, _ := attributevalue.MarshalMap(m)
	return av
}

// ctx is a ready-to-use background context for tests.
var ctx = context.Background()
