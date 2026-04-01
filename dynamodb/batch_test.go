package dynamodb

import (
	"context"
	"testing"

	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ----- BatchGet -----

func TestBatchGet_ReturnItems(t *testing.T) {
	o1 := testOrder{UserID: "u1", OrderID: "o1", Total: 10}
	o2 := testOrder{UserID: "u1", OrderID: "o2", Total: 20}

	mock := &mockDynamo{
		batchGetItem: func(_ context.Context, in *awsdynamodb.BatchGetItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchGetItemOutput, error) {
			return &awsdynamodb.BatchGetItemOutput{
				Responses: map[string][]map[string]dbtypes.AttributeValue{
					"orders": {marshalOrder(o1), marshalOrder(o2)},
				},
			}, nil
		},
	}

	results, err := bindOrders(mock).BatchGet(ctx, []Keys{
		{"user_id": "u1", "order_id": "o1"},
		{"user_id": "u1", "order_id": "o2"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("len(results) = %d; want 2", len(results))
	}
}

func TestBatchGet_ReturnsNilForMissingItems(t *testing.T) {
	o1 := testOrder{UserID: "u1", OrderID: "o1"}

	mock := &mockDynamo{
		batchGetItem: func(_ context.Context, _ *awsdynamodb.BatchGetItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchGetItemOutput, error) {
			// Only return one item even though two were requested.
			return &awsdynamodb.BatchGetItemOutput{
				Responses: map[string][]map[string]dbtypes.AttributeValue{
					"orders": {marshalOrder(o1)},
				},
			}, nil
		},
	}

	results, err := bindOrders(mock).BatchGet(ctx, []Keys{
		{"user_id": "u1", "order_id": "o1"},
		{"user_id": "u1", "order_id": "o_missing"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Errorf("len(results) = %d; want 2", len(results))
	}
	// The missing item should be nil — we can't guarantee ordering since
	// BatchGetItem doesn't preserve it, so just check at least one is non-nil.
	nonNil := 0
	for _, r := range results {
		if r != nil {
			nonNil++
		}
	}
	if nonNil != 1 {
		t.Errorf("expected exactly 1 non-nil result, got %d", nonNil)
	}
}

func TestBatchGet_UsesCorrectTableName(t *testing.T) {
	var gotTableName string
	mock := &mockDynamo{
		batchGetItem: func(_ context.Context, in *awsdynamodb.BatchGetItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchGetItemOutput, error) {
			for name := range in.RequestItems {
				gotTableName = name
			}
			return &awsdynamodb.BatchGetItemOutput{
				Responses: map[string][]map[string]dbtypes.AttributeValue{},
			}, nil
		},
	}

	_, err := bindOrders(mock).BatchGet(ctx, []Keys{
		{"user_id": "u1", "order_id": "o1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotTableName != "orders" {
		t.Errorf("table name = %q; want orders", gotTableName)
	}
}

func TestBatchGet_WithConsistentRead(t *testing.T) {
	var gotConsistent *bool
	mock := &mockDynamo{
		batchGetItem: func(_ context.Context, in *awsdynamodb.BatchGetItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchGetItemOutput, error) {
			if ka, ok := in.RequestItems["orders"]; ok {
				gotConsistent = ka.ConsistentRead
			}
			return &awsdynamodb.BatchGetItemOutput{
				Responses: map[string][]map[string]dbtypes.AttributeValue{},
			}, nil
		},
	}

	_, err := bindOrders(mock).BatchGet(ctx, []Keys{
		{"user_id": "u1", "order_id": "o1"},
	}, WithConsistentRead())
	if err != nil {
		t.Fatal(err)
	}
	if gotConsistent == nil || !*gotConsistent {
		t.Error("expected ConsistentRead=true")
	}
}

func TestBatchGet_EmptyKeys_ReturnsEmpty(t *testing.T) {
	results, err := bindOrders(&mockDynamo{}).BatchGet(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 0 {
		t.Errorf("expected empty result for empty keys, got %d", len(results))
	}
}

func TestBatchGet_NumericKey_MatchesCorrectly(t *testing.T) {
	// Tests the avString N-branch by using a numeric partition key.
	type numItem struct {
		ID    int    `json:"id" dkmio:"pk"`
		Label string `json:"label"`
	}
	numTable := Table[numItem]{Name: "num_items"}.Bind(newTestClient(&mockDynamo{
		batchGetItem: func(_ context.Context, in *awsdynamodb.BatchGetItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchGetItemOutput, error) {
			var item map[string]dbtypes.AttributeValue
			for _, avKey := range in.RequestItems["num_items"].Keys {
				item = map[string]dbtypes.AttributeValue{
					"id":    avKey["id"],
					"label": &dbtypes.AttributeValueMemberS{Value: "found"},
				}
			}
			return &awsdynamodb.BatchGetItemOutput{
				Responses: map[string][]map[string]dbtypes.AttributeValue{
					"num_items": {item},
				},
			}, nil
		},
	}))

	results, err := numTable.BatchGet(ctx, []Keys{{"id": 42}})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0] == nil {
		t.Fatalf("expected 1 result, got %v", results)
	}
	if results[0].Label != "found" {
		t.Errorf("Label = %q; want found", results[0].Label)
	}
}

func TestBatchWriter_Delete_MissingSK_IsIgnored(t *testing.T) {
	// Delete with missing SK silently ignores the item (error path in buildKey).
	called := false
	mock := &mockDynamo{
		batchWriteItem: func(_ context.Context, _ *awsdynamodb.BatchWriteItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchWriteItemOutput, error) {
			called = true
			return &awsdynamodb.BatchWriteItemOutput{}, nil
		},
	}

	// Call Delete without SK on a table that requires SK — buildKey returns error.
	err := bindOrders(mock).BatchWrite(ctx).
		Delete("u1" /* missing order_id SK */).
		Exec()
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Error("expected no BatchWriteItem call when all deletes are invalid")
	}
}

func TestBatchGet_WithProjection(t *testing.T) {
	var gotProjection *string
	mock := &mockDynamo{
		batchGetItem: func(_ context.Context, in *awsdynamodb.BatchGetItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchGetItemOutput, error) {
			if ka, ok := in.RequestItems["orders"]; ok {
				gotProjection = ka.ProjectionExpression
			}
			return &awsdynamodb.BatchGetItemOutput{
				Responses: map[string][]map[string]dbtypes.AttributeValue{},
			}, nil
		},
	}

	_, err := bindOrders(mock).BatchGet(ctx, []Keys{
		{"user_id": "u1", "order_id": "o1"},
	}, WithProjection("total", "status"))
	if err != nil {
		t.Fatal(err)
	}
	if gotProjection == nil || *gotProjection == "" {
		t.Error("expected ProjectionExpression to be set")
	}
}

// ----- BatchWriter -----

func TestBatchWriter_Put_SendsItems(t *testing.T) {
	var capturedRequests []dbtypes.WriteRequest
	mock := &mockDynamo{
		batchWriteItem: func(_ context.Context, in *awsdynamodb.BatchWriteItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchWriteItemOutput, error) {
			capturedRequests = in.RequestItems["orders"]
			return &awsdynamodb.BatchWriteItemOutput{}, nil
		},
	}

	err := bindOrders(mock).BatchWrite(ctx).
		Put(
			testOrder{UserID: "u1", OrderID: "o1"},
			testOrder{UserID: "u1", OrderID: "o2"},
		).
		Exec()
	if err != nil {
		t.Fatal(err)
	}
	if len(capturedRequests) != 2 {
		t.Errorf("expected 2 write requests, got %d", len(capturedRequests))
	}
	for _, req := range capturedRequests {
		if req.PutRequest == nil {
			t.Error("expected PutRequest, got nil")
		}
	}
}

func TestBatchWriter_Delete_SendsDeleteRequest(t *testing.T) {
	var capturedRequests []dbtypes.WriteRequest
	mock := &mockDynamo{
		batchWriteItem: func(_ context.Context, in *awsdynamodb.BatchWriteItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchWriteItemOutput, error) {
			capturedRequests = in.RequestItems["orders"]
			return &awsdynamodb.BatchWriteItemOutput{}, nil
		},
	}

	err := bindOrders(mock).BatchWrite(ctx).
		Delete("u1", "o1").
		Exec()
	if err != nil {
		t.Fatal(err)
	}
	if len(capturedRequests) != 1 {
		t.Errorf("expected 1 write request, got %d", len(capturedRequests))
	}
	if capturedRequests[0].DeleteRequest == nil {
		t.Error("expected DeleteRequest, got nil")
	}
}

func TestBatchWriter_MixedPutDelete(t *testing.T) {
	var capturedRequests []dbtypes.WriteRequest
	mock := &mockDynamo{
		batchWriteItem: func(_ context.Context, in *awsdynamodb.BatchWriteItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchWriteItemOutput, error) {
			capturedRequests = in.RequestItems["orders"]
			return &awsdynamodb.BatchWriteItemOutput{}, nil
		},
	}

	err := bindOrders(mock).BatchWrite(ctx).
		Put(testOrder{UserID: "u1", OrderID: "o_new"}).
		Delete("u1", "o_old").
		Exec()
	if err != nil {
		t.Fatal(err)
	}
	if len(capturedRequests) != 2 {
		t.Errorf("expected 2 requests, got %d", len(capturedRequests))
	}
}

func TestBatchWriter_Exec_NoOps_DoesNothing(t *testing.T) {
	// Empty BatchWriter should not call BatchWriteItem.
	err := bindOrders(&mockDynamo{}).BatchWrite(ctx).Exec()
	if err != nil {
		t.Fatalf("empty BatchWrite.Exec should not error: %v", err)
	}
}

func TestBatchWriter_RetriesUnprocessedItems(t *testing.T) {
	callCount := 0
	o := testOrder{UserID: "u1", OrderID: "o1"}
	mock := &mockDynamo{
		batchWriteItem: func(_ context.Context, in *awsdynamodb.BatchWriteItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchWriteItemOutput, error) {
			callCount++
			out := &awsdynamodb.BatchWriteItemOutput{}
			// Return unprocessed items on first call only.
			if callCount == 1 {
				av, _ := marshalItem(o)
				out.UnprocessedItems = map[string][]dbtypes.WriteRequest{
					"orders": {{PutRequest: &dbtypes.PutRequest{Item: av}}},
				}
			}
			return out, nil
		},
	}

	err := bindOrders(mock).BatchWrite(ctx).
		Put(o).
		Exec()
	if err != nil {
		t.Fatal(err)
	}
	if callCount < 2 {
		t.Errorf("expected at least 2 calls (retry), got %d", callCount)
	}
}
