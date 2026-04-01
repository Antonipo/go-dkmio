package dynamodb

import (
	"context"
	"errors"
	"strings"
	"testing"

	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ----- Query -----

func TestQuery_BuildsKeyCondition(t *testing.T) {
	var gotInput *awsdynamodb.QueryInput
	mock := &mockDynamo{
		query: func(_ context.Context, in *awsdynamodb.QueryInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
			gotInput = in
			return &awsdynamodb.QueryOutput{Items: nil, Count: 0}, nil
		},
	}

	_, err := bindOrders(mock).Query(ctx, "u1").Exec()
	if err != nil {
		t.Fatal(err)
	}
	if gotInput.KeyConditionExpression == nil {
		t.Fatal("expected KeyConditionExpression")
	}
	if !strings.Contains(*gotInput.KeyConditionExpression, " = ") {
		t.Errorf("KeyConditionExpression = %q; expected EQ clause", *gotInput.KeyConditionExpression)
	}
}

func TestQuery_WithWhere_AppendsSKCondition(t *testing.T) {
	var gotKCE *string
	mock := &mockDynamo{
		query: func(_ context.Context, in *awsdynamodb.QueryInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
			gotKCE = in.KeyConditionExpression
			return &awsdynamodb.QueryOutput{}, nil
		},
	}

	_, err := bindOrders(mock).Query(ctx, "u1").Where(SKGTE("o_100")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(*gotKCE, "AND") || !strings.Contains(*gotKCE, ">=") {
		t.Errorf("KeyConditionExpression = %q; expected AND and >= for SKGTE", *gotKCE)
	}
}

func TestQuery_WithFilter_SetsFilterExpression(t *testing.T) {
	var gotFilter *string
	mock := &mockDynamo{
		query: func(_ context.Context, in *awsdynamodb.QueryInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
			gotFilter = in.FilterExpression
			return &awsdynamodb.QueryOutput{}, nil
		},
	}

	_, err := bindOrders(mock).Query(ctx, "u1").Filter(EQ("status", "shipped"), GTE("total", 50)).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if gotFilter == nil {
		t.Fatal("expected FilterExpression")
	}
	if !strings.Contains(*gotFilter, "AND") {
		t.Errorf("FilterExpression = %q; expected AND for two conditions", *gotFilter)
	}
}

func TestQuery_Limit_SetsLimit(t *testing.T) {
	var gotLimit *int32
	mock := &mockDynamo{
		query: func(_ context.Context, in *awsdynamodb.QueryInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
			gotLimit = in.Limit
			return &awsdynamodb.QueryOutput{}, nil
		},
	}

	_, err := bindOrders(mock).Query(ctx, "u1").Limit(10).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if gotLimit == nil || *gotLimit != 10 {
		t.Errorf("Limit = %v; want 10", gotLimit)
	}
}

func TestQuery_ScanForwardFalse_SetsFalse(t *testing.T) {
	var gotForward *bool
	mock := &mockDynamo{
		query: func(_ context.Context, in *awsdynamodb.QueryInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
			gotForward = in.ScanIndexForward
			return &awsdynamodb.QueryOutput{}, nil
		},
	}

	_, err := bindOrders(mock).Query(ctx, "u1").ScanForward(false).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if gotForward == nil || *gotForward {
		t.Errorf("ScanIndexForward = %v; want false", gotForward)
	}
}

func TestQuery_Consistent_SetsConsistentRead(t *testing.T) {
	var gotConsistent *bool
	mock := &mockDynamo{
		query: func(_ context.Context, in *awsdynamodb.QueryInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
			gotConsistent = in.ConsistentRead
			return &awsdynamodb.QueryOutput{}, nil
		},
	}

	_, err := bindOrders(mock).Query(ctx, "u1").Consistent().Exec()
	if err != nil {
		t.Fatal(err)
	}
	if gotConsistent == nil || !*gotConsistent {
		t.Error("expected ConsistentRead=true")
	}
}

func TestQuery_ReturnsItems(t *testing.T) {
	o1 := testOrder{UserID: "u1", OrderID: "o1", Total: 10}
	o2 := testOrder{UserID: "u1", OrderID: "o2", Total: 20}
	mock := &mockDynamo{
		query: func(_ context.Context, _ *awsdynamodb.QueryInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
			return &awsdynamodb.QueryOutput{
				Items: []map[string]dbtypes.AttributeValue{
					marshalOrder(o1),
					marshalOrder(o2),
				},
				Count: 2,
			}, nil
		},
	}

	result, err := bindOrders(mock).Query(ctx, "u1").Exec()
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 2 {
		t.Errorf("len(Items) = %d; want 2", len(result.Items))
	}
	if result.Items[0].OrderID != "o1" {
		t.Errorf("Items[0].OrderID = %q; want o1", result.Items[0].OrderID)
	}
}

func TestQuery_HasMore_TrueWhenLastKeyPresent(t *testing.T) {
	lastKey := map[string]dbtypes.AttributeValue{
		"user_id": &dbtypes.AttributeValueMemberS{Value: "u1"},
	}
	mock := &mockDynamo{
		query: func(_ context.Context, _ *awsdynamodb.QueryInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
			return &awsdynamodb.QueryOutput{LastEvaluatedKey: lastKey}, nil
		},
	}

	result, err := bindOrders(mock).Query(ctx, "u1").Exec()
	if err != nil {
		t.Fatal(err)
	}
	if !result.HasMore() {
		t.Error("expected HasMore()=true when LastEvaluatedKey is set")
	}
}

func TestQuery_FetchAll_PaginatesUntilEmpty(t *testing.T) {
	callCount := 0
	lastKey := map[string]dbtypes.AttributeValue{
		"user_id": &dbtypes.AttributeValueMemberS{Value: "u1"},
	}
	mock := &mockDynamo{
		query: func(_ context.Context, in *awsdynamodb.QueryInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
			callCount++
			o := testOrder{UserID: "u1", OrderID: "o" + string(rune('0'+callCount))}
			out := &awsdynamodb.QueryOutput{
				Items: []map[string]dbtypes.AttributeValue{marshalOrder(o)},
				Count: 1,
			}
			// Return last key on first two calls only.
			if callCount < 3 {
				out.LastEvaluatedKey = lastKey
			}
			return out, nil
		},
	}

	all, err := bindOrders(mock).Query(ctx, "u1").FetchAll(0)
	if err != nil {
		t.Fatal(err)
	}
	if callCount != 3 {
		t.Errorf("expected 3 DynamoDB calls, got %d", callCount)
	}
	if len(all) != 3 {
		t.Errorf("expected 3 items total, got %d", len(all))
	}
}

func TestQuery_FetchAll_RespectsMaxItems(t *testing.T) {
	mock := &mockDynamo{
		query: func(_ context.Context, _ *awsdynamodb.QueryInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
			items := make([]map[string]dbtypes.AttributeValue, 5)
			for i := range items {
				items[i] = marshalOrder(testOrder{UserID: "u1", OrderID: "o" + string(rune('0'+i))})
			}
			return &awsdynamodb.QueryOutput{Items: items, Count: 5}, nil
		},
	}

	all, err := bindOrders(mock).Query(ctx, "u1").FetchAll(3)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Errorf("expected 3 items (maxItems), got %d", len(all))
	}
}

func TestQuery_Count_SumsAcrossPages(t *testing.T) {
	callCount := 0
	lastKey := map[string]dbtypes.AttributeValue{
		"user_id": &dbtypes.AttributeValueMemberS{Value: "u1"},
	}
	mock := &mockDynamo{
		query: func(_ context.Context, _ *awsdynamodb.QueryInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
			callCount++
			out := &awsdynamodb.QueryOutput{Count: 5}
			if callCount == 1 {
				out.LastEvaluatedKey = lastKey
			}
			return out, nil
		},
	}

	count, err := bindOrders(mock).Query(ctx, "u1").Count()
	if err != nil {
		t.Fatal(err)
	}
	if count != 10 {
		t.Errorf("Count = %d; want 10 (2 pages × 5)", count)
	}
}

func TestQuery_StartFrom_SetsExclusiveStartKey(t *testing.T) {
	lastKey := map[string]dbtypes.AttributeValue{
		"user_id": &dbtypes.AttributeValueMemberS{Value: "u1"},
	}
	var gotStartKey map[string]dbtypes.AttributeValue
	mock := &mockDynamo{
		query: func(_ context.Context, in *awsdynamodb.QueryInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
			gotStartKey = in.ExclusiveStartKey
			return &awsdynamodb.QueryOutput{}, nil
		},
	}

	_, err := bindOrders(mock).Query(ctx, "u1").StartFrom(lastKey).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if len(gotStartKey) == 0 {
		t.Error("expected ExclusiveStartKey to be set")
	}
}

// ----- Scan -----

func TestScan_BuildsInput(t *testing.T) {
	var gotInput *awsdynamodb.ScanInput
	mock := &mockDynamo{
		scan: func(_ context.Context, in *awsdynamodb.ScanInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.ScanOutput, error) {
			gotInput = in
			return &awsdynamodb.ScanOutput{}, nil
		},
	}

	_, err := bindOrders(mock).Scan(ctx).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if *gotInput.TableName != "orders" {
		t.Errorf("TableName = %q; want orders", *gotInput.TableName)
	}
}

func TestScan_WithFilter(t *testing.T) {
	var gotFilter *string
	mock := &mockDynamo{
		scan: func(_ context.Context, in *awsdynamodb.ScanInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.ScanOutput, error) {
			gotFilter = in.FilterExpression
			return &awsdynamodb.ScanOutput{}, nil
		},
	}

	_, err := bindOrders(mock).Scan(ctx).Filter(EQ("status", "pending")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if gotFilter == nil {
		t.Fatal("expected FilterExpression on Scan")
	}
}

// ----- Index query -----

func TestIndexQuery_SetsIndexName(t *testing.T) {
	var gotIndexName *string
	mock := &mockDynamo{
		query: func(_ context.Context, in *awsdynamodb.QueryInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
			gotIndexName = in.IndexName
			return &awsdynamodb.QueryOutput{}, nil
		},
	}

	_, err := bindOrders(mock).Index("by_status").Query(ctx, "shipped").Exec()
	if err != nil {
		t.Fatal(err)
	}
	if gotIndexName == nil || *gotIndexName != "gsi-status-date" {
		t.Errorf("IndexName = %v; want gsi-status-date", gotIndexName)
	}
}

func TestIndexQuery_ProjectionValidation_Rejects(t *testing.T) {
	// "description" is not in the by_status ProjectionInclude list and is not a key attr.
	_, err := bindOrders(&mockDynamo{}).Index("by_status").Query(ctx, "shipped").
		Select("description").
		Exec()

	var ipe *InvalidProjectionError
	if !errors.As(err, &ipe) {
		t.Errorf("expected InvalidProjectionError, got %T: %v", err, err)
	}
}

func TestIndexQuery_ProjectionValidation_AllowsProjectedAttrs(t *testing.T) {
	mock := &mockDynamo{
		query: func(_ context.Context, _ *awsdynamodb.QueryInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
			return &awsdynamodb.QueryOutput{}, nil
		},
	}

	// "total" and "created_at" are in the ProjectionInclude list.
	_, err := bindOrders(mock).Index("by_status").Query(ctx, "shipped").
		Select("total", "created_at").
		Exec()
	if err != nil {
		t.Fatalf("unexpected error for projected attrs: %v", err)
	}
}

// ----- Where on table without SK -----

func TestQuery_WhereOnNonSKTable_ReturnsError(t *testing.T) {
	_, err := bindUsers(&mockDynamo{}).Query(ctx, "u1").Where(SKEQ("x")).Exec()
	if err == nil {
		t.Fatal("expected error when using Where on a table with no sort key")
	}
}

// ----- Index Scan -----

func TestIndexScan_SetsIndexName(t *testing.T) {
	var gotIndexName *string
	mock := &mockDynamo{
		scan: func(_ context.Context, in *awsdynamodb.ScanInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.ScanOutput, error) {
			gotIndexName = in.IndexName
			return &awsdynamodb.ScanOutput{}, nil
		},
	}

	_, err := bindOrders(mock).Index("by_status").Scan(ctx).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if gotIndexName == nil || *gotIndexName != "gsi-status-date" {
		t.Errorf("IndexName = %v; want gsi-status-date", gotIndexName)
	}
}

// ----- Index projection constructors -----

func TestProjectionAllAttrs(t *testing.T) {
	p := ProjectionAllAttrs()
	if p.Type != ProjectionAll {
		t.Errorf("Type = %v; want ProjectionAll", p.Type)
	}
}

func TestProjectionKeysOnlyAttrs(t *testing.T) {
	p := ProjectionKeysOnlyAttrs()
	if p.Type != ProjectionKeysOnly {
		t.Errorf("Type = %v; want ProjectionKeysOnly", p.Type)
	}
}
