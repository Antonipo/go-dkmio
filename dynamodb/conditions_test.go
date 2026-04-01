package dynamodb

import (
	"context"
	"strings"
	"testing"

	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// filterCapture returns a mock that captures the FilterExpression from a Query.
func filterCapture(out *string) *mockDynamo {
	return &mockDynamo{
		query: func(_ context.Context, in *awsdynamodb.QueryInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
			if in.FilterExpression != nil {
				*out = *in.FilterExpression
			}
			return &awsdynamodb.QueryOutput{}, nil
		},
	}
}

// kceCapture returns a mock that captures the KeyConditionExpression from a Query.
func kceCapture(out *string) *mockDynamo {
	return &mockDynamo{
		query: func(_ context.Context, in *awsdynamodb.QueryInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
			if in.KeyConditionExpression != nil {
				*out = *in.KeyConditionExpression
			}
			return &awsdynamodb.QueryOutput{}, nil
		},
	}
}

// ----- filter condition constructors -----

func TestCondition_NEQ(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(NEQ("status", "cancelled")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if got == "" {
		t.Error("expected FilterExpression to be set")
	}
}

func TestCondition_GT(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(GT("total", 100)).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, ">") {
		t.Errorf("FilterExpression %q: want '>'", got)
	}
}

func TestCondition_LT(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(LT("total", 50)).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "<") {
		t.Errorf("FilterExpression %q: want '<'", got)
	}
}

func TestCondition_LTE(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(LTE("total", 50)).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "<=") {
		t.Errorf("FilterExpression %q: want '<='", got)
	}
}

func TestCondition_Between(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(Between("total", 10, 100)).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(strings.ToLower(got), "between") {
		t.Errorf("FilterExpression %q: want BETWEEN", got)
	}
}

func TestCondition_BeginsWith(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(BeginsWith("status", "ship")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "begins_with") {
		t.Errorf("FilterExpression %q: want begins_with", got)
	}
}

func TestCondition_Contains(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(Contains("status", "ship")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "contains") {
		t.Errorf("FilterExpression %q: want contains", got)
	}
}

func TestCondition_NotContains(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(NotContains("status", "cancel")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if got == "" {
		t.Error("expected FilterExpression to be set")
	}
}

func TestCondition_NotBeginsWith(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(NotBeginsWith("status", "x")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if got == "" {
		t.Error("expected FilterExpression to be set")
	}
}

func TestCondition_Exists(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(Exists("status")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "attribute_exists") {
		t.Errorf("FilterExpression %q: want attribute_exists", got)
	}
}

func TestCondition_AttrType(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(AttrType("total", "N")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "attribute_type") {
		t.Errorf("FilterExpression %q: want attribute_type", got)
	}
}

func TestCondition_In(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(In("status", "shipped", "delivered")).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if got == "" {
		t.Error("expected FilterExpression to be set")
	}
}

func TestCondition_SizeEQ(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(SizeEQ("status", 5)).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "size") {
		t.Errorf("FilterExpression %q: want size", got)
	}
}

func TestCondition_SizeGT(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(SizeGT("status", 3)).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if got == "" {
		t.Error("expected FilterExpression to be set")
	}
}

func TestCondition_SizeGTE(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(SizeGTE("status", 3)).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if got == "" {
		t.Error("expected FilterExpression to be set")
	}
}

func TestCondition_SizeLT(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(SizeLT("status", 10)).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if got == "" {
		t.Error("expected FilterExpression to be set")
	}
}

func TestCondition_SizeLTE(t *testing.T) {
	var got string
	_, err := bindOrders(filterCapture(&got)).Query(ctx, "u1").Filter(SizeLTE("status", 10)).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if got == "" {
		t.Error("expected FilterExpression to be set")
	}
}

// ----- SK condition constructors -----

func queryWithSK(t *testing.T, sk SKCondition) string {
	t.Helper()
	var got string
	_, err := bindOrders(kceCapture(&got)).Query(ctx, "u1").Where(sk).Exec()
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestSKCondition_SKGT(t *testing.T) {
	kce := queryWithSK(t, SKGT("o_100"))
	if !strings.Contains(kce, ">") {
		t.Errorf("KeyConditionExpression %q: want '>'", kce)
	}
}

func TestSKCondition_SKLT(t *testing.T) {
	kce := queryWithSK(t, SKLT("o_100"))
	if !strings.Contains(kce, "<") {
		t.Errorf("KeyConditionExpression %q: want '<'", kce)
	}
}

func TestSKCondition_SKLTE(t *testing.T) {
	kce := queryWithSK(t, SKLTE("o_100"))
	if !strings.Contains(kce, "<=") {
		t.Errorf("KeyConditionExpression %q: want '<='", kce)
	}
}

func TestSKCondition_SKBetween(t *testing.T) {
	kce := queryWithSK(t, SKBetween("o_001", "o_999"))
	if !strings.Contains(strings.ToLower(kce), "between") {
		t.Errorf("KeyConditionExpression %q: want BETWEEN", kce)
	}
}

func TestSKCondition_SKBeginsWith(t *testing.T) {
	kce := queryWithSK(t, SKBeginsWith("o_"))
	if !strings.Contains(kce, "begins_with") {
		t.Errorf("KeyConditionExpression %q: want begins_with", kce)
	}
}

// ----- Scan with filter -----

func TestScan_WithItems(t *testing.T) {
	o := testOrder{UserID: "u1", OrderID: "o1"}
	mock := &mockDynamo{
		scan: func(_ context.Context, _ *awsdynamodb.ScanInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.ScanOutput, error) {
			return &awsdynamodb.ScanOutput{
				Items: []map[string]dbtypes.AttributeValue{marshalOrder(o)},
				Count: 1,
			}, nil
		},
	}
	result, err := bindOrders(mock).Scan(ctx).Exec()
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 {
		t.Errorf("expected 1 item, got %d", len(result.Items))
	}
}
