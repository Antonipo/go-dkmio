package dynamodb

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ----- Bind -----

func TestBind_PanicsWithoutPK(t *testing.T) {
	type noPK struct{ Name string }
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for struct without pk tag")
		}
	}()
	Table[noPK]{Name: "test"}.Bind(newTestClient(&mockDynamo{}))
}

func TestBind_ResolvesGSIFromStructTags(t *testing.T) {
	orders := bindOrders(&mockDynamo{})
	if orders.schema.pkAttr != "user_id" {
		t.Errorf("pkAttr = %q; want user_id", orders.schema.pkAttr)
	}
	if orders.schema.skAttr != "order_id" {
		t.Errorf("skAttr = %q; want order_id", orders.schema.skAttr)
	}
	if pk := orders.schema.gsiPK["gsi-status-date"]; pk != "status" {
		t.Errorf("gsiPK[gsi-status-date] = %q; want status", pk)
	}
}

// ----- Get -----

func TestGet_ReturnsItem(t *testing.T) {
	want := testOrder{UserID: "u1", OrderID: "o1", Total: 42.5, Status: "pending"}
	mock := &mockDynamo{
		getItem: func(_ context.Context, in *awsdynamodb.GetItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.GetItemOutput, error) {
			if *in.TableName != "orders" {
				t.Errorf("TableName = %q; want orders", *in.TableName)
			}
			return &awsdynamodb.GetItemOutput{Item: marshalOrder(want)}, nil
		},
	}

	got, err := bindOrders(mock).Get(ctx, Keys{"user_id": "u1", "order_id": "o1"})
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("expected item, got nil")
	}
	if got.UserID != want.UserID || got.OrderID != want.OrderID {
		t.Errorf("got %+v; want %+v", *got, want)
	}
	if got.Total != want.Total {
		t.Errorf("Total = %v; want %v", got.Total, want.Total)
	}
}

func TestGet_ReturnsNilWhenNotFound(t *testing.T) {
	mock := &mockDynamo{
		getItem: func(_ context.Context, _ *awsdynamodb.GetItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.GetItemOutput, error) {
			return &awsdynamodb.GetItemOutput{Item: nil}, nil
		},
	}

	got, err := bindOrders(mock).Get(ctx, Keys{"user_id": "u1", "order_id": "o_missing"})
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("expected nil, got %+v", *got)
	}
}

func TestGet_MissingSK_ReturnsError(t *testing.T) {
	orders := bindOrders(&mockDynamo{})
	// orders has a sk — calling Get without the sk value must error.
	_, err := orders.Get(ctx, Keys{"user_id": "u1"}) // missing order_id
	if err == nil {
		t.Fatal("expected MissingKeyError, got nil")
	}
	var mke *MissingKeyError
	if !errors.As(err, &mke) {
		t.Errorf("expected MissingKeyError, got %T: %v", err, err)
	}
}

func TestGet_WithConsistentRead(t *testing.T) {
	var gotConsistent *bool
	mock := &mockDynamo{
		getItem: func(_ context.Context, in *awsdynamodb.GetItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.GetItemOutput, error) {
			gotConsistent = in.ConsistentRead
			return &awsdynamodb.GetItemOutput{Item: marshalOrder(testOrder{UserID: "u1", OrderID: "o1"})}, nil
		},
	}

	_, err := bindOrders(mock).Get(ctx, Keys{"user_id": "u1", "order_id": "o1"}, WithConsistentRead())
	if err != nil {
		t.Fatal(err)
	}
	if gotConsistent == nil || !*gotConsistent {
		t.Error("expected ConsistentRead=true")
	}
}

func TestGet_MapsConditionError(t *testing.T) {
	mock := &mockDynamo{
		getItem: func(_ context.Context, _ *awsdynamodb.GetItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.GetItemOutput, error) {
			return nil, &dbtypes.ResourceNotFoundException{}
		},
	}

	_, err := bindOrders(mock).Get(ctx, Keys{"user_id": "u1", "order_id": "o1"})
	var tne *TableNotFoundError
	if !errors.As(err, &tne) {
		t.Errorf("expected TableNotFoundError, got %T: %v", err, err)
	}
}

// ----- Put -----

func TestPut_SendsItem(t *testing.T) {
	var capturedItem map[string]dbtypes.AttributeValue
	mock := &mockDynamo{
		putItem: func(_ context.Context, in *awsdynamodb.PutItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.PutItemOutput, error) {
			capturedItem = in.Item
			return &awsdynamodb.PutItemOutput{}, nil
		},
	}

	item := testOrder{UserID: "u1", OrderID: "o1", Total: 99.9, Status: "pending"}
	if err := bindOrders(mock).Put(ctx, item); err != nil {
		t.Fatal(err)
	}

	if capturedItem == nil {
		t.Fatal("expected item to be sent, got nil")
	}
	// Verify PK was included.
	if _, ok := capturedItem["user_id"]; !ok {
		t.Error("user_id not in sent item")
	}
}

func TestPut_WithCondition_SetsExpression(t *testing.T) {
	var gotCondExpr *string
	mock := &mockDynamo{
		putItem: func(_ context.Context, in *awsdynamodb.PutItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.PutItemOutput, error) {
			gotCondExpr = in.ConditionExpression
			return &awsdynamodb.PutItemOutput{}, nil
		},
	}

	err := bindOrders(mock).Put(ctx,
		testOrder{UserID: "u1", OrderID: "o1"},
		WithCondition(NotExists("user_id")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if gotCondExpr == nil {
		t.Fatal("expected ConditionExpression, got nil")
	}
	if !strings.Contains(*gotCondExpr, "attribute_not_exists") {
		t.Errorf("ConditionExpression = %q; expected attribute_not_exists", *gotCondExpr)
	}
}

func TestPut_MapsConditionError(t *testing.T) {
	mock := &mockDynamo{
		putItem: func(_ context.Context, _ *awsdynamodb.PutItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.PutItemOutput, error) {
			return nil, &dbtypes.ConditionalCheckFailedException{}
		},
	}

	err := bindOrders(mock).Put(ctx, testOrder{UserID: "u1", OrderID: "o1"})
	var ce *ConditionError
	if !errors.As(err, &ce) {
		t.Errorf("expected ConditionError, got %T: %v", err, err)
	}
}

// ----- Update -----

func TestUpdate_BuildsUpdateExpression(t *testing.T) {
	var gotExpr *string
	mock := &mockDynamo{
		updateItem: func(_ context.Context, in *awsdynamodb.UpdateItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.UpdateItemOutput, error) {
			gotExpr = in.UpdateExpression
			return &awsdynamodb.UpdateItemOutput{}, nil
		},
	}

	_, err := bindOrders(mock).Update(ctx,
		Keys{"user_id": "u1", "order_id": "o1"},
		[]UpdateOp{Set(map[string]any{"status": "shipped"})},
	)
	if err != nil {
		t.Fatal(err)
	}
	if gotExpr == nil || !strings.Contains(*gotExpr, "SET") {
		t.Errorf("UpdateExpression = %v; expected SET clause", gotExpr)
	}
}

func TestUpdate_NoOps_ReturnsError(t *testing.T) {
	_, err := bindOrders(&mockDynamo{}).Update(ctx,
		Keys{"user_id": "u1", "order_id": "o1"},
		nil,
	)
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Errorf("expected ValidationError, got %T: %v", err, err)
	}
}

func TestUpdate_ReturnsUpdatedItem(t *testing.T) {
	updated := testOrder{UserID: "u1", OrderID: "o1", Status: "shipped"}
	mock := &mockDynamo{
		updateItem: func(_ context.Context, _ *awsdynamodb.UpdateItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.UpdateItemOutput, error) {
			return &awsdynamodb.UpdateItemOutput{Attributes: marshalOrder(updated)}, nil
		},
	}

	got, err := bindOrders(mock).Update(ctx,
		Keys{"user_id": "u1", "order_id": "o1"},
		[]UpdateOp{Set(map[string]any{"status": "shipped"})},
		ReturnAllNew(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Status != "shipped" {
		t.Errorf("expected Status=shipped, got %v", got)
	}
}

func TestUpdate_MultipleOps(t *testing.T) {
	var gotExpr *string
	mock := &mockDynamo{
		updateItem: func(_ context.Context, in *awsdynamodb.UpdateItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.UpdateItemOutput, error) {
			gotExpr = in.UpdateExpression
			return &awsdynamodb.UpdateItemOutput{}, nil
		},
	}

	_, err := bindOrders(mock).Update(ctx,
		Keys{"user_id": "u1", "order_id": "o1"},
		[]UpdateOp{
			Set(map[string]any{"status": "shipped"}),
			Add(map[string]any{"view_count": 1}),
			Remove("temp"),
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, keyword := range []string{"SET", "ADD", "REMOVE"} {
		if !strings.Contains(*gotExpr, keyword) {
			t.Errorf("expected %s in UpdateExpression %q", keyword, *gotExpr)
		}
	}
}

// ----- Delete -----

func TestDelete_SendsCorrectKey(t *testing.T) {
	var capturedKey map[string]dbtypes.AttributeValue
	mock := &mockDynamo{
		deleteItem: func(_ context.Context, in *awsdynamodb.DeleteItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.DeleteItemOutput, error) {
			capturedKey = in.Key
			return &awsdynamodb.DeleteItemOutput{}, nil
		},
	}

	if err := bindOrders(mock).Delete(ctx, Keys{"user_id": "u1", "order_id": "o1"}); err != nil {
		t.Fatal(err)
	}

	if capturedKey == nil {
		t.Fatal("expected key to be sent")
	}
	if _, ok := capturedKey["user_id"]; !ok {
		t.Error("user_id missing from delete key")
	}
	if _, ok := capturedKey["order_id"]; !ok {
		t.Error("order_id missing from delete key")
	}
}

func TestDelete_WithCondition(t *testing.T) {
	var gotCondExpr *string
	mock := &mockDynamo{
		deleteItem: func(_ context.Context, in *awsdynamodb.DeleteItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.DeleteItemOutput, error) {
			gotCondExpr = in.ConditionExpression
			return &awsdynamodb.DeleteItemOutput{}, nil
		},
	}

	err := bindOrders(mock).Delete(ctx,
		Keys{"user_id": "u1", "order_id": "o1"},
		WithCondition(EQ("status", "pending")),
	)
	if err != nil {
		t.Fatal(err)
	}
	if gotCondExpr == nil || !strings.Contains(*gotCondExpr, " = ") {
		t.Errorf("ConditionExpression = %v; expected EQ clause", gotCondExpr)
	}
}

// ----- Index -----

func TestIndex_PanicsOnUnknownAlias(t *testing.T) {
	orders := bindOrders(&mockDynamo{})
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for unknown index alias")
		}
	}()
	orders.Index("nonexistent")
}

func TestIndex_ReturnsIndexTable(t *testing.T) {
	orders := bindOrders(&mockDynamo{})
	ix := orders.Index("by_status")
	if ix == nil {
		t.Fatal("expected IndexTable, got nil")
	}
	if ix.pkAttr != "status" {
		t.Errorf("IndexTable pkAttr = %q; want status", ix.pkAttr)
	}
	if ix.skAttr != "created_at" {
		t.Errorf("IndexTable skAttr = %q; want created_at", ix.skAttr)
	}
}

// ----- TableClient interface compliance -----

func TestTableImplementsTableClient(t *testing.T) {
	// Compile-time check that *Table[T] implements TableClient[T].
	var _ TableClient[testOrder] = (*Table[testOrder])(nil)
	// If this compiles, the interface is satisfied.
}

// ----- Table must be bound before use -----

func TestMustBound_PanicsWhenUnbound(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for unbound table")
		}
	}()
	unbound := Table[testOrder]{Name: "orders"}
	_, _ = unbound.Get(ctx, Keys{"user_id": "u1", "order_id": "o1"})
}

// ----- ReturnValues -----

func TestPut_ReturnAllOld(t *testing.T) {
	var gotReturnValues dbtypes.ReturnValue
	mock := &mockDynamo{
		putItem: func(_ context.Context, in *awsdynamodb.PutItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.PutItemOutput, error) {
			gotReturnValues = in.ReturnValues
			return &awsdynamodb.PutItemOutput{}, nil
		},
	}

	err := bindOrders(mock).Put(ctx, testOrder{UserID: "u1", OrderID: "o1"}, ReturnAllOld())
	if err != nil {
		t.Fatal(err)
	}
	if gotReturnValues != dbtypes.ReturnValueAllOld {
		t.Errorf("ReturnValues = %v; want ALL_OLD", gotReturnValues)
	}
}

// ----- Key building -----

func TestBuildKey_NoPK_ReturnsError(t *testing.T) {
	users := bindUsers(&mockDynamo{})
	// users has no SK — passing a sk value should be silently ignored and succeed.
	key, err := users.buildKey("u1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := key["user_id"]; !ok {
		t.Error("user_id missing from key")
	}
}

// ----- Projection -----

func TestGet_WithProjection_SetsExpression(t *testing.T) {
	var gotProj *string
	mock := &mockDynamo{
		getItem: func(_ context.Context, in *awsdynamodb.GetItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.GetItemOutput, error) {
			gotProj = in.ProjectionExpression
			return &awsdynamodb.GetItemOutput{Item: marshalOrder(testOrder{UserID: "u1", OrderID: "o1"})}, nil
		},
	}

	_, err := bindOrders(mock).Get(ctx, Keys{"user_id": "u1", "order_id": "o1"}, WithProjection("total", "status"))
	if err != nil {
		t.Fatal(err)
	}
	if gotProj == nil {
		t.Fatal("expected ProjectionExpression, got nil")
	}
	// "status" is reserved so it must be escaped.
	if strings.Contains(*gotProj, "status") && !strings.Contains(*gotProj, "#n") {
		t.Errorf("reserved word 'status' should be escaped in ProjectionExpression: %q", *gotProj)
	}
}

// ----- Update return options -----

func TestUpdate_ReturnUpdatedNew(t *testing.T) {
	o := testOrder{UserID: "u1", OrderID: "o1", Status: "shipped"}
	mock := &mockDynamo{
		updateItem: func(_ context.Context, in *awsdynamodb.UpdateItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.UpdateItemOutput, error) {
			return &awsdynamodb.UpdateItemOutput{Attributes: marshalOrder(o)}, nil
		},
	}

	result, err := bindOrders(mock).Update(ctx,
		Keys{"user_id": "u1", "order_id": "o1"},
		[]UpdateOp{Set(map[string]any{"status": "shipped"})},
		ReturnUpdatedNew(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Status != "shipped" {
		t.Errorf("expected updated item, got %v", result)
	}
}

func TestUpdate_ReturnUpdatedOld(t *testing.T) {
	o := testOrder{UserID: "u1", OrderID: "o1", Status: "pending"}
	mock := &mockDynamo{
		updateItem: func(_ context.Context, in *awsdynamodb.UpdateItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.UpdateItemOutput, error) {
			return &awsdynamodb.UpdateItemOutput{Attributes: marshalOrder(o)}, nil
		},
	}

	result, err := bindOrders(mock).Update(ctx,
		Keys{"user_id": "u1", "order_id": "o1"},
		[]UpdateOp{Set(map[string]any{"status": "shipped"})},
		ReturnUpdatedOld(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil || result.Status != "pending" {
		t.Errorf("expected old item, got %v", result)
	}
}

// ----- Update operations: Append, DeleteSet -----

func TestUpdate_Append(t *testing.T) {
	var gotExpr *string
	mock := &mockDynamo{
		updateItem: func(_ context.Context, in *awsdynamodb.UpdateItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.UpdateItemOutput, error) {
			gotExpr = in.UpdateExpression
			return &awsdynamodb.UpdateItemOutput{}, nil
		},
	}

	_, err := bindOrders(mock).Update(ctx,
		Keys{"user_id": "u1", "order_id": "o1"},
		[]UpdateOp{Append(map[string]any{"events": []string{"shipped"}})},
	)
	if err != nil {
		t.Fatal(err)
	}
	if gotExpr == nil || !strings.Contains(*gotExpr, "list_append") {
		t.Errorf("UpdateExpression %v: want list_append", gotExpr)
	}
}

func TestUpdate_DeleteSet(t *testing.T) {
	var gotExpr *string
	mock := &mockDynamo{
		updateItem: func(_ context.Context, in *awsdynamodb.UpdateItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.UpdateItemOutput, error) {
			gotExpr = in.UpdateExpression
			return &awsdynamodb.UpdateItemOutput{}, nil
		},
	}

	_, err := bindOrders(mock).Update(ctx,
		Keys{"user_id": "u1", "order_id": "o1"},
		[]UpdateOp{DeleteSet(map[string]any{"tags": []string{"old"}})},
	)
	if err != nil {
		t.Fatal(err)
	}
	if gotExpr == nil || !strings.Contains(*gotExpr, "DELETE") {
		t.Errorf("UpdateExpression %v: want DELETE", gotExpr)
	}
}

// ----- buildKeyFromMap missing key -----

func TestGet_MissingPK_ReturnsMissingKeyError(t *testing.T) {
	_, err := bindOrders(&mockDynamo{}).Get(ctx, Keys{"order_id": "o1"}) // no user_id
	var mke *MissingKeyError
	if !errors.As(err, &mke) {
		t.Errorf("expected MissingKeyError, got %T: %v", err, err)
	}
	if mke.Attr != "user_id" {
		t.Errorf("Attr = %q; want user_id", mke.Attr)
	}
}

func TestGet_MissingSK_ReturnsMissingKeyError(t *testing.T) {
	_, err := bindOrders(&mockDynamo{}).Get(ctx, Keys{"user_id": "u1"}) // no order_id
	var mke *MissingKeyError
	if !errors.As(err, &mke) {
		t.Errorf("expected MissingKeyError, got %T: %v", err, err)
	}
	if mke.Attr != "order_id" {
		t.Errorf("Attr = %q; want order_id", mke.Attr)
	}
}

// ----- Index with undeclared alias -----

func TestIndex_UndeclaredAlias_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for undeclared index alias")
		}
	}()
	bindOrders(&mockDynamo{}).Index("nonexistent")
}

// ----- Index with explicit PKAttr/SKAttr overrides -----

func TestIndex_ExplicitAttrOverride(t *testing.T) {
	desc := Table[testOrder]{
		Name: "orders",
		Indexes: IndexMap{
			"custom": IndexDef{
				Name:   "custom-index",
				PKAttr: "custom_pk",
				SKAttr: "custom_sk",
			},
		},
	}
	idx := desc.Bind(newTestClient(&mockDynamo{})).Index("custom")
	if idx.pkAttr != "custom_pk" {
		t.Errorf("pkAttr = %q; want custom_pk", idx.pkAttr)
	}
	if idx.skAttr != "custom_sk" {
		t.Errorf("skAttr = %q; want custom_sk", idx.skAttr)
	}
}

// ----- ThrottlingError propagation -----

func TestGet_MapsThrottlingError(t *testing.T) {
	mock := &mockDynamo{
		getItem: func(_ context.Context, _ *awsdynamodb.GetItemInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.GetItemOutput, error) {
			return nil, &dbtypes.ProvisionedThroughputExceededException{Message: aws.String("throttled")}
		},
	}

	_, err := bindOrders(mock).Get(ctx, Keys{"user_id": "u1", "order_id": "o1"})
	var te *ThrottlingError
	if !errors.As(err, &te) {
		t.Errorf("expected ThrottlingError, got %T: %v", err, err)
	}
}
