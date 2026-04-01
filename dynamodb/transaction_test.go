package dynamodb

import (
	"context"
	"errors"
	"testing"

	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ----- WriteTransaction -----

func TestWriteTransaction_PutsItem(t *testing.T) {
	var capturedItems []dbtypes.TransactWriteItem
	mock := &mockDynamo{
		transactWrite: func(_ context.Context, in *awsdynamodb.TransactWriteItemsInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactWriteItemsOutput, error) {
			capturedItems = in.TransactItems
			return &awsdynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	client := newTestClient(mock)
	orders := testOrdersDesc.Bind(client)

	err := WriteTransaction(client, ctx, func(tx *TxWriter) error {
		return TxPut(tx, orders, testOrder{UserID: "u1", OrderID: "o1", Total: 50})
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(capturedItems) != 1 {
		t.Fatalf("expected 1 transact item, got %d", len(capturedItems))
	}
	if capturedItems[0].Put == nil {
		t.Error("expected Put operation")
	}
	if *capturedItems[0].Put.TableName != "orders" {
		t.Errorf("TableName = %q; want orders", *capturedItems[0].Put.TableName)
	}
}

func TestWriteTransaction_UpdatesItem(t *testing.T) {
	var capturedItems []dbtypes.TransactWriteItem
	mock := &mockDynamo{
		transactWrite: func(_ context.Context, in *awsdynamodb.TransactWriteItemsInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactWriteItemsOutput, error) {
			capturedItems = in.TransactItems
			return &awsdynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	client := newTestClient(mock)
	orders := testOrdersDesc.Bind(client)

	err := WriteTransaction(client, ctx, func(tx *TxWriter) error {
		return TxUpdate(tx, orders,
			Keys{"user_id": "u1", "order_id": "o1"},
			[]UpdateOp{Set(map[string]any{"status": "shipped"})},
		)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(capturedItems) != 1 {
		t.Fatalf("expected 1 transact item, got %d", len(capturedItems))
	}
	if capturedItems[0].Update == nil {
		t.Error("expected Update operation")
	}
}

func TestWriteTransaction_DeletesItem(t *testing.T) {
	var capturedItems []dbtypes.TransactWriteItem
	mock := &mockDynamo{
		transactWrite: func(_ context.Context, in *awsdynamodb.TransactWriteItemsInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactWriteItemsOutput, error) {
			capturedItems = in.TransactItems
			return &awsdynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	client := newTestClient(mock)
	orders := testOrdersDesc.Bind(client)

	err := WriteTransaction(client, ctx, func(tx *TxWriter) error {
		return TxDelete(tx, orders, "u1", "o1")
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(capturedItems) != 1 {
		t.Fatalf("expected 1 transact item, got %d", len(capturedItems))
	}
	if capturedItems[0].Delete == nil {
		t.Error("expected Delete operation")
	}
}

func TestWriteTransaction_ConditionCheck(t *testing.T) {
	var capturedItems []dbtypes.TransactWriteItem
	mock := &mockDynamo{
		transactWrite: func(_ context.Context, in *awsdynamodb.TransactWriteItemsInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactWriteItemsOutput, error) {
			capturedItems = in.TransactItems
			return &awsdynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	client := newTestClient(mock)
	orders := testOrdersDesc.Bind(client)

	err := WriteTransaction(client, ctx, func(tx *TxWriter) error {
		return TxConditionCheck(tx, orders, "u1", []any{"o1"}, EQ("status", "pending"))
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(capturedItems) != 1 {
		t.Fatalf("expected 1 transact item, got %d", len(capturedItems))
	}
	if capturedItems[0].ConditionCheck == nil {
		t.Error("expected ConditionCheck operation")
	}
}

func TestWriteTransaction_MixedOps(t *testing.T) {
	var capturedItems []dbtypes.TransactWriteItem
	mock := &mockDynamo{
		transactWrite: func(_ context.Context, in *awsdynamodb.TransactWriteItemsInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactWriteItemsOutput, error) {
			capturedItems = in.TransactItems
			return &awsdynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	client := newTestClient(mock)
	orders := testOrdersDesc.Bind(client)
	users := testUsersDesc.Bind(client)

	err := WriteTransaction(client, ctx, func(tx *TxWriter) error {
		if err := TxPut(tx, orders, testOrder{UserID: "u1", OrderID: "o1"}); err != nil {
			return err
		}
		if err := TxUpdate(tx, users,
			Keys{"user_id": "u1"},
			[]UpdateOp{Add(map[string]any{"order_count": 1})},
		); err != nil {
			return err
		}
		return TxDelete(tx, orders, "u1", "o_old")
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(capturedItems) != 3 {
		t.Errorf("expected 3 transact items, got %d", len(capturedItems))
	}
}

func TestWriteTransaction_EmptyFn_DoesNotCallAPI(t *testing.T) {
	called := false
	mock := &mockDynamo{
		transactWrite: func(_ context.Context, _ *awsdynamodb.TransactWriteItemsInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactWriteItemsOutput, error) {
			called = true
			return &awsdynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	client := newTestClient(mock)
	err := WriteTransaction(client, ctx, func(tx *TxWriter) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Error("expected TransactWriteItems to not be called for empty transaction")
	}
}

func TestWriteTransaction_FnError_Aborts(t *testing.T) {
	called := false
	mock := &mockDynamo{
		transactWrite: func(_ context.Context, _ *awsdynamodb.TransactWriteItemsInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactWriteItemsOutput, error) {
			called = true
			return &awsdynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	client := newTestClient(mock)
	wantErr := errors.New("validation failed")
	err := WriteTransaction(client, ctx, func(tx *TxWriter) error {
		return wantErr
	})
	if err != wantErr {
		t.Errorf("expected wantErr, got %v", err)
	}
	if called {
		t.Error("TransactWriteItems should not be called when fn returns error")
	}
}

func TestWriteTransaction_WithCondition(t *testing.T) {
	var capturedItems []dbtypes.TransactWriteItem
	mock := &mockDynamo{
		transactWrite: func(_ context.Context, in *awsdynamodb.TransactWriteItemsInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactWriteItemsOutput, error) {
			capturedItems = in.TransactItems
			return &awsdynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	client := newTestClient(mock)
	orders := testOrdersDesc.Bind(client)

	err := WriteTransaction(client, ctx, func(tx *TxWriter) error {
		return TxPut(tx, orders, testOrder{UserID: "u1", OrderID: "o1"},
			WithCondition(NotExists("user_id")),
		)
	})
	if err != nil {
		t.Fatal(err)
	}
	put := capturedItems[0].Put
	if put.ConditionExpression == nil {
		t.Error("expected ConditionExpression to be set")
	}
}

// ----- ReadTransaction -----

func TestReadTransaction_GetsItems(t *testing.T) {
	o := testOrder{UserID: "u1", OrderID: "o1", Total: 99}
	u := testUser{UserID: "u1", Name: "Alice"}

	mock := &mockDynamo{
		transactGet: func(_ context.Context, in *awsdynamodb.TransactGetItemsInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactGetItemsOutput, error) {
			return &awsdynamodb.TransactGetItemsOutput{
				Responses: []dbtypes.ItemResponse{
					{Item: marshalOrder(o)},
					{Item: marshalUser(u)},
				},
			}, nil
		},
	}

	client := newTestClient(mock)
	orders := testOrdersDesc.Bind(client)
	users := testUsersDesc.Bind(client)

	result, err := ReadTransaction(client, ctx, func(tx *TxReader) error {
		if _, err := TxGetTyped(tx, orders, "u1", "o1"); err != nil {
			return err
		}
		if _, err := TxGetTyped(tx, users, "u1"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	gotOrder, err := TxGetResult[testOrder](result, 0)
	if err != nil {
		t.Fatal(err)
	}
	if gotOrder == nil || gotOrder.OrderID != "o1" {
		t.Errorf("order = %v; want o1", gotOrder)
	}

	gotUser, err := TxGetResult[testUser](result, 1)
	if err != nil {
		t.Fatal(err)
	}
	if gotUser == nil || gotUser.Name != "Alice" {
		t.Errorf("user = %v; want Alice", gotUser)
	}
}

func TestReadTransaction_MissingItem_ReturnsNil(t *testing.T) {
	mock := &mockDynamo{
		transactGet: func(_ context.Context, _ *awsdynamodb.TransactGetItemsInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactGetItemsOutput, error) {
			return &awsdynamodb.TransactGetItemsOutput{
				Responses: []dbtypes.ItemResponse{
					{Item: nil}, // item not found
				},
			}, nil
		},
	}

	client := newTestClient(mock)
	orders := testOrdersDesc.Bind(client)

	result, err := ReadTransaction(client, ctx, func(tx *TxReader) error {
		_, err := TxGetTyped(tx, orders, "u1", "missing")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := TxGetResult[testOrder](result, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("expected nil for missing item, got %v", got)
	}
}

func TestReadTransaction_Empty_DoesNotCallAPI(t *testing.T) {
	called := false
	mock := &mockDynamo{
		transactGet: func(_ context.Context, _ *awsdynamodb.TransactGetItemsInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactGetItemsOutput, error) {
			called = true
			return &awsdynamodb.TransactGetItemsOutput{}, nil
		},
	}

	client := newTestClient(mock)
	result, err := ReadTransaction(client, ctx, func(tx *TxReader) error {
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Error("expected TransactGetItems to not be called for empty transaction")
	}
	if result == nil {
		t.Error("expected non-nil result")
	}
}

func TestReadTransaction_FnError_Aborts(t *testing.T) {
	called := false
	mock := &mockDynamo{
		transactGet: func(_ context.Context, _ *awsdynamodb.TransactGetItemsInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactGetItemsOutput, error) {
			called = true
			return &awsdynamodb.TransactGetItemsOutput{}, nil
		},
	}

	client := newTestClient(mock)
	wantErr := errors.New("setup failed")
	_, err := ReadTransaction(client, ctx, func(tx *TxReader) error {
		return wantErr
	})
	if err != wantErr {
		t.Errorf("expected wantErr, got %v", err)
	}
	if called {
		t.Error("TransactGetItems should not be called when fn returns error")
	}
}

// ----- TxWriter error paths -----

func TestWriteTransaction_UpdateEmptyOps_ReturnsError(t *testing.T) {
	client := newTestClient(&mockDynamo{})
	orders := testOrdersDesc.Bind(client)

	err := WriteTransaction(client, ctx, func(tx *TxWriter) error {
		avKeys, _ := marshalMap(map[string]any{"user_id": "u1", "order_id": "o1"})
		return tx.Update(orders.Name, avKeys, nil)
	})
	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Errorf("expected ValidationError for empty ops, got %T: %v", err, err)
	}
}

func TestWriteTransaction_DeleteWithCondition(t *testing.T) {
	var capturedItems []dbtypes.TransactWriteItem
	mock := &mockDynamo{
		transactWrite: func(_ context.Context, in *awsdynamodb.TransactWriteItemsInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactWriteItemsOutput, error) {
			capturedItems = in.TransactItems
			return &awsdynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	client := newTestClient(mock)
	orders := testOrdersDesc.Bind(client)

	err := WriteTransaction(client, ctx, func(tx *TxWriter) error {
		key, err := marshalMap(map[string]any{"user_id": "u1", "order_id": "o1"})
		if err != nil {
			return err
		}
		tx.Delete(orders.Name, key, WithCondition(EQ("status", "pending")))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	del := capturedItems[0].Delete
	if del == nil {
		t.Fatal("expected Delete operation")
	}
	if del.ConditionExpression == nil {
		t.Error("expected ConditionExpression on Delete")
	}
}

func TestWriteTransaction_UpdateWithCondition(t *testing.T) {
	var capturedItems []dbtypes.TransactWriteItem
	mock := &mockDynamo{
		transactWrite: func(_ context.Context, in *awsdynamodb.TransactWriteItemsInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactWriteItemsOutput, error) {
			capturedItems = in.TransactItems
			return &awsdynamodb.TransactWriteItemsOutput{}, nil
		},
	}

	client := newTestClient(mock)
	orders := testOrdersDesc.Bind(client)

	err := WriteTransaction(client, ctx, func(tx *TxWriter) error {
		return TxUpdate(tx, orders,
			Keys{"user_id": "u1", "order_id": "o1"},
			[]UpdateOp{Set(map[string]any{"status": "shipped"})},
			WithCondition(EQ("status", "pending")),
		)
	})
	if err != nil {
		t.Fatal(err)
	}
	upd := capturedItems[0].Update
	if upd == nil {
		t.Fatal("expected Update operation")
	}
	if upd.ConditionExpression == nil {
		t.Error("expected ConditionExpression on Update")
	}
}

// ----- TxReader.Get with projection -----

func TestReadTransaction_GetWithProjection(t *testing.T) {
	var gotGet *dbtypes.Get
	mock := &mockDynamo{
		transactGet: func(_ context.Context, in *awsdynamodb.TransactGetItemsInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactGetItemsOutput, error) {
			if len(in.TransactItems) > 0 {
				gotGet = in.TransactItems[0].Get
			}
			return &awsdynamodb.TransactGetItemsOutput{
				Responses: []dbtypes.ItemResponse{{Item: nil}},
			}, nil
		},
	}

	client := newTestClient(mock)
	orders := testOrdersDesc.Bind(client)

	_, err := ReadTransaction(client, ctx, func(tx *TxReader) error {
		key, err := marshalMap(map[string]any{"user_id": "u1", "order_id": "o1"})
		if err != nil {
			return err
		}
		tx.Get(orders.Name, key, WithProjection("total", "status"))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotGet == nil || gotGet.ProjectionExpression == nil {
		t.Error("expected ProjectionExpression to be set on TxReader.Get")
	}
}

func TestReadTransaction_OutOfBoundsRaw_ReturnsNil(t *testing.T) {
	mock := &mockDynamo{
		transactGet: func(_ context.Context, _ *awsdynamodb.TransactGetItemsInput, _ ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactGetItemsOutput, error) {
			return &awsdynamodb.TransactGetItemsOutput{
				Responses: []dbtypes.ItemResponse{},
			}, nil
		},
	}

	client := newTestClient(mock)
	orders := testOrdersDesc.Bind(client)

	result, err := ReadTransaction(client, ctx, func(tx *TxReader) error {
		_, err := TxGetTyped(tx, orders, "u1", "o1")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	// Access out-of-bounds index.
	got, err := TxGetResult[testOrder](result, 5)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Errorf("expected nil for out-of-bounds index, got %v", got)
	}
}
