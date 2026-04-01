package dynamodb

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/Antonipo/go-dkmio/internal/expr"
)

const maxTransactionOps = 100 // DynamoDB TransactWriteItems / TransactGetItems limit

// ----- WriteTransaction -----

// TxWriter accumulates transactional write operations.
// Use WriteTransaction to create one.
type TxWriter struct {
	items []dbtypes.TransactWriteItem
}

// WriteTransaction executes a set of write operations atomically.
// fn receives a *TxWriter where operations are queued; all are committed
// when fn returns without error, or the transaction is aborted on error.
//
//	err := dynamodb.WriteTransaction(dynamo, ctx, func(tx *dynamodb.TxWriter) error {
//	    tx.Put(orders, order)
//	    tx.Update(inventory, dynamodb.Keys{...}, []dynamodb.UpdateOp{...})
//	    tx.Delete(carts, "u1")
//	    return nil
//	})
func WriteTransaction(client *Client, ctx context.Context, fn func(tx *TxWriter) error) error {
	tx := &TxWriter{}
	if err := fn(tx); err != nil {
		return err
	}
	if len(tx.items) == 0 {
		return nil
	}
	if len(tx.items) > maxTransactionOps {
		return &ValidationError{msg: fmt.Sprintf("transaction exceeds maximum of %d operations", maxTransactionOps)}
	}

	input := &dynamodb.TransactWriteItemsInput{TransactItems: tx.items}

	return client.execute(ctx, func() error {
		_, e := client.service().TransactWriteItems(ctx, input)
		mapped, _ := mapError(e)
		return mapped
	})
}

// Put queues a PutItem in the transaction.
func (tx *TxWriter) Put(tableName string, item map[string]dbtypes.AttributeValue, opts ...WriteOption) {
	cfg := applyWriteOptions(opts)
	put := &dbtypes.Put{
		TableName: aws.String(tableName),
		Item:      item,
	}
	if len(cfg.conditions) > 0 {
		b := expr.NewBuilder()
		put.ConditionExpression = aws.String(buildFilter(b, cfg.conditions))
		put.ExpressionAttributeNames = b.Names()
		if len(b.Values()) > 0 {
			put.ExpressionAttributeValues = marshalValues(b.Values())
		}
	}
	tx.items = append(tx.items, dbtypes.TransactWriteItem{Put: put})
}

// PutItem is a helper that marshals a typed item and queues it as a Put.
func TxPut[T any](tx *TxWriter, t *Table[T], item T, opts ...WriteOption) error {
	av, err := marshalItem(item)
	if err != nil {
		return &ValidationError{cause: err}
	}
	tx.Put(t.Name, av, opts...)
	return nil
}

// Update queues an UpdateItem in the transaction.
func (tx *TxWriter) Update(tableName string, keys map[string]dbtypes.AttributeValue, ops []UpdateOp, opts ...WriteOption) error {
	cfg := applyWriteOptions(opts)
	if len(ops) == 0 {
		return &ValidationError{msg: "at least one update operation is required"}
	}

	b := expr.NewBuilder()
	updateExpr := buildUpdateExpression(b, ops)

	update := &dbtypes.Update{
		TableName:                aws.String(tableName),
		Key:                      keys,
		UpdateExpression:         aws.String(updateExpr),
		ExpressionAttributeNames: b.Names(),
	}
	if len(b.Values()) > 0 {
		update.ExpressionAttributeValues = marshalValues(b.Values())
	}
	if len(cfg.conditions) > 0 {
		update.ConditionExpression = aws.String(buildFilter(b, cfg.conditions))
		update.ExpressionAttributeNames = b.Names()
		if len(b.Values()) > 0 {
			update.ExpressionAttributeValues = marshalValues(b.Values())
		}
	}

	tx.items = append(tx.items, dbtypes.TransactWriteItem{Update: update})
	return nil
}

// TxUpdate is a helper that converts typed Keys and queues an Update.
func TxUpdate[T any](tx *TxWriter, t *Table[T], keys Keys, ops []UpdateOp, opts ...WriteOption) error {
	avKeys, err := marshalMap(map[string]any(keys))
	if err != nil {
		return &ValidationError{cause: err}
	}
	return tx.Update(t.Name, avKeys, ops, opts...)
}

// Delete queues a DeleteItem in the transaction.
func (tx *TxWriter) Delete(tableName string, keys map[string]dbtypes.AttributeValue, opts ...WriteOption) {
	cfg := applyWriteOptions(opts)
	del := &dbtypes.Delete{
		TableName: aws.String(tableName),
		Key:       keys,
	}
	if len(cfg.conditions) > 0 {
		b := expr.NewBuilder()
		del.ConditionExpression = aws.String(buildFilter(b, cfg.conditions))
		del.ExpressionAttributeNames = b.Names()
		if len(b.Values()) > 0 {
			del.ExpressionAttributeValues = marshalValues(b.Values())
		}
	}
	tx.items = append(tx.items, dbtypes.TransactWriteItem{Delete: del})
}

// TxDelete is a helper that resolves typed keys and queues a Delete.
func TxDelete[T any](tx *TxWriter, t *Table[T], pkVal any, skVals ...any) error {
	key, err := t.buildKey(pkVal, skVals)
	if err != nil {
		return err
	}
	tx.Delete(t.Name, key)
	return nil
}

// ConditionCheck queues a ConditionCheck in the transaction.
// The operation succeeds only if the condition holds — no item is modified.
func (tx *TxWriter) ConditionCheck(tableName string, keys map[string]dbtypes.AttributeValue, conds ...Condition) {
	b := expr.NewBuilder()
	cc := &dbtypes.ConditionCheck{
		TableName:                aws.String(tableName),
		Key:                      keys,
		ConditionExpression:      aws.String(buildFilter(b, conds)),
		ExpressionAttributeNames: b.Names(),
	}
	if len(b.Values()) > 0 {
		cc.ExpressionAttributeValues = marshalValues(b.Values())
	}
	tx.items = append(tx.items, dbtypes.TransactWriteItem{ConditionCheck: cc})
}

// TxConditionCheck is a helper that resolves typed keys and queues a ConditionCheck.
func TxConditionCheck[T any](tx *TxWriter, t *Table[T], pkVal any, skVals []any, conds ...Condition) error {
	key, err := t.buildKey(pkVal, skVals)
	if err != nil {
		return err
	}
	tx.ConditionCheck(t.Name, key, conds...)
	return nil
}

// ----- ReadTransaction -----

// TxReader accumulates transactional get operations.
// Use ReadTransaction to create one.
type TxReader struct {
	items   []dbtypes.TransactGetItem
	results []any // pre-allocated slots for decoded results
}

// ReadTransactionResult holds the results of a ReadTransaction.
type ReadTransactionResult struct {
	responses []map[string]dbtypes.AttributeValue
}

// Get returns the item at position idx as a raw attribute map.
// Use TxGetResult to decode into a typed struct.
func (r *ReadTransactionResult) Raw(idx int) map[string]dbtypes.AttributeValue {
	if idx < 0 || idx >= len(r.responses) {
		return nil
	}
	return r.responses[idx]
}

// ReadTransaction executes a set of get operations atomically.
// Returns a result object from which individual items can be extracted.
//
//	result, err := dynamodb.ReadTransaction(dynamo, ctx, func(tx *dynamodb.TxReader) error {
//	    tx.Get(orders, "u1", "o1")
//	    tx.Get(users, "u1")
//	    return nil
//	})
//	order, err := dynamodb.TxGetResult[Order](result, 0)
//	user,  err := dynamodb.TxGetResult[User](result, 1)
func ReadTransaction(client *Client, ctx context.Context, fn func(tx *TxReader) error) (*ReadTransactionResult, error) {
	tx := &TxReader{}
	if err := fn(tx); err != nil {
		return nil, err
	}
	if len(tx.items) == 0 {
		return &ReadTransactionResult{}, nil
	}
	if len(tx.items) > maxTransactionOps {
		return nil, &ValidationError{msg: fmt.Sprintf("transaction exceeds maximum of %d operations", maxTransactionOps)}
	}

	input := &dynamodb.TransactGetItemsInput{TransactItems: tx.items}

	var out *dynamodb.TransactGetItemsOutput
	err := client.execute(ctx, func() error {
		var e error
		out, e = client.service().TransactGetItems(ctx, input)
		mapped, _ := mapError(e)
		return mapped
	})
	if err != nil {
		return nil, err
	}

	responses := make([]map[string]dbtypes.AttributeValue, len(out.Responses))
	for i, resp := range out.Responses {
		responses[i] = resp.Item
	}
	return &ReadTransactionResult{responses: responses}, nil
}

// TxGet queues a GetItem in the read transaction.
// Returns the index of this get in the result (for use with TxGetResult).
func (tx *TxReader) Get(tableName string, keys map[string]dbtypes.AttributeValue, opts ...GetOption) int {
	cfg := applyGetOptions(opts)
	get := &dbtypes.Get{
		TableName: aws.String(tableName),
		Key:       keys,
	}
	if cfg.consistent {
		// TransactGetItems doesn't support consistent reads per-item,
		// but we capture the intent for future API compat.
	}
	if len(cfg.projection) > 0 {
		// Simplified projection — full escaping not applied here.
		proj := ""
		for i, a := range cfg.projection {
			if i > 0 {
				proj += ", "
			}
			proj += a
		}
		get.ProjectionExpression = aws.String(proj)
	}
	idx := len(tx.items)
	tx.items = append(tx.items, dbtypes.TransactGetItem{Get: get})
	return idx
}

// TxGetTyped is a helper that resolves typed keys and queues a Get.
// Returns the slot index.
func TxGetTyped[T any](tx *TxReader, t *Table[T], pkVal any, skVals ...any) (int, error) {
	key, err := t.buildKey(pkVal, skVals)
	if err != nil {
		return 0, err
	}
	idx := tx.Get(t.Name, key)
	return idx, nil
}

// TxGetResult decodes the item at position idx into type T.
// Returns (nil, nil) if the item was not found.
func TxGetResult[T any](r *ReadTransactionResult, idx int) (*T, error) {
	raw := r.Raw(idx)
	if len(raw) == 0 {
		return nil, nil
	}
	return unmarshalItem[T](raw)
}
