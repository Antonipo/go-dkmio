package dynamodb

// mockDynamo is a test double for the dynamoSvc interface.
// Each field is a function that replaces the corresponding AWS call.
// If a field is nil the test will panic, signalling an unexpected call.

import (
	"context"
	"fmt"

	awsdynamodb "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type mockDynamo struct {
	getItem          func(ctx context.Context, input *awsdynamodb.GetItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.GetItemOutput, error)
	putItem          func(ctx context.Context, input *awsdynamodb.PutItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.PutItemOutput, error)
	updateItem       func(ctx context.Context, input *awsdynamodb.UpdateItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.UpdateItemOutput, error)
	deleteItem       func(ctx context.Context, input *awsdynamodb.DeleteItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.DeleteItemOutput, error)
	query            func(ctx context.Context, input *awsdynamodb.QueryInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error)
	scan             func(ctx context.Context, input *awsdynamodb.ScanInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.ScanOutput, error)
	batchGetItem     func(ctx context.Context, input *awsdynamodb.BatchGetItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchGetItemOutput, error)
	batchWriteItem   func(ctx context.Context, input *awsdynamodb.BatchWriteItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchWriteItemOutput, error)
	transactWrite    func(ctx context.Context, input *awsdynamodb.TransactWriteItemsInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactWriteItemsOutput, error)
	transactGet      func(ctx context.Context, input *awsdynamodb.TransactGetItemsInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactGetItemsOutput, error)
}

func (m *mockDynamo) GetItem(ctx context.Context, in *awsdynamodb.GetItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.GetItemOutput, error) {
	if m.getItem == nil {
		panic(fmt.Sprintf("unexpected call to GetItem on table %s", *in.TableName))
	}
	return m.getItem(ctx, in, opts...)
}
func (m *mockDynamo) PutItem(ctx context.Context, in *awsdynamodb.PutItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.PutItemOutput, error) {
	if m.putItem == nil {
		panic(fmt.Sprintf("unexpected call to PutItem on table %s", *in.TableName))
	}
	return m.putItem(ctx, in, opts...)
}
func (m *mockDynamo) UpdateItem(ctx context.Context, in *awsdynamodb.UpdateItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.UpdateItemOutput, error) {
	if m.updateItem == nil {
		panic(fmt.Sprintf("unexpected call to UpdateItem on table %s", *in.TableName))
	}
	return m.updateItem(ctx, in, opts...)
}
func (m *mockDynamo) DeleteItem(ctx context.Context, in *awsdynamodb.DeleteItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.DeleteItemOutput, error) {
	if m.deleteItem == nil {
		panic(fmt.Sprintf("unexpected call to DeleteItem on table %s", *in.TableName))
	}
	return m.deleteItem(ctx, in, opts...)
}
func (m *mockDynamo) Query(ctx context.Context, in *awsdynamodb.QueryInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.QueryOutput, error) {
	if m.query == nil {
		panic(fmt.Sprintf("unexpected call to Query on table %s", *in.TableName))
	}
	return m.query(ctx, in, opts...)
}
func (m *mockDynamo) Scan(ctx context.Context, in *awsdynamodb.ScanInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.ScanOutput, error) {
	if m.scan == nil {
		panic(fmt.Sprintf("unexpected call to Scan on table %s", *in.TableName))
	}
	return m.scan(ctx, in, opts...)
}
func (m *mockDynamo) BatchGetItem(ctx context.Context, in *awsdynamodb.BatchGetItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchGetItemOutput, error) {
	if m.batchGetItem == nil {
		panic("unexpected call to BatchGetItem")
	}
	return m.batchGetItem(ctx, in, opts...)
}
func (m *mockDynamo) BatchWriteItem(ctx context.Context, in *awsdynamodb.BatchWriteItemInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.BatchWriteItemOutput, error) {
	if m.batchWriteItem == nil {
		panic("unexpected call to BatchWriteItem")
	}
	return m.batchWriteItem(ctx, in, opts...)
}
func (m *mockDynamo) TransactWriteItems(ctx context.Context, in *awsdynamodb.TransactWriteItemsInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactWriteItemsOutput, error) {
	if m.transactWrite == nil {
		panic("unexpected call to TransactWriteItems")
	}
	return m.transactWrite(ctx, in, opts...)
}
func (m *mockDynamo) TransactGetItems(ctx context.Context, in *awsdynamodb.TransactGetItemsInput, opts ...func(*awsdynamodb.Options)) (*awsdynamodb.TransactGetItemsOutput, error) {
	if m.transactGet == nil {
		panic("unexpected call to TransactGetItems")
	}
	return m.transactGet(ctx, in, opts...)
}
