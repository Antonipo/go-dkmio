package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/Antonipo/go-dkmio/internal/expr"
	"github.com/Antonipo/go-dkmio/internal/serial"
)

// TableClient is the interface that Table[T] satisfies.
// Expose this interface in your own code so operations can be mocked in tests.
//
//	type OrderRepo struct {
//	    table dynamodb.TableClient[Order]
//	}
//
// Keys must contain the partition key and, for tables with a sort key, the
// sort key as well. Use the attribute names defined in the struct's json tags.
type TableClient[T any] interface {
	Get(ctx context.Context, keys Keys, opts ...GetOption) (*T, error)
	Put(ctx context.Context, item T, opts ...WriteOption) error
	Update(ctx context.Context, keys Keys, ops []UpdateOp, opts ...WriteOption) (*T, error)
	Delete(ctx context.Context, keys Keys, opts ...WriteOption) error
	Query(ctx context.Context, pkVal any) *QueryBuilder[T]
	Scan(ctx context.Context) *QueryBuilder[T]
	Index(alias string) *IndexTable[T]
	BatchGet(ctx context.Context, keys []Keys, opts ...GetOption) ([]*T, error)
	BatchWrite(ctx context.Context) *BatchWriter[T]
}

// Keys is a set of primary key attribute values for a DynamoDB item.
type Keys map[string]any

// Table[T] describes an existing DynamoDB table and, after Bind, provides
// all read and write operations on it.
//
// T must be a struct whose fields are annotated with dkmio and json struct tags.
//
// Declare at package level, then bind to a client at startup:
//
//	var OrdersTable = dynamodb.Table[Order]{Name: "orders"}
//
//	// at startup:
//	orders := OrdersTable.Bind(dynamo)
//
//	// in handlers:
//	item, err := orders.Get(ctx, "u1", "o1")
type Table[T any] struct {
	// Name is the DynamoDB table name. Required.
	Name string

	// Indexes declares the GSI/LSI definitions used by this table.
	// The map key is an alias used in code (table.Index("alias")).
	Indexes IndexMap

	// client is set by Bind.
	client *Client
	// schema is parsed from struct tags, cached on first use.
	schema *tableSchema
}

// Bind attaches client to this Table descriptor and parses the struct schema.
// It returns a new Table value (the original is unchanged).
// Bind panics if the struct type T has no field tagged dkmio:"pk".
func (t Table[T]) Bind(client *Client) *Table[T] {
	var zero T
	rt := reflect.TypeOf(zero)
	s, err := parseSchema(rt)
	if err != nil {
		panic(fmt.Sprintf("dkmigo/dynamodb: Table[%T].Bind: %v", zero, err))
	}

	// Resolve index key attrs from schema if not overridden.
	for alias, def := range t.Indexes {
		if def.PKAttr == "" {
			def.PKAttr = s.gsiPK[def.Name]
		}
		if def.SKAttr == "" {
			def.SKAttr = s.gsiSK[def.Name]
		}
		t.Indexes[alias] = def
	}

	bound := t
	bound.client = client
	bound.schema = s
	return &bound
}

// mustBound panics if the Table has not been bound to a client.
func (t *Table[T]) mustBound() {
	if t.client == nil {
		panic(fmt.Sprintf("dkmigo/dynamodb: Table[%T] must be bound via .Bind(client) before use", *new(T)))
	}
}

// ----- primary key helpers -----

// buildKeyFromMap constructs a DynamoDB key from a Keys map, validating that
// required key attributes (PK and SK if applicable) are present.
func (t *Table[T]) buildKeyFromMap(keys Keys) (map[string]dbtypes.AttributeValue, error) {
	if _, ok := keys[t.schema.pkAttr]; !ok {
		return nil, &MissingKeyError{Attr: t.schema.pkAttr}
	}
	if t.schema.skAttr != "" {
		if _, ok := keys[t.schema.skAttr]; !ok {
			return nil, &MissingKeyError{Attr: t.schema.skAttr}
		}
	}
	return marshalMap(map[string]any(keys))
}

// buildKey constructs the DynamoDB key map for pkVal (and optionally skVal).
// Used internally by batch and transaction helpers.
func (t *Table[T]) buildKey(pkVal any, skVals []any) (map[string]dbtypes.AttributeValue, error) {
	key := map[string]any{t.schema.pkAttr: pkVal}
	if t.schema.skAttr != "" {
		if len(skVals) == 0 {
			return nil, &MissingKeyError{Attr: t.schema.skAttr}
		}
		key[t.schema.skAttr] = skVals[0]
	}
	return marshalMap(key)
}

// ----- GET -----

// Get fetches a single item by its primary key.
// keys must contain the partition key attribute and, for tables with a sort
// key, the sort key attribute as well. Use the attribute names from json tags.
//
// Returns (nil, nil) when the item does not exist.
//
//	item, err := orders.Get(ctx, dynamodb.Keys{"user_id": "u1", "order_id": "o1"})
//	item, err := orders.Get(ctx, dynamodb.Keys{"user_id": "u1", "order_id": "o1"}, dynamodb.WithConsistentRead())
func (t *Table[T]) Get(ctx context.Context, keys Keys, opts ...GetOption) (*T, error) {
	t.mustBound()
	cfg := applyGetOptions(opts)

	key, err := t.buildKeyFromMap(keys)
	if err != nil {
		return nil, err
	}

	input := &dynamodb.GetItemInput{
		TableName:      aws.String(t.Name),
		Key:            key,
		ConsistentRead: aws.Bool(cfg.consistent),
	}

	if len(cfg.projection) > 0 {
		b := expr.NewBuilder()
		input.ProjectionExpression = aws.String(b.Projection(cfg.projection))
		if n := b.Names(); len(n) > 0 {
			input.ExpressionAttributeNames = n
		}
	}

	var out *dynamodb.GetItemOutput
	err = t.client.execute(ctx, func() error {
		var e error
		out, e = t.client.service().GetItem(ctx, input)
		return e
	})
	if err != nil {
		mapped, _ := mapError(err)
		return nil, mapped
	}
	if len(out.Item) == 0 {
		return nil, nil
	}

	return unmarshalItem[T](out.Item)
}

// ----- PUT -----

// Put creates or replaces an item in the table.
// The item must include all primary key attributes.
func (t *Table[T]) Put(ctx context.Context, item T, opts ...WriteOption) error {
	t.mustBound()
	cfg := applyWriteOptions(opts)

	av, err := marshalItem(item)
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		TableName: aws.String(t.Name),
		Item:      av,
	}

	if cfg.returnValues != "" {
		input.ReturnValues = dbtypes.ReturnValue(cfg.returnValues)
	}

	if len(cfg.conditions) > 0 {
		b := expr.NewBuilder()
		input.ConditionExpression = aws.String(buildFilter(b, cfg.conditions))
		input.ExpressionAttributeNames = b.Names()
		if len(b.Values()) > 0 {
			input.ExpressionAttributeValues = marshalValues(b.Values())
		}
	}

	return t.client.execute(ctx, func() error {
		_, e := t.client.service().PutItem(ctx, input)
		mapped, _ := mapError(e)
		return mapped
	})
}

// ----- UPDATE -----

// Update modifies specific attributes of an existing item.
// keys must contain the PK (and SK if applicable).
// ops defines the changes: Set, Remove, Add, Append, DeleteSet.
// Returns the item after the update when ReturnUpdated() option is used.
func (t *Table[T]) Update(ctx context.Context, keys Keys, ops []UpdateOp, opts ...WriteOption) (*T, error) {
	t.mustBound()
	cfg := applyWriteOptions(opts)

	if len(ops) == 0 {
		return nil, &ValidationError{msg: "at least one update operation is required"}
	}

	keyAV, err := marshalMap(map[string]any(keys))
	if err != nil {
		return nil, &ValidationError{cause: err}
	}

	b := expr.NewBuilder()
	updateExpr := buildUpdateExpression(b, ops)

	input := &dynamodb.UpdateItemInput{
		TableName:                aws.String(t.Name),
		Key:                      keyAV,
		UpdateExpression:         aws.String(updateExpr),
		ExpressionAttributeNames: b.Names(),
	}

	if len(b.Values()) > 0 {
		input.ExpressionAttributeValues = marshalValues(b.Values())
	}

	if cfg.returnValues != "" {
		input.ReturnValues = dbtypes.ReturnValue(cfg.returnValues)
	} else {
		input.ReturnValues = dbtypes.ReturnValueNone
	}

	if len(cfg.conditions) > 0 {
		condExpr := buildFilter(b, cfg.conditions)
		input.ConditionExpression = aws.String(condExpr)
		// Re-apply names/values after condition was built.
		input.ExpressionAttributeNames = b.Names()
		if len(b.Values()) > 0 {
			input.ExpressionAttributeValues = marshalValues(b.Values())
		}
	}

	var out *dynamodb.UpdateItemOutput
	err = t.client.execute(ctx, func() error {
		var e error
		out, e = t.client.service().UpdateItem(ctx, input)
		mapped, _ := mapError(e)
		return mapped
	})
	if err != nil {
		return nil, err
	}

	if len(out.Attributes) == 0 {
		return nil, nil
	}
	return unmarshalItem[T](out.Attributes)
}

// ----- DELETE -----

// Delete removes an item from the table by its primary key.
//
//	err := orders.Delete(ctx, dynamodb.Keys{"user_id": "u1", "order_id": "o1"})
//	err := orders.Delete(ctx, dynamodb.Keys{...}, dynamodb.WithCondition(dynamodb.EQ("status", "pending")))
func (t *Table[T]) Delete(ctx context.Context, keys Keys, opts ...WriteOption) error {
	t.mustBound()
	cfg := applyWriteOptions(opts)

	key, err := t.buildKeyFromMap(keys)
	if err != nil {
		return err
	}

	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(t.Name),
		Key:       key,
	}

	if cfg.returnValues != "" {
		input.ReturnValues = dbtypes.ReturnValue(cfg.returnValues)
	}

	if len(cfg.conditions) > 0 {
		b := expr.NewBuilder()
		input.ConditionExpression = aws.String(buildFilter(b, cfg.conditions))
		input.ExpressionAttributeNames = b.Names()
		if len(b.Values()) > 0 {
			input.ExpressionAttributeValues = marshalValues(b.Values())
		}
	}

	return t.client.execute(ctx, func() error {
		_, e := t.client.service().DeleteItem(ctx, input)
		mapped, _ := mapError(e)
		return mapped
	})
}

// ----- QUERY / SCAN -----

// Query starts a QueryBuilder for this table using pkVal as the partition key.
// Chain .Where, .Filter, .Select, .Limit, etc. and call .Exec() to run.
func (t *Table[T]) Query(ctx context.Context, pkVal any) *QueryBuilder[T] {
	t.mustBound()
	return newQueryBuilder(t, pkVal).withContext(ctx)
}

// Scan starts a QueryBuilder that will perform a full table scan.
// Use sparingly — scans read every item in the table.
func (t *Table[T]) Scan(ctx context.Context) *QueryBuilder[T] {
	t.mustBound()
	return newScanBuilder(t).withContext(ctx)
}

// Index returns an IndexTable for querying the named index alias.
// Panics if alias is not declared in Table.Indexes.
func (t *Table[T]) Index(alias string) *IndexTable[T] {
	t.mustBound()
	def, ok := t.Indexes[alias]
	if !ok {
		panic(fmt.Sprintf("dkmigo/dynamodb: index alias %q not declared in Table.Indexes", alias))
	}

	pkAttr := def.PKAttr
	if pkAttr == "" {
		pkAttr = t.schema.gsiPK[def.Name]
	}
	skAttr := def.SKAttr
	if skAttr == "" {
		skAttr = t.schema.gsiSK[def.Name]
	}

	return &IndexTable[T]{
		table:  t,
		alias:  alias,
		def:    def,
		pkAttr: pkAttr,
		skAttr: skAttr,
	}
}

// ----- BATCH -----

// BatchGet fetches multiple items by key in a single request (up to 100 keys).
// Missing items appear as nil at the corresponding index position.
func (t *Table[T]) BatchGet(ctx context.Context, keys []Keys, opts ...GetOption) ([]*T, error) {
	t.mustBound()
	return batchGet(ctx, t, keys, opts)
}

// BatchWrite returns a BatchWriter for efficient multi-item puts and deletes.
//
//	err := table.BatchWrite(ctx).Put(item1, item2).Delete(k1, k2).Exec()
func (t *Table[T]) BatchWrite(ctx context.Context) *BatchWriter[T] {
	t.mustBound()
	return newBatchWriter(ctx, t)
}

// ----- internal marshal helpers -----

// marshalItem converts a typed struct to a DynamoDB attribute map using json
// tags as attribute names. This ensures DynamoDB attribute names match json
// tags (e.g. "user_id") rather than Go field names (e.g. "UserID").
func marshalItem[T any](item T) (map[string]dbtypes.AttributeValue, error) {
	// json.Marshal respects json tags → map keys are json tag values.
	b, err := json.Marshal(item)
	if err != nil {
		return nil, &ValidationError{cause: err}
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, &ValidationError{cause: err}
	}
	return attributevalue.MarshalMap(m)
}

func unmarshalItem[T any](av map[string]dbtypes.AttributeValue) (*T, error) {
	normalized := serial.NormalizeItem(av)

	// Round-trip through JSON to unmarshal into T via json tags.
	b, err := json.Marshal(normalized)
	if err != nil {
		return nil, err
	}
	var out T
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// marshalMap converts map[string]any to DynamoDB AttributeValue map.
func marshalMap(m map[string]any) (map[string]dbtypes.AttributeValue, error) {
	return attributevalue.MarshalMap(m)
}

// marshalValues converts map[string]any ExpressionAttributeValues to AWS types.
func marshalValues(m map[string]any) map[string]dbtypes.AttributeValue {
	out := make(map[string]dbtypes.AttributeValue, len(m))
	for k, v := range m {
		av, err := attributevalue.Marshal(v)
		if err == nil {
			out[k] = av
		}
	}
	return out
}
