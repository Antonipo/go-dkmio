package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/Antonipo/go-dkmio/internal/expr"
	"github.com/Antonipo/go-dkmio/internal/serial"
)

// QueryBuilder constructs and executes a DynamoDB Query or Scan operation.
// All methods return *QueryBuilder so calls can be chained fluently.
// Nothing is sent to DynamoDB until Exec, FetchAll, or Count is called.
type QueryBuilder[T any] struct {
	table      *Table[T]
	ctx        context.Context
	isScan     bool
	pkVal      any
	skCond     *SKCondition
	filters    []Condition
	projection []string
	limit      *int32
	lastKey    map[string]dbtypes.AttributeValue
	forward    *bool
	consistent bool
	index      *IndexTable[T]
}

func newQueryBuilder[T any](t *Table[T], pkVal any) *QueryBuilder[T] {
	return &QueryBuilder[T]{table: t, pkVal: pkVal, ctx: context.Background()}
}

func newScanBuilder[T any](t *Table[T]) *QueryBuilder[T] {
	return &QueryBuilder[T]{table: t, isScan: true, ctx: context.Background()}
}

func (q *QueryBuilder[T]) withContext(ctx context.Context) *QueryBuilder[T] {
	q.ctx = ctx
	return q
}

func (q *QueryBuilder[T]) withIndex(ix *IndexTable[T]) *QueryBuilder[T] {
	q.index = ix
	return q
}

// Where adds a sort-key condition (KeyConditionExpression).
// Only valid for Query (not Scan). Use the SK* constructors:
//
//	.Where(dynamodb.SKGTE("ord_100"))
//	.Where(dynamodb.SKBetween("a", "z"))
func (q *QueryBuilder[T]) Where(cond SKCondition) *QueryBuilder[T] {
	q.skCond = &cond
	return q
}

// Filter adds one or more filter expressions (FilterExpression).
// Multiple calls are AND-joined with previous filters.
func (q *QueryBuilder[T]) Filter(conds ...Condition) *QueryBuilder[T] {
	q.filters = append(q.filters, conds...)
	return q
}

// Select limits which attributes are returned (ProjectionExpression).
// When querying an index with ProjectionInclude, dkmigo validates that all
// requested attrs are available in the index's projection.
func (q *QueryBuilder[T]) Select(attrs ...string) *QueryBuilder[T] {
	q.projection = append(q.projection, attrs...)
	return q
}

// Limit sets the maximum number of items evaluated per page.
// DynamoDB applies the limit before FilterExpression, so fewer items than
// limit may appear in the result.
func (q *QueryBuilder[T]) Limit(n int32) *QueryBuilder[T] {
	q.limit = &n
	return q
}

// StartFrom resumes pagination from the LastKey of a previous QueryResult.
func (q *QueryBuilder[T]) StartFrom(lastKey map[string]dbtypes.AttributeValue) *QueryBuilder[T] {
	q.lastKey = lastKey
	return q
}

// ScanForward sets the sort order: true = ascending (default), false = descending.
func (q *QueryBuilder[T]) ScanForward(v bool) *QueryBuilder[T] {
	q.forward = &v
	return q
}

// Consistent enables strongly consistent reads.
func (q *QueryBuilder[T]) Consistent() *QueryBuilder[T] {
	q.consistent = true
	return q
}

// Exec executes the Query or Scan and returns one page of results.
func (q *QueryBuilder[T]) Exec() (*QueryResult[T], error) {
	if q.isScan {
		return q.execScan()
	}
	return q.execQuery()
}

// FetchAll paginates automatically until all matching items are retrieved.
// If maxItems > 0, stops after that many items.
func (q *QueryBuilder[T]) FetchAll(maxItems int) ([]T, error) {
	var all []T
	cur := q
	for {
		res, err := cur.Exec()
		if err != nil {
			return nil, err
		}
		all = append(all, res.Items...)
		if maxItems > 0 && len(all) >= maxItems {
			return all[:maxItems], nil
		}
		if !res.HasMore() {
			return all, nil
		}
		next := *cur
		next.lastKey = res.LastKey
		cur = &next
	}
}

// Count returns the total number of matching items, paginating automatically.
func (q *QueryBuilder[T]) Count() (int64, error) {
	var total int64
	cur := q
	for {
		res, err := cur.Exec()
		if err != nil {
			return 0, err
		}
		total += int64(res.Count)
		if !res.HasMore() {
			return total, nil
		}
		next := *cur
		next.lastKey = res.LastKey
		cur = &next
	}
}

// ----- internal execution -----

func (q *QueryBuilder[T]) execQuery() (*QueryResult[T], error) {
	t := q.table
	b := expr.NewBuilder()

	pkAttr := t.schema.pkAttr
	if q.index != nil {
		pkAttr = q.index.pkAttr
	}
	pkCond := b.NameRef(pkAttr) + " = " + b.ValueRef(q.pkVal)

	var keyCond string
	if q.skCond != nil {
		skAttr := t.schema.skAttr
		if q.index != nil {
			skAttr = q.index.skAttr
		}
		if skAttr == "" {
			return nil, fmt.Errorf("dkmigo/dynamodb: Where() requires a sort key — table has none")
		}
		skExpr := b.KeyCondition(skAttr, q.skCond.op, q.skCond.vals...)
		keyCond = pkCond + " AND " + skExpr
	} else {
		keyCond = pkCond
	}

	input := &dynamodb.QueryInput{
		TableName:              aws.String(t.Name),
		KeyConditionExpression: aws.String(keyCond),
		ConsistentRead:         aws.Bool(q.consistent),
	}

	if q.index != nil {
		input.IndexName = aws.String(q.index.def.Name)
	}
	if len(q.filters) > 0 {
		input.FilterExpression = aws.String(buildFilter(b, q.filters))
	}
	if len(q.projection) > 0 {
		if err := q.validateProjection(); err != nil {
			return nil, err
		}
		input.ProjectionExpression = aws.String(b.Projection(q.projection))
	}
	if q.limit != nil {
		input.Limit = q.limit
	}
	if len(q.lastKey) > 0 {
		input.ExclusiveStartKey = q.lastKey
	}
	if q.forward != nil {
		input.ScanIndexForward = q.forward
	}

	input.ExpressionAttributeNames = b.Names()
	if len(b.Values()) > 0 {
		input.ExpressionAttributeValues = marshalValues(b.Values())
	}

	var out *dynamodb.QueryOutput
	err := t.client.execute(q.ctx, func() error {
		var e error
		out, e = t.client.service().Query(q.ctx, input)
		mapped, _ := mapError(e)
		return mapped
	})
	if err != nil {
		return nil, err
	}

	return buildQueryResult[T](out.Items, out.LastEvaluatedKey, out.Count, out.ScannedCount), nil
}

func (q *QueryBuilder[T]) execScan() (*QueryResult[T], error) {
	t := q.table
	b := expr.NewBuilder()

	input := &dynamodb.ScanInput{
		TableName:      aws.String(t.Name),
		ConsistentRead: aws.Bool(q.consistent),
	}

	if q.index != nil {
		input.IndexName = aws.String(q.index.def.Name)
	}
	if len(q.filters) > 0 {
		input.FilterExpression = aws.String(buildFilter(b, q.filters))
	}
	if len(q.projection) > 0 {
		input.ProjectionExpression = aws.String(b.Projection(q.projection))
	}
	if q.limit != nil {
		input.Limit = q.limit
	}
	if len(q.lastKey) > 0 {
		input.ExclusiveStartKey = q.lastKey
	}

	input.ExpressionAttributeNames = b.Names()
	if len(b.Values()) > 0 {
		input.ExpressionAttributeValues = marshalValues(b.Values())
	}

	var out *dynamodb.ScanOutput
	err := t.client.execute(q.ctx, func() error {
		var e error
		out, e = t.client.service().Scan(q.ctx, input)
		mapped, _ := mapError(e)
		return mapped
	})
	if err != nil {
		return nil, err
	}

	return buildQueryResult[T](out.Items, out.LastEvaluatedKey, out.Count, out.ScannedCount), nil
}

func (q *QueryBuilder[T]) validateProjection() error {
	if q.index == nil {
		return nil
	}
	proj := q.index.def.Projection
	if proj.Type == ProjectionAll {
		return nil
	}

	allowed := map[string]bool{
		q.table.schema.pkAttr: true,
		q.table.schema.skAttr: true,
		q.index.pkAttr:        true,
		q.index.skAttr:        true,
	}
	if proj.Type == ProjectionInclude {
		for _, a := range proj.Attrs {
			allowed[a] = true
		}
	}
	for _, attr := range q.projection {
		if !allowed[attr] {
			return &InvalidProjectionError{Attr: attr, Index: q.index.alias}
		}
	}
	return nil
}

// ----- QueryResult -----

// QueryResult holds one page of results from a Query or Scan operation.
type QueryResult[T any] struct {
	// Items is the list of decoded items in this page.
	Items []T
	// LastKey is the pagination token for the next page.
	// nil when there are no more pages.
	LastKey map[string]dbtypes.AttributeValue
	// Count is the number of items returned after filter expressions.
	Count int32
	// ScannedCount is the number of items read before filter expressions.
	ScannedCount int32
}

// HasMore reports whether there are additional pages to fetch.
func (r *QueryResult[T]) HasMore() bool { return len(r.LastKey) > 0 }

func buildQueryResult[T any](
	rawItems []map[string]dbtypes.AttributeValue,
	lastKey map[string]dbtypes.AttributeValue,
	count, scanned int32,
) *QueryResult[T] {
	items := make([]T, 0, len(rawItems))
	for _, raw := range rawItems {
		normalized := serial.NormalizeItem(raw)
		b, err := json.Marshal(normalized)
		if err != nil {
			continue
		}
		var item T
		if err := json.Unmarshal(b, &item); err == nil {
			items = append(items, item)
		}
	}
	return &QueryResult[T]{
		Items:        items,
		LastKey:      lastKey,
		Count:        count,
		ScannedCount: scanned,
	}
}
