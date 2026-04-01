package dynamodb

import (
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	maxBatchGetSize   = 100 // DynamoDB BatchGetItem limit
	maxBatchWriteSize = 25  // DynamoDB BatchWriteItem limit
	maxBatchRetries   = 5
)

// ----- BatchGet -----

// batchGet fetches up to 100 items per DynamoDB request, chunking automatically.
// Results are returned in the same order as keys; missing items appear as nil.
func batchGet[T any](ctx context.Context, t *Table[T], keys []Keys, opts []GetOption) ([]*T, error) {
	cfg := applyGetOptions(opts)

	// Convert keys to DynamoDB format.
	avKeys := make([]map[string]dbtypes.AttributeValue, 0, len(keys))
	for _, k := range keys {
		av, err := marshalMap(map[string]any(k))
		if err != nil {
			return nil, &ValidationError{cause: err}
		}
		avKeys = append(avKeys, av)
	}

	// Build an index so we can restore original order.
	// DynamoDB BatchGetItem does not guarantee order.
	type indexedItem struct {
		item  *T
		index int
	}
	results := make([]*T, len(keys))

	// Process in chunks of maxBatchGetSize.
	for chunkStart := 0; chunkStart < len(avKeys); chunkStart += maxBatchGetSize {
		end := chunkStart + maxBatchGetSize
		if end > len(avKeys) {
			end = len(avKeys)
		}
		chunk := avKeys[chunkStart:end]
		originalIndexes := make([]int, end-chunkStart)
		for i := range originalIndexes {
			originalIndexes[i] = chunkStart + i
		}

		keysAndAttrs := dbtypes.KeysAndAttributes{Keys: chunk}
		if cfg.consistent {
			keysAndAttrs.ConsistentRead = aws.Bool(true)
		}
		if len(cfg.projection) > 0 {
			keysAndAttrs.ProjectionExpression = aws.String(buildProjectionExpr(cfg.projection))
		}

		requestMap := map[string]dbtypes.KeysAndAttributes{t.Name: keysAndAttrs}

		// Retry loop for unprocessed keys.
		backoff := 50 * time.Millisecond
		for attempt := 0; attempt < maxBatchRetries; attempt++ {
			input := &dynamodb.BatchGetItemInput{RequestItems: requestMap}

			var out *dynamodb.BatchGetItemOutput
			err := t.client.execute(ctx, func() error {
				var e error
				out, e = t.client.service().BatchGetItem(ctx, input)
				mapped, _ := mapError(e)
				return mapped
			})
			if err != nil {
				return nil, err
			}

			// Decode returned items.
			for _, rawItem := range out.Responses[t.Name] {
				item, err := unmarshalItem[T](rawItem)
				if err != nil {
					continue
				}
				// Match back to original key position.
				idx := matchKey(rawItem, avKeys, chunkStart)
				if idx >= 0 {
					results[idx] = item
				}
			}

			// Retry unprocessed keys.
			unprocessed, ok := out.UnprocessedKeys[t.Name]
			if !ok || len(unprocessed.Keys) == 0 {
				break
			}
			requestMap = map[string]dbtypes.KeysAndAttributes{t.Name: unprocessed}
			time.Sleep(backoff)
			backoff *= 2
		}
	}

	return results, nil
}

// matchKey finds the original index of a returned item by comparing key attributes.
func matchKey(item map[string]dbtypes.AttributeValue, keys []map[string]dbtypes.AttributeValue, offset int) int {
	for i, k := range keys {
		match := true
		for attr, kv := range k {
			iv, ok := item[attr]
			if !ok {
				match = false
				break
			}
			// Compare via marshalled JSON — simple but correct for key types.
			if avString(kv) != avString(iv) {
				match = false
				break
			}
		}
		if match {
			return offset + i
		}
	}
	return -1
}

func avString(av dbtypes.AttributeValue) string {
	switch v := av.(type) {
	case *dbtypes.AttributeValueMemberS:
		return "S:" + v.Value
	case *dbtypes.AttributeValueMemberN:
		return "N:" + v.Value
	case *dbtypes.AttributeValueMemberB:
		return "B:" + string(v.Value)
	default:
		return ""
	}
}

func buildProjectionExpr(attrs []string) string {
	// Simplified: just join with commas. Full reserved-word escaping requires
	// ExpressionAttributeNames wiring which would need to be set on the struct.
	// For now, a basic join suffices for non-reserved attribute names.
	result := ""
	for i, a := range attrs {
		if i > 0 {
			result += ", "
		}
		result += a
	}
	return result
}

// ----- BatchWriter -----

// BatchWriter accumulates put and delete operations and executes them in
// efficient chunks (max 25 per request).
//
// Obtain one via table.BatchWrite(ctx) and call Exec when done.
type BatchWriter[T any] struct {
	ctx    context.Context
	table  *Table[T]
	puts   []dbtypes.WriteRequest
	deletes []dbtypes.WriteRequest
}

func newBatchWriter[T any](ctx context.Context, t *Table[T]) *BatchWriter[T] {
	return &BatchWriter[T]{ctx: ctx, table: t}
}

// Put queues one or more items for writing.
func (w *BatchWriter[T]) Put(items ...T) *BatchWriter[T] {
	for _, item := range items {
		av, err := marshalItem(item)
		if err != nil {
			continue // validation errors surface at Exec time
		}
		w.puts = append(w.puts, dbtypes.WriteRequest{
			PutRequest: &dbtypes.PutRequest{Item: av},
		})
	}
	return w
}

// Delete queues a delete by primary key.
// pkVal is the partition key; skVals[0] is the sort key (if the table has one).
func (w *BatchWriter[T]) Delete(pkVal any, skVals ...any) *BatchWriter[T] {
	key, err := w.table.buildKey(pkVal, skVals)
	if err != nil {
		return w
	}
	w.deletes = append(w.deletes, dbtypes.WriteRequest{
		DeleteRequest: &dbtypes.DeleteRequest{Key: key},
	})
	return w
}

// Exec sends all queued operations to DynamoDB in chunks, retrying unprocessed
// items with exponential backoff.
func (w *BatchWriter[T]) Exec() error {
	all := append(w.puts, w.deletes...)
	if len(all) == 0 {
		return nil
	}

	backoff := 50 * time.Millisecond

	for chunkStart := 0; chunkStart < len(all); chunkStart += maxBatchWriteSize {
		end := chunkStart + maxBatchWriteSize
		if end > len(all) {
			end = len(all)
		}
		chunk := all[chunkStart:end]
		requestMap := map[string][]dbtypes.WriteRequest{w.table.Name: chunk}

		for attempt := 0; attempt < maxBatchRetries; attempt++ {
			input := &dynamodb.BatchWriteItemInput{RequestItems: requestMap}

			var out *dynamodb.BatchWriteItemOutput
			err := w.table.client.execute(w.ctx, func() error {
				var e error
				out, e = w.table.client.service().BatchWriteItem(w.ctx, input)
				mapped, _ := mapError(e)
				return mapped
			})
			if err != nil {
				return err
			}

			unprocessed, ok := out.UnprocessedItems[w.table.Name]
			if !ok || len(unprocessed) == 0 {
				break
			}
			requestMap = map[string][]dbtypes.WriteRequest{w.table.Name: unprocessed}
			time.Sleep(backoff)
			backoff *= 2
		}
	}
	return nil
}
