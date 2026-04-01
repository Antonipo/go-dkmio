// Package serial handles type normalization for DynamoDB responses.
// The AWS SDK v2 returns numbers as string when using low-level APIs,
// and the attributevalue unmarshaler uses its own type rules.
// This package normalises values to standard Go types.
package serial

import (
	"math/big"
	"sort"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// NormalizeItem converts a DynamoDB attribute map to a plain map[string]any
// with Go-native types:
//   - AttributeValueMemberN  → int64 or float64
//   - AttributeValueMemberSS → []string (sorted)
//   - AttributeValueMemberNS → []float64 (sorted)
//   - AttributeValueMemberBS → [][]byte
//   - AttributeValueMemberL  → []any (recursive)
//   - AttributeValueMemberM  → map[string]any (recursive)
//   - Others                 → their native Go value
func NormalizeItem(item map[string]types.AttributeValue) map[string]any {
	out := make(map[string]any, len(item))
	for k, v := range item {
		out[k] = NormalizeValue(v)
	}
	return out
}

// NormalizeValue converts a single AttributeValue to a Go-native type.
func NormalizeValue(v types.AttributeValue) any {
	switch av := v.(type) {
	case *types.AttributeValueMemberS:
		return av.Value
	case *types.AttributeValueMemberN:
		return parseNumber(av.Value)
	case *types.AttributeValueMemberBOOL:
		return av.Value
	case *types.AttributeValueMemberNULL:
		return nil
	case *types.AttributeValueMemberB:
		return av.Value
	case *types.AttributeValueMemberSS:
		sorted := make([]string, len(av.Value))
		copy(sorted, av.Value)
		sort.Strings(sorted)
		return sorted
	case *types.AttributeValueMemberNS:
		nums := make([]float64, len(av.Value))
		for i, s := range av.Value {
			f, _, _ := new(big.Float).Parse(s, 10)
			v, _ := f.Float64()
			nums[i] = v
		}
		sort.Float64s(nums)
		return nums
	case *types.AttributeValueMemberBS:
		return av.Value
	case *types.AttributeValueMemberL:
		list := make([]any, len(av.Value))
		for i, item := range av.Value {
			list[i] = NormalizeValue(item)
		}
		return list
	case *types.AttributeValueMemberM:
		return NormalizeItem(av.Value)
	default:
		return nil
	}
}

// parseNumber converts a DynamoDB number string to int64 if it has no
// fractional part, or float64 otherwise.
func parseNumber(s string) any {
	f, _, err := new(big.Float).Parse(s, 10)
	if err != nil {
		return s // fallback: return raw string
	}
	// Try integer first.
	if f.IsInt() {
		i, _ := f.Int64()
		return i
	}
	v, _ := f.Float64()
	return v
}
