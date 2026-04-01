package dynamodb

import "github.com/Antonipo/go-dkmio/internal/expr"

// Condition represents a filter or condition expression clause.
// Use the constructor functions (EQ, GT, Exists, etc.) to build conditions.
type Condition struct {
	attr string
	op   string
	vals []any
}

// SKCondition represents a sort-key condition for Query's Where clause.
type SKCondition struct {
	op   string
	vals []any
}

// ----- filter condition constructors -----

// EQ matches items where attr equals val.
func EQ(attr string, val any) Condition { return Condition{attr, "eq", []any{val}} }

// NEQ matches items where attr does not equal val.
func NEQ(attr string, val any) Condition { return Condition{attr, "neq", []any{val}} }

// GT matches items where attr is greater than val.
func GT(attr string, val any) Condition { return Condition{attr, "gt", []any{val}} }

// GTE matches items where attr is greater than or equal to val.
func GTE(attr string, val any) Condition { return Condition{attr, "gte", []any{val}} }

// LT matches items where attr is less than val.
func LT(attr string, val any) Condition { return Condition{attr, "lt", []any{val}} }

// LTE matches items where attr is less than or equal to val.
func LTE(attr string, val any) Condition { return Condition{attr, "lte", []any{val}} }

// Between matches items where attr is between lo and hi (inclusive).
func Between(attr string, lo, hi any) Condition {
	return Condition{attr, "between", []any{lo, hi}}
}

// BeginsWith matches items where attr starts with prefix.
func BeginsWith(attr, prefix string) Condition {
	return Condition{attr, "begins_with", []any{prefix}}
}

// Contains matches items where attr contains val (substring or list element).
func Contains(attr string, val any) Condition {
	return Condition{attr, "contains", []any{val}}
}

// NotContains matches items where attr does not contain val.
func NotContains(attr string, val any) Condition {
	return Condition{attr, "not_contains", []any{val}}
}

// NotBeginsWith matches items where attr does not start with prefix.
func NotBeginsWith(attr, prefix string) Condition {
	return Condition{attr, "not_begins_with", []any{prefix}}
}

// Exists matches items where attr exists.
func Exists(attr string) Condition { return Condition{attr, "exists", nil} }

// NotExists matches items where attr does not exist.
func NotExists(attr string) Condition { return Condition{attr, "not_exists", nil} }

// AttrType matches items where attr is of the given DynamoDB type
// (S, N, B, SS, NS, BS, M, L, NULL, BOOL).
func AttrType(attr, dynamoType string) Condition {
	return Condition{attr, "type", []any{dynamoType}}
}

// In matches items where attr is one of the provided values.
func In(attr string, vals ...any) Condition { return Condition{attr, "in", vals} }

// SizeEQ matches items where the size of attr equals n.
func SizeEQ(attr string, n int) Condition { return Condition{attr, "size_eq", []any{n}} }

// SizeGT matches items where the size of attr is greater than n.
func SizeGT(attr string, n int) Condition { return Condition{attr, "size_gt", []any{n}} }

// SizeGTE matches items where the size of attr is greater than or equal to n.
func SizeGTE(attr string, n int) Condition { return Condition{attr, "size_gte", []any{n}} }

// SizeLT matches items where the size of attr is less than n.
func SizeLT(attr string, n int) Condition { return Condition{attr, "size_lt", []any{n}} }

// SizeLTE matches items where the size of attr is less than or equal to n.
func SizeLTE(attr string, n int) Condition { return Condition{attr, "size_lte", []any{n}} }

// ----- sort key condition constructors -----

// SKEQ matches items where the sort key equals val.
func SKEQ(val any) SKCondition { return SKCondition{"eq", []any{val}} }

// SKGT matches items where the sort key is greater than val.
func SKGT(val any) SKCondition { return SKCondition{"gt", []any{val}} }

// SKGTE matches items where the sort key is greater than or equal to val.
func SKGTE(val any) SKCondition { return SKCondition{"gte", []any{val}} }

// SKLT matches items where the sort key is less than val.
func SKLT(val any) SKCondition { return SKCondition{"lt", []any{val}} }

// SKLTE matches items where the sort key is less than or equal to val.
func SKLTE(val any) SKCondition { return SKCondition{"lte", []any{val}} }

// SKBetween matches items where the sort key is between lo and hi (inclusive).
func SKBetween(lo, hi any) SKCondition { return SKCondition{"between", []any{lo, hi}} }

// SKBeginsWith matches items where the sort key starts with prefix.
func SKBeginsWith(prefix string) SKCondition { return SKCondition{"begins_with", []any{prefix}} }

// ----- internal helpers -----

// buildFilter renders a slice of conditions into an expression string using b.
// Multiple conditions are joined with AND.
func buildFilter(b *expr.Builder, conds []Condition) string {
	clauses := make([]string, 0, len(conds))
	for _, c := range conds {
		clauses = append(clauses, b.FilterExpr(c.attr, c.op, c.vals...))
	}
	return expr.JoinAND(clauses)
}

