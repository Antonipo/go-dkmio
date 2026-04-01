package dynamodb

import "github.com/Antonipo/go-dkmio/internal/expr"

// UpdateOp is a single update action (SET, REMOVE, ADD, DELETE, APPEND).
// Use the constructor functions (Set, Remove, Add, Append, DeleteSet) to build them.
type UpdateOp struct {
	clause expr.UpdateClause
}

// Set overwrites the given attributes with the provided values.
// Supports nested paths: Set(map[string]any{"address.city": "Lima"}).
func Set(attrs map[string]any) UpdateOp {
	return UpdateOp{expr.UpdateClause{
		Action: expr.ActionSet,
		Attrs:  attrs,
	}}
}

// Remove removes the listed attributes from the item.
func Remove(attrs ...string) UpdateOp {
	m := make(map[string]any, len(attrs))
	for _, a := range attrs {
		m[a] = nil
	}
	return UpdateOp{expr.UpdateClause{Action: expr.ActionRemove, Attrs: m}}
}

// Append appends elements to list attributes using list_append.
// If the attribute does not exist it is initialised as an empty list first.
func Append(attrs map[string]any) UpdateOp {
	return UpdateOp{expr.UpdateClause{Action: expr.ActionAppend, Attrs: attrs}}
}

// Add adds a number delta to numeric attributes or unions a set with a set attribute.
// Use negative values to subtract.
func Add(attrs map[string]any) UpdateOp {
	return UpdateOp{expr.UpdateClause{Action: expr.ActionAdd, Attrs: attrs}}
}

// DeleteSet removes elements from a set attribute.
// vals must be sets (e.g. map[string]any{"tags": []string{"old"}}).
func DeleteSet(attrs map[string]any) UpdateOp {
	return UpdateOp{expr.UpdateClause{Action: expr.ActionDelete, Attrs: attrs}}
}

// buildUpdateExpression compiles a list of UpdateOps into a DynamoDB
// UpdateExpression string and populates b with the required name/value mappings.
func buildUpdateExpression(b *expr.Builder, ops []UpdateOp) string {
	clauses := make([]expr.UpdateClause, len(ops))
	for i, op := range ops {
		clauses[i] = op.clause
	}
	return b.UpdateExpression(clauses)
}
