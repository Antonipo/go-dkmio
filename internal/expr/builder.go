package expr

import (
	"fmt"
	"strings"
)

// Builder constructs DynamoDB expression strings and their attribute maps.
// A single Builder is used per operation — call the appropriate methods then
// call Build() to produce the final params struct.
//
// Thread-safety: Builder is NOT safe for concurrent use. Create a new one
// per operation.
type Builder struct {
	names  map[string]string // placeholder → actual attribute name
	values map[string]any    // placeholder → actual value
	nameN  int
	valueN int
}

// NewBuilder returns an initialised Builder.
func NewBuilder() *Builder {
	return &Builder{
		names:  make(map[string]string),
		values: make(map[string]any),
	}
}

// Names returns the ExpressionAttributeNames map (nil if empty).
func (b *Builder) Names() map[string]string {
	if len(b.names) == 0 {
		return nil
	}
	return b.names
}

// Values returns the ExpressionAttributeValues map (nil if empty).
func (b *Builder) Values() map[string]any {
	if len(b.values) == 0 {
		return nil
	}
	return b.values
}

// NameRef returns a safe placeholder for attrPath, escaping reserved words and
// registering the mapping in ExpressionAttributeNames.
// attrPath may be a dot-separated nested path like "address.city" or
// a list-indexed path like "items[0].qty".
func (b *Builder) NameRef(attrPath string) string {
	// Split on "." but respect list indexes like items[0].name
	parts := splitPath(attrPath)
	refs := make([]string, len(parts))
	for i, p := range parts {
		// Separate list index suffix (e.g. "items[0]" → "items" + "[0]")
		name, idx := splitListIndex(p)
		if IsReserved(name) || strings.ContainsAny(name, ".-[]") {
			refs[i] = b.registerName(name) + idx
		} else {
			refs[i] = name + idx
		}
	}
	return strings.Join(refs, ".")
}

// ValueRef registers val as an ExpressionAttributeValue and returns its
// placeholder string (e.g. ":v0").
func (b *Builder) ValueRef(val any) string {
	placeholder := fmt.Sprintf(":v%d", b.valueN)
	b.values[placeholder] = val
	b.valueN++
	return placeholder
}

// KeyCondition builds a KeyConditionExpression for sort-key conditions.
// op is one of: eq, gt, gte, lt, lte, between, begins_with.
func (b *Builder) KeyCondition(skAttr, op string, vals ...any) string {
	nameRef := b.NameRef(skAttr)
	switch op {
	case "eq":
		return nameRef + " = " + b.ValueRef(vals[0])
	case "gt":
		return nameRef + " > " + b.ValueRef(vals[0])
	case "gte":
		return nameRef + " >= " + b.ValueRef(vals[0])
	case "lt":
		return nameRef + " < " + b.ValueRef(vals[0])
	case "lte":
		return nameRef + " <= " + b.ValueRef(vals[0])
	case "between":
		return fmt.Sprintf("%s BETWEEN %s AND %s",
			nameRef, b.ValueRef(vals[0]), b.ValueRef(vals[1]))
	case "begins_with":
		return fmt.Sprintf("begins_with(%s, %s)", nameRef, b.ValueRef(vals[0]))
	default:
		panic("dkmigo/expr: unknown key condition op: " + op)
	}
}

// FilterExpr builds one FilterExpression clause for the given operator.
func (b *Builder) FilterExpr(attr, op string, vals ...any) string {
	nameRef := b.NameRef(attr)
	switch op {
	case "eq":
		return nameRef + " = " + b.ValueRef(vals[0])
	case "neq":
		return nameRef + " <> " + b.ValueRef(vals[0])
	case "gt":
		return nameRef + " > " + b.ValueRef(vals[0])
	case "gte":
		return nameRef + " >= " + b.ValueRef(vals[0])
	case "lt":
		return nameRef + " < " + b.ValueRef(vals[0])
	case "lte":
		return nameRef + " <= " + b.ValueRef(vals[0])
	case "between":
		return fmt.Sprintf("%s BETWEEN %s AND %s",
			nameRef, b.ValueRef(vals[0]), b.ValueRef(vals[1]))
	case "begins_with":
		return fmt.Sprintf("begins_with(%s, %s)", nameRef, b.ValueRef(vals[0]))
	case "contains":
		return fmt.Sprintf("contains(%s, %s)", nameRef, b.ValueRef(vals[0]))
	case "not_contains":
		return fmt.Sprintf("NOT contains(%s, %s)", nameRef, b.ValueRef(vals[0]))
	case "not_begins_with":
		return fmt.Sprintf("NOT begins_with(%s, %s)", nameRef, b.ValueRef(vals[0]))
	case "exists":
		return fmt.Sprintf("attribute_exists(%s)", nameRef)
	case "not_exists":
		return fmt.Sprintf("attribute_not_exists(%s)", nameRef)
	case "type":
		return fmt.Sprintf("attribute_type(%s, %s)", nameRef, b.ValueRef(vals[0]))
	case "in":
		return b.buildIN(nameRef, vals)
	case "size_eq":
		return fmt.Sprintf("size(%s) = %s", nameRef, b.ValueRef(vals[0]))
	case "size_gt":
		return fmt.Sprintf("size(%s) > %s", nameRef, b.ValueRef(vals[0]))
	case "size_gte":
		return fmt.Sprintf("size(%s) >= %s", nameRef, b.ValueRef(vals[0]))
	case "size_lt":
		return fmt.Sprintf("size(%s) < %s", nameRef, b.ValueRef(vals[0]))
	case "size_lte":
		return fmt.Sprintf("size(%s) <= %s", nameRef, b.ValueRef(vals[0]))
	default:
		panic("dkmigo/expr: unknown filter op: " + op)
	}
}

// JoinAND joins multiple expression clauses with " AND ".
func JoinAND(clauses []string) string {
	return strings.Join(clauses, " AND ")
}

// JoinOR joins multiple expression clauses with " OR ".
func JoinOR(clauses []string) string {
	return "(" + strings.Join(clauses, " OR ") + ")"
}

// Projection builds a ProjectionExpression from a list of attribute names.
func (b *Builder) Projection(attrs []string) string {
	refs := make([]string, len(attrs))
	for i, a := range attrs {
		refs[i] = b.NameRef(a)
	}
	return strings.Join(refs, ", ")
}

// UpdateExpression builds a complete UpdateExpression string from the provided
// update operations.
func (b *Builder) UpdateExpression(ops []UpdateClause) string {
	var setParts, removeParts, addParts, deleteParts []string

	for _, op := range ops {
		switch op.Action {
		case ActionSet:
			for attr, val := range op.Attrs {
				nameRef := b.NameRef(attr)
				setParts = append(setParts, nameRef+" = "+b.ValueRef(val))
			}
		case ActionAppend:
			for attr, val := range op.Attrs {
				nameRef := b.NameRef(attr)
				valRef := b.ValueRef(val)
				emptyListRef := b.ValueRef([]any{})
				setParts = append(setParts, fmt.Sprintf(
					"%s = list_append(if_not_exists(%s, %s), %s)",
					nameRef, nameRef, emptyListRef, valRef,
				))
			}
		case ActionRemove:
			for attr := range op.Attrs {
				removeParts = append(removeParts, b.NameRef(attr))
			}
		case ActionAdd:
			for attr, val := range op.Attrs {
				addParts = append(addParts, b.NameRef(attr)+" "+b.ValueRef(val))
			}
		case ActionDelete:
			for attr, val := range op.Attrs {
				deleteParts = append(deleteParts, b.NameRef(attr)+" "+b.ValueRef(val))
			}
		}
	}

	var parts []string
	if len(setParts) > 0 {
		parts = append(parts, "SET "+strings.Join(setParts, ", "))
	}
	if len(removeParts) > 0 {
		parts = append(parts, "REMOVE "+strings.Join(removeParts, ", "))
	}
	if len(addParts) > 0 {
		parts = append(parts, "ADD "+strings.Join(addParts, ", "))
	}
	if len(deleteParts) > 0 {
		parts = append(parts, "DELETE "+strings.Join(deleteParts, ", "))
	}

	return strings.Join(parts, " ")
}

// ----- update clause types -----

// UpdateAction is the DynamoDB update action keyword.
type UpdateAction int

const (
	ActionSet    UpdateAction = iota // SET attr = val
	ActionAppend                     // SET attr = list_append(...)
	ActionRemove                     // REMOVE attr
	ActionAdd                        // ADD attr val
	ActionDelete                     // DELETE attr val (for sets)
)

// UpdateClause represents one update action with its attributes.
type UpdateClause struct {
	Action UpdateAction
	Attrs  map[string]any // attr → value (value unused for REMOVE)
}

// ----- helpers -----

func (b *Builder) registerName(attr string) string {
	// Check if already registered.
	for placeholder, name := range b.names {
		if name == attr {
			return placeholder
		}
	}
	placeholder := fmt.Sprintf("#n%d", b.nameN)
	b.names[placeholder] = attr
	b.nameN++
	return placeholder
}

func (b *Builder) buildIN(nameRef string, vals []any) string {
	if len(vals) == 0 {
		return "false" // IN with empty set is always false
	}
	// vals[0] should be a slice; flatten it.
	var items []any
	switch v := vals[0].(type) {
	case []any:
		items = v
	case []string:
		for _, s := range v {
			items = append(items, s)
		}
	default:
		items = vals
	}

	refs := make([]string, len(items))
	for i, item := range items {
		refs[i] = b.ValueRef(item)
	}
	return nameRef + " IN (" + strings.Join(refs, ", ") + ")"
}

// splitPath splits a DynamoDB attribute path on "." boundaries,
// preserving list index suffixes (e.g. "a[0]" stays together).
func splitPath(path string) []string {
	var parts []string
	var cur strings.Builder
	depth := 0
	for _, c := range path {
		switch c {
		case '[':
			depth++
			cur.WriteRune(c)
		case ']':
			depth--
			cur.WriteRune(c)
		case '.':
			if depth == 0 {
				parts = append(parts, cur.String())
				cur.Reset()
			} else {
				cur.WriteRune(c)
			}
		default:
			cur.WriteRune(c)
		}
	}
	if cur.Len() > 0 {
		parts = append(parts, cur.String())
	}
	return parts
}

// splitListIndex separates "items[0]" into ("items", "[0]").
func splitListIndex(s string) (name, index string) {
	i := strings.IndexByte(s, '[')
	if i < 0 {
		return s, ""
	}
	return s[:i], s[i:]
}
