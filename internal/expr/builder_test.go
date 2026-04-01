package expr

import (
	"strings"
	"testing"
)

func TestNameRef_NonReserved(t *testing.T) {
	b := NewBuilder()
	ref := b.NameRef("email")
	// Non-reserved word should NOT be added to ExpressionAttributeNames.
	if ref != "email" {
		t.Errorf("NameRef(email) = %q; want %q", ref, "email")
	}
	if len(b.Names()) != 0 {
		t.Errorf("Names should be empty for non-reserved words, got %v", b.Names())
	}
}

func TestNameRef_Reserved(t *testing.T) {
	b := NewBuilder()
	ref := b.NameRef("status")
	if !strings.HasPrefix(ref, "#n") {
		t.Errorf("NameRef(status) = %q; want a #n placeholder", ref)
	}
	if b.Names()[ref] != "status" {
		t.Errorf("Names()[%s] = %q; want %q", ref, b.Names()[ref], "status")
	}
}

func TestNameRef_NestedPath(t *testing.T) {
	b := NewBuilder()
	ref := b.NameRef("address.city")
	// "address" is not reserved, "city" is not reserved → no escaping needed.
	if ref != "address.city" {
		t.Errorf("NameRef(address.city) = %q; want %q", ref, "address.city")
	}
}

func TestNameRef_NestedReserved(t *testing.T) {
	b := NewBuilder()
	// Both "order" and "status" are reserved → both parts must be escaped.
	ref := b.NameRef("order.status")
	parts := strings.Split(ref, ".")
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %v", parts)
	}
	if !strings.HasPrefix(parts[0], "#n") {
		t.Errorf("first part = %q; want a placeholder (order is reserved)", parts[0])
	}
	if !strings.HasPrefix(parts[1], "#n") {
		t.Errorf("second part = %q; want a placeholder (status is reserved)", parts[1])
	}
}

func TestValueRef(t *testing.T) {
	b := NewBuilder()
	ref1 := b.ValueRef("hello")
	ref2 := b.ValueRef(42)
	if ref1 == ref2 {
		t.Error("ValueRef returned same placeholder for different values")
	}
	if b.Values()[ref1] != "hello" {
		t.Errorf("Values()[%s] = %v; want %q", ref1, b.Values()[ref1], "hello")
	}
	if b.Values()[ref2] != 42 {
		t.Errorf("Values()[%s] = %v; want %d", ref2, b.Values()[ref2], 42)
	}
}

func TestKeyCondition_EQ(t *testing.T) {
	b := NewBuilder()
	expr := b.KeyCondition("order_id", "eq", "o1")
	if !strings.Contains(expr, " = ") {
		t.Errorf("EQ expression = %q; expected to contain ' = '", expr)
	}
}

func TestKeyCondition_Between(t *testing.T) {
	b := NewBuilder()
	expr := b.KeyCondition("order_id", "between", "a", "z")
	if !strings.Contains(expr, "BETWEEN") || !strings.Contains(expr, "AND") {
		t.Errorf("BETWEEN expression = %q; malformed", expr)
	}
}

func TestFilterExpr_IN(t *testing.T) {
	b := NewBuilder()
	expr := b.FilterExpr("status", "in", []any{"pending", "shipped"})
	if !strings.Contains(expr, " IN (") {
		t.Errorf("IN expression = %q; malformed", expr)
	}
}

func TestFilterExpr_Exists(t *testing.T) {
	b := NewBuilder()
	expr := b.FilterExpr("deleted_at", "exists")
	if !strings.Contains(expr, "attribute_exists") {
		t.Errorf("exists expression = %q; malformed", expr)
	}
}

func TestUpdateExpression_Set(t *testing.T) {
	b := NewBuilder()
	expr := b.UpdateExpression([]UpdateClause{
		{Action: ActionSet, Attrs: map[string]any{"status": "shipped"}},
	})
	if !strings.HasPrefix(expr, "SET ") {
		t.Errorf("update expression = %q; want SET prefix", expr)
	}
}

func TestUpdateExpression_MultiAction(t *testing.T) {
	b := NewBuilder()
	expr := b.UpdateExpression([]UpdateClause{
		{Action: ActionSet, Attrs: map[string]any{"status": "shipped"}},
		{Action: ActionRemove, Attrs: map[string]any{"temp": nil}},
		{Action: ActionAdd, Attrs: map[string]any{"views": 1}},
	})
	if !strings.Contains(expr, "SET ") {
		t.Errorf("expected SET in expression: %q", expr)
	}
	if !strings.Contains(expr, "REMOVE ") {
		t.Errorf("expected REMOVE in expression: %q", expr)
	}
	if !strings.Contains(expr, "ADD ") {
		t.Errorf("expected ADD in expression: %q", expr)
	}
}

func TestIsReserved(t *testing.T) {
	cases := []struct {
		word     string
		reserved bool
	}{
		{"status", true},
		{"STATUS", true},
		{"Status", true},
		{"email", false},
		{"total", true},
		{"name", true},
		{"userEmail", false},
	}
	for _, c := range cases {
		got := IsReserved(c.word)
		if got != c.reserved {
			t.Errorf("IsReserved(%q) = %v; want %v", c.word, got, c.reserved)
		}
	}
}
