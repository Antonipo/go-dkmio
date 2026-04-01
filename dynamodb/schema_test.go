package dynamodb

import (
	"reflect"
	"testing"
)

// ----- test structs -----

type orderBasic struct {
	UserID  string  `json:"user_id" dkmio:"pk"`
	OrderID string  `json:"order_id" dkmio:"sk"`
	Total   float64 `json:"total"`
	Status  string  `json:"status"`
}

type userNoPK struct {
	Name string `json:"name"`
}

type orderWithGSI struct {
	UserID    string `json:"user_id" dkmio:"pk"`
	OrderID   string `json:"order_id" dkmio:"sk"`
	Status    string `json:"status" dkmio:"gsi:gsi-status-date:pk"`
	CreatedAt string `json:"created_at" dkmio:"gsi:gsi-status-date:sk"`
	ExpiresAt int64  `json:"ttl" dkmio:"ttl"`
}

type orderMultipleTags struct {
	UserID    string `json:"user_id" dkmio:"pk,gsi:gsi-date:pk"`
	OrderID   string `json:"order_id" dkmio:"sk"`
	CreatedAt string `json:"created_at" dkmio:"gsi:gsi-date:sk"`
}

// ----- tests -----

func TestParseSchema_Basic(t *testing.T) {
	s, err := parseSchema(reflect.TypeOf(orderBasic{}))
	if err != nil {
		t.Fatal(err)
	}

	if s.pkAttr != "user_id" {
		t.Errorf("pkAttr = %q; want %q", s.pkAttr, "user_id")
	}
	if s.skAttr != "order_id" {
		t.Errorf("skAttr = %q; want %q", s.skAttr, "order_id")
	}
	if s.ttlAttr != "" {
		t.Errorf("ttlAttr = %q; want empty", s.ttlAttr)
	}
}

func TestParseSchema_NoPK(t *testing.T) {
	_, err := parseSchema(reflect.TypeOf(userNoPK{}))
	if err == nil {
		t.Fatal("expected error for missing pk tag, got nil")
	}
}

func TestParseSchema_GSI(t *testing.T) {
	s, err := parseSchema(reflect.TypeOf(orderWithGSI{}))
	if err != nil {
		t.Fatal(err)
	}

	if s.gsiPK["gsi-status-date"] != "status" {
		t.Errorf("gsiPK[gsi-status-date] = %q; want %q", s.gsiPK["gsi-status-date"], "status")
	}
	if s.gsiSK["gsi-status-date"] != "created_at" {
		t.Errorf("gsiSK[gsi-status-date] = %q; want %q", s.gsiSK["gsi-status-date"], "created_at")
	}
	if s.ttlAttr != "ttl" {
		t.Errorf("ttlAttr = %q; want %q", s.ttlAttr, "ttl")
	}
}

func TestParseSchema_MultipleTagDirectives(t *testing.T) {
	s, err := parseSchema(reflect.TypeOf(orderMultipleTags{}))
	if err != nil {
		t.Fatal(err)
	}

	if s.pkAttr != "user_id" {
		t.Errorf("pkAttr = %q; want %q", s.pkAttr, "user_id")
	}
	if s.gsiPK["gsi-date"] != "user_id" {
		t.Errorf("gsiPK[gsi-date] = %q; want %q", s.gsiPK["gsi-date"], "user_id")
	}
	if s.gsiSK["gsi-date"] != "created_at" {
		t.Errorf("gsiSK[gsi-date] = %q; want %q", s.gsiSK["gsi-date"], "created_at")
	}
}

func TestParseSchema_CachedOnSecondCall(t *testing.T) {
	// Two calls must return the exact same pointer (from cache).
	s1, err := parseSchema(reflect.TypeOf(orderBasic{}))
	if err != nil {
		t.Fatal(err)
	}
	s2, err := parseSchema(reflect.TypeOf(orderBasic{}))
	if err != nil {
		t.Fatal(err)
	}
	if s1 != s2 {
		t.Error("expected cached schema pointer, got different pointers")
	}
}

func TestParseSchema_PointerType(t *testing.T) {
	// parseSchema should unwrap pointer types transparently.
	s, err := parseSchema(reflect.TypeOf(&orderBasic{}))
	if err != nil {
		t.Fatal(err)
	}
	if s.pkAttr != "user_id" {
		t.Errorf("pkAttr = %q; want user_id", s.pkAttr)
	}
}

func TestParseSchema_NonStruct_ReturnsError(t *testing.T) {
	_, err := parseSchema(reflect.TypeOf("string"))
	if err == nil {
		t.Fatal("expected error for non-struct type, got nil")
	}
}

type EmbeddedBase struct {
	UserID string `json:"user_id" dkmio:"pk"`
}

type orderWithEmbedded struct {
	EmbeddedBase
	OrderID string `json:"order_id" dkmio:"sk"`
}

func TestParseSchema_EmbeddedStruct(t *testing.T) {
	s, err := parseSchema(reflect.TypeOf(orderWithEmbedded{}))
	if err != nil {
		t.Fatal(err)
	}
	if s.pkAttr != "user_id" {
		t.Errorf("pkAttr = %q; want user_id (from embedded)", s.pkAttr)
	}
	if s.skAttr != "order_id" {
		t.Errorf("skAttr = %q; want order_id", s.skAttr)
	}
}

type orderJsonDash struct {
	UserID  string `json:"user_id" dkmio:"pk"`
	OrderID string `json:"order_id" dkmio:"sk"`
	Hidden  string `json:"-"`
}

func TestParseSchema_JsonDashFieldSkipped(t *testing.T) {
	s, err := parseSchema(reflect.TypeOf(orderJsonDash{}))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.fieldByAttr["-"]; ok {
		t.Error("field with json:\"-\" should be skipped")
	}
}

type orderWithLSI struct {
	UserID    string `json:"user_id" dkmio:"pk"`
	OrderID   string `json:"order_id" dkmio:"sk"`
	CreatedAt string `json:"created_at" dkmio:"lsi:lsi-date:sk"`
}

func TestParseSchema_LSI(t *testing.T) {
	s, err := parseSchema(reflect.TypeOf(orderWithLSI{}))
	if err != nil {
		t.Fatal(err)
	}
	if s.gsiSK["lsi-date"] != "created_at" {
		t.Errorf("gsiSK[lsi-date] = %q; want created_at", s.gsiSK["lsi-date"])
	}
}

type orderMultiplePK struct {
	UserID  string `json:"user_id" dkmio:"pk"`
	OrderID string `json:"order_id" dkmio:"pk"`
}

func TestParseSchema_MultiplePK_ReturnsError(t *testing.T) {
	_, err := parseSchema(reflect.TypeOf(orderMultiplePK{}))
	if err == nil {
		t.Fatal("expected error for multiple pk fields, got nil")
	}
}

type orderMalformedGSI struct {
	UserID  string `json:"user_id" dkmio:"pk"`
	OrderID string `json:"order_id" dkmio:"gsi:only_two_parts"`
}

func TestParseSchema_MalformedGSI_ReturnsError(t *testing.T) {
	_, err := parseSchema(reflect.TypeOf(orderMalformedGSI{}))
	if err == nil {
		t.Fatal("expected error for malformed gsi tag, got nil")
	}
}

type orderBadGSIRole struct {
	UserID  string `json:"user_id" dkmio:"pk"`
	OrderID string `json:"order_id" dkmio:"gsi:my-index:bad_role"`
}

func TestParseSchema_UnknownGSIRole_ReturnsError(t *testing.T) {
	_, err := parseSchema(reflect.TypeOf(orderBadGSIRole{}))
	if err == nil {
		t.Fatal("expected error for unknown gsi role, got nil")
	}
}

type orderBadLSIRole struct {
	UserID  string `json:"user_id" dkmio:"pk"`
	OrderID string `json:"order_id" dkmio:"lsi:my-lsi:pk"`
}

func TestParseSchema_LSIWrongRole_ReturnsError(t *testing.T) {
	_, err := parseSchema(reflect.TypeOf(orderBadLSIRole{}))
	if err == nil {
		t.Fatal("expected error for lsi with pk role, got nil")
	}
}

type orderNoJsonTag struct {
	UserID  string `dkmio:"pk"`
	OrderID string `dkmio:"sk"`
}

func TestParseSchema_NoJsonTag_UsesFieldName(t *testing.T) {
	// resolveAttrName falls back to lowercase field name when no json tag.
	s, err := parseSchema(reflect.TypeOf(orderNoJsonTag{}))
	if err != nil {
		t.Fatal(err)
	}
	// Without json tag the attr name is derived from the field name (lowercased).
	if s.pkAttr == "" {
		t.Error("expected pkAttr to be set even without json tag")
	}
}
