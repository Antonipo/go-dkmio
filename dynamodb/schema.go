package dynamodb

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// tableSchema holds the parsed DynamoDB schema for a Go struct type T.
// It is derived from struct tags and IndexMap at Table.Bind() time.
type tableSchema struct {
	pkAttr string // DynamoDB attribute name for the partition key
	skAttr string // DynamoDB attribute name for the sort key (empty if none)
	ttlAttr string // DynamoDB attribute name for TTL (empty if none)

	// fieldByAttr maps DynamoDB attribute name → struct field index path.
	fieldByAttr map[string][]int

	// attrByField maps struct field index path string → DynamoDB attribute name.
	attrByField map[string]string

	// gsiPK / gsiSK: index-name → attribute name
	gsiPK map[string]string
	gsiSK map[string]string
}

var schemaCache sync.Map // reflect.Type → *tableSchema

// parseSchema extracts the tableSchema for T using struct field tags.
// Results are cached globally per type.
//
// Supported dkmio tags:
//
//	pk                        → partition key
//	sk                        → sort key
//	ttl                       → TTL attribute
//	gsi:<index-name>:pk       → GSI partition key
//	gsi:<index-name>:sk       → GSI sort key
//	lsi:<index-name>:sk       → LSI sort key
//	-                         → skip field (not mapped to DynamoDB)
//
// The DynamoDB attribute name defaults to the json tag name, then the
// lowercased field name.
func parseSchema(t reflect.Type) (*tableSchema, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("dkmigo/dynamodb: schema type must be a struct, got %s", t.Kind())
	}

	if cached, ok := schemaCache.Load(t); ok {
		return cached.(*tableSchema), nil
	}

	s := &tableSchema{
		fieldByAttr: make(map[string][]int),
		attrByField: make(map[string]string),
		gsiPK:       make(map[string]string),
		gsiSK:       make(map[string]string),
	}

	if err := walkFields(t, nil, s); err != nil {
		return nil, err
	}

	if s.pkAttr == "" {
		return nil, fmt.Errorf("dkmigo/dynamodb: type %s has no field tagged dkmio:\"pk\"", t.Name())
	}

	schemaCache.Store(t, s)
	return s, nil
}

func walkFields(t reflect.Type, indexPath []int, s *tableSchema) error {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		path := append(append([]int{}, indexPath...), i)

		// Skip unexported fields.
		if !field.IsExported() {
			continue
		}

		// Embedded structs: recurse.
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			if err := walkFields(field.Type, path, s); err != nil {
				return err
			}
			continue
		}

		attrName := resolveAttrName(field)
		if attrName == "-" {
			continue
		}

		s.fieldByAttr[attrName] = path
		s.attrByField[indexPathKey(path)] = attrName

		dkmioTag := field.Tag.Get("dkmio")
		if dkmioTag == "" {
			continue
		}

		for _, directive := range strings.Split(dkmioTag, ",") {
			directive = strings.TrimSpace(directive)
			switch {
			case directive == "pk":
				if s.pkAttr != "" {
					return fmt.Errorf("dkmigo/dynamodb: multiple pk fields in %s", t.Name())
				}
				s.pkAttr = attrName

			case directive == "sk":
				if s.skAttr != "" {
					return fmt.Errorf("dkmigo/dynamodb: multiple sk fields in %s", t.Name())
				}
				s.skAttr = attrName

			case directive == "ttl":
				s.ttlAttr = attrName

			case strings.HasPrefix(directive, "gsi:"):
				parts := strings.SplitN(directive, ":", 3)
				if len(parts) != 3 {
					return fmt.Errorf("dkmigo/dynamodb: malformed gsi tag %q on field %s", directive, field.Name)
				}
				indexName, role := parts[1], parts[2]
				switch role {
				case "pk":
					s.gsiPK[indexName] = attrName
				case "sk":
					s.gsiSK[indexName] = attrName
				default:
					return fmt.Errorf("dkmigo/dynamodb: unknown gsi role %q (must be pk or sk)", role)
				}

			case strings.HasPrefix(directive, "lsi:"):
				parts := strings.SplitN(directive, ":", 3)
				if len(parts) != 3 {
					return fmt.Errorf("dkmigo/dynamodb: malformed lsi tag %q on field %s", directive, field.Name)
				}
				indexName, role := parts[1], parts[2]
				if role != "sk" {
					return fmt.Errorf("dkmigo/dynamodb: LSI only supports role sk, got %q", role)
				}
				s.gsiSK[indexName] = attrName // LSIs share same map
			}
		}
	}
	return nil
}

// resolveAttrName returns the DynamoDB attribute name for a struct field.
// Priority: json tag → lowercased field name.
func resolveAttrName(field reflect.StructField) string {
	if jsonTag := field.Tag.Get("json"); jsonTag != "" {
		name, _, _ := strings.Cut(jsonTag, ",")
		if name != "" && name != "-" {
			return name
		}
		if name == "-" {
			return "-"
		}
	}
	// Default: lowercase field name.
	name := field.Name
	if len(name) == 0 {
		return name
	}
	b := []byte(name)
	if b[0] >= 'A' && b[0] <= 'Z' {
		b[0] += 'a' - 'A'
	}
	return string(b)
}

// indexPathKey converts a field index path to a map key string.
func indexPathKey(path []int) string {
	parts := make([]string, len(path))
	for i, p := range path {
		parts[i] = fmt.Sprintf("%d", p)
	}
	return strings.Join(parts, ".")
}
