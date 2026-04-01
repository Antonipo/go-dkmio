package dynamodb

import "context"

// ProjectionType defines what attributes an index projects.
type ProjectionType int

const (
	// ProjectionAll projects all table attributes (DynamoDB ALL projection).
	ProjectionAll ProjectionType = iota
	// ProjectionKeysOnly projects only key attributes.
	ProjectionKeysOnly
	// ProjectionInclude projects key attributes plus an explicit list.
	ProjectionInclude
)

// IndexProjection describes the projection configuration of a DynamoDB index.
type IndexProjection struct {
	Type  ProjectionType
	Attrs []string // Only used for ProjectionInclude
}

// ProjectionAllAttrs returns an IndexProjection that projects all attributes.
func ProjectionAllAttrs() IndexProjection { return IndexProjection{Type: ProjectionAll} }

// ProjectionKeysOnlyAttrs returns an IndexProjection that projects only keys.
func ProjectionKeysOnlyAttrs() IndexProjection { return IndexProjection{Type: ProjectionKeysOnly} }

// ProjectionIncludeAttrs returns an IndexProjection that includes specific attrs.
func ProjectionIncludeAttrs(attrs ...string) IndexProjection {
	return IndexProjection{Type: ProjectionInclude, Attrs: attrs}
}

// IndexDef describes an existing DynamoDB GSI or LSI.
// It does not create the index — it declares how dkmigo interacts with it.
//
// Key attribute names are resolved from struct tags (dkmio:"gsi:<name>:pk", etc.)
// unless PKAttr / SKAttr are provided explicitly here.
//
// Providing a Projection enables dkmigo to validate that attributes requested
// via Select are actually available in the index.
type IndexDef struct {
	// Name is the DynamoDB index name (e.g. "gsi-status-date").
	Name string

	// PKAttr overrides the struct-tag-derived partition key attribute name.
	PKAttr string

	// SKAttr overrides the struct-tag-derived sort key attribute name.
	SKAttr string

	// Projection describes the index's projection type.
	// Defaults to ProjectionAll if zero-valued.
	Projection IndexProjection
}

// IndexMap maps a user-chosen alias to an IndexDef.
// The alias is used to access the index via table.Index("alias").
//
//	dynamodb.IndexMap{
//	    "by_status": dynamodb.IndexDef{
//	        Name:       "gsi-status-date",
//	        Projection: dynamodb.ProjectionIncludeAttrs("total", "items_count"),
//	    },
//	}
type IndexMap map[string]IndexDef

// IndexTable provides query and scan access to a specific DynamoDB index.
// Obtain one via table.Index("alias").
type IndexTable[T any] struct {
	table  *Table[T]
	alias  string
	def    IndexDef
	pkAttr string
	skAttr string
}

// Query starts a QueryBuilder on this index using pkVal as the partition key.
// Chain Where, Filter, Select, Limit, etc. and call Exec to run.
//
//	result, err := orders.Index("by_status").Query(ctx, "shipped").
//	    Where(dynamodb.SKGTE("2024-01-01")).
//	    Exec()
func (ix *IndexTable[T]) Query(ctx context.Context, pkVal any) *QueryBuilder[T] {
	return newQueryBuilder(ix.table, pkVal).withContext(ctx).withIndex(ix)
}

// Scan starts a full scan on this index.
func (ix *IndexTable[T]) Scan(ctx context.Context) *QueryBuilder[T] {
	return newScanBuilder(ix.table).withContext(ctx).withIndex(ix)
}
