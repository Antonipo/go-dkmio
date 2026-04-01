# go-dkmio

A lightweight, type-safe DynamoDB Object-Key Mapper (OKM) for Go, built on the AWS SDK v2.

[![CI](https://github.com/Antonipo/go-dkmio/actions/workflows/ci.yml/badge.svg)](https://github.com/Antonipo/go-dkmio/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/Antonipo/go-dkmio.svg)](https://pkg.go.dev/github.com/Antonipo/go-dkmio)
[![Go Report Card](https://goreportcard.com/badge/github.com/Antonipo/go-dkmio)](https://goreportcard.com/report/github.com/Antonipo/go-dkmio)

## Features

- **Generics-based API** — `Table[T]` returns `*T`, never `map[string]any`
- **Struct tags** — declare keys, indexes, and TTL directly on your structs
- **Fluent query builder** — chain `Where`, `Filter`, `Limit`, `Select`, and paginate with `FetchAll`
- **Automatic expression building** — reserved words are escaped transparently
- **Batch and transaction support** — `BatchGet`, `BatchWrite`, `WriteTransaction`, `ReadTransaction`
- **Circuit breaker** — wraps [sony/gobreaker](https://github.com/sony/gobreaker), only trips on infrastructure errors
- **Testable** — `TableClient[T]` interface makes mocking trivial

## Installation

```bash
go get github.com/Antonipo/go-dkmio
```

Requires Go 1.21+.

## Quick start

### 1. Define your structs

Use `json` tags for DynamoDB attribute names and `dkmio` tags to declare key roles:

```go
type Order struct {
    UserID    string  `json:"user_id"    dkmio:"pk"`
    OrderID   string  `json:"order_id"   dkmio:"sk"`
    Status    string  `json:"status"     dkmio:"gsi:gsi-status-date:pk"`
    CreatedAt string  `json:"created_at" dkmio:"gsi:gsi-status-date:sk"`
    Total     float64 `json:"total"`
    ExpiresAt int64   `json:"ttl"        dkmio:"ttl"`
}
```

Supported `dkmio` directives:

| Tag | Meaning |
|---|---|
| `dkmio:"pk"` | Partition key |
| `dkmio:"sk"` | Sort key |
| `dkmio:"ttl"` | TTL attribute |
| `dkmio:"gsi:<name>:pk"` | GSI partition key |
| `dkmio:"gsi:<name>:sk"` | GSI sort key |
| `dkmio:"lsi:<name>:sk"` | LSI sort key |

### 2. Create a client and bind tables

```go
import (
    "github.com/Antonipo/go-dkmio"
    "github.com/Antonipo/go-dkmio/dynamodb"
)

// Declare table descriptors at package level (no infrastructure created).
var OrdersTable = dynamodb.Table[Order]{
    Name: "orders",
    Indexes: dynamodb.IndexMap{
        "by_status": dynamodb.IndexDef{
            Name:       "gsi-status-date",
            Projection: dynamodb.ProjectionIncludeAttrs("total", "created_at"),
        },
    },
}

func main() {
    root, err := dkmigo.New(dkmigo.Config{
        Region: "us-east-1",
        // EndpointURL: "http://localhost:8000", // DynamoDB Local
        CircuitBreaker: &dkmigo.CircuitBreakerConfig{
            FailureThreshold: 5,
            RecoveryTimeout:  30 * time.Second,
        },
    })

    dynamo, err := dynamodb.New(root)
    orders := OrdersTable.Bind(dynamo)
}
```

### 3. CRUD operations

```go
ctx := context.Background()

// Get
item, err := orders.Get(ctx, dynamodb.Keys{"user_id": "u1", "order_id": "o1"})

// Put
err = orders.Put(ctx, Order{UserID: "u1", OrderID: "o1", Status: "pending", Total: 59.99})

// Update
updated, err := orders.Update(ctx,
    dynamodb.Keys{"user_id": "u1", "order_id": "o1"},
    []dynamodb.UpdateOp{
        dynamodb.Set(map[string]any{"status": "shipped"}),
    },
    dynamodb.WithCondition(dynamodb.EQ("status", "pending")),
    dynamodb.ReturnAllNew(),
)

// Delete
err = orders.Delete(ctx, dynamodb.Keys{"user_id": "u1", "order_id": "o1"})
```

### 4. Query and Scan

```go
// Query with sort key condition and filter
result, err := orders.Query(ctx, "u1").
    Where(dynamodb.SKBeginsWith("2024-")).
    Filter(dynamodb.GTE("total", 100)).
    Limit(20).
    ScanForward(false).
    Exec()

for _, item := range result.Items { ... }

// Paginate through all results
allItems, err := orders.Query(ctx, "u1").FetchAll()

// Scan
result, err := orders.Scan(ctx).Filter(dynamodb.EQ("status", "shipped")).Exec()
```

### 5. GSI queries

```go
result, err := orders.Index("by_status").
    Query(ctx, "shipped").
    Where(dynamodb.SKGTE("2024-01-01")).
    Select("total", "created_at").
    Exec()
```

### 6. Batch operations

```go
// BatchGet — missing items appear as nil at the same index position
items, err := orders.BatchGet(ctx, []dynamodb.Keys{
    {"user_id": "u1", "order_id": "o1"},
    {"user_id": "u1", "order_id": "o2"},
})

// BatchWrite
err = orders.BatchWrite(ctx).
    Put(order1, order2).
    Delete("u1", "o_old").
    Exec()
```

### 7. Transactions

```go
// Write transaction
err = dynamodb.WriteTransaction(dynamo, ctx, func(tx *dynamodb.TxWriter) error {
    if err := dynamodb.TxPut(tx, orders, newOrder); err != nil {
        return err
    }
    return dynamodb.TxUpdate(tx, users,
        dynamodb.Keys{"user_id": "u1"},
        []dynamodb.UpdateOp{dynamodb.Add(map[string]any{"order_count": 1})},
    )
})

// Read transaction
result, err := dynamodb.ReadTransaction(dynamo, ctx, func(tx *dynamodb.TxReader) error {
    dynamodb.TxGetTyped(tx, orders, "u1", "o1")
    dynamodb.TxGetTyped(tx, users, "u1")
    return nil
})
order, err := dynamodb.TxGetResult[Order](result, 0)
user,  err := dynamodb.TxGetResult[User](result, 1)
```

## Testing your code

`TableClient[T]` is an interface — inject it in your structs so you can swap in a mock:

```go
type OrderRepo struct {
    table dynamodb.TableClient[Order]
}

// In tests, pass a mock implementation of TableClient[Order].
```

## Local development with DynamoDB Local

```go
root, _ := dkmigo.New(dkmigo.Config{
    Region:          "us-east-1",
    EndpointURL:     "http://localhost:8000",
    AccessKeyID:     "local",
    SecretAccessKey: "local",
})
```

## Available conditions

| Constructor | DynamoDB equivalent |
|---|---|
| `EQ(attr, val)` | `= :v` |
| `NEQ(attr, val)` | `<> :v` |
| `GT / GTE / LT / LTE` | `> >= < <=` |
| `Between(attr, lo, hi)` | `BETWEEN :lo AND :hi` |
| `BeginsWith(attr, prefix)` | `begins_with(attr, :v)` |
| `Contains / NotContains` | `contains / not contains` |
| `Exists / NotExists` | `attribute_exists / attribute_not_exists` |
| `AttrType(attr, type)` | `attribute_type(attr, :t)` |
| `In(attr, vals...)` | `attr IN (:v0, :v1, ...)` |
| `SizeEQ / SizeGT / ...` | `size(attr) = / > ...` |
| `SKEQ / SKGT / SKBetween / SKBeginsWith / ...` | Sort key variants of the above |

## License

Apache 2.0 — see [LICENSE](LICENSE).
