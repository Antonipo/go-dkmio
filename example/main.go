// Package example demonstrates the complete dkmigo API.
// This file is intentionally not part of any test suite — it serves as
// living documentation of how to use the library.
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/Antonipo/go-dkmio"
	"github.com/Antonipo/go-dkmio/dynamodb"
)

// ----- Schema definition -----
// Define your structs using json tags for DynamoDB attribute names and
// dkmio tags to declare key roles.

// Order represents a single DynamoDB order item.
type Order struct {
	UserID    string  `json:"user_id" dkmio:"pk"`
	OrderID   string  `json:"order_id" dkmio:"sk"`
	Total     float64 `json:"total"`
	Status    string  `json:"status" dkmio:"gsi:gsi-status-date:pk"`
	CreatedAt string  `json:"created_at" dkmio:"gsi:gsi-status-date:sk,gsi:gsi-date:sk"`
	ExpiresAt int64   `json:"ttl" dkmio:"ttl"`
}

// User represents a DynamoDB user item (no sort key).
type User struct {
	UserID string `json:"user_id" dkmio:"pk"`
	Name   string `json:"name"`
	Email  string `json:"email"`
}

// ----- Table descriptors (declare at package level) -----
// These describe existing DynamoDB tables — dkmigo never creates infrastructure.

var OrdersTable = dynamodb.Table[Order]{
	Name: "orders",
	Indexes: dynamodb.IndexMap{
		// Alias → IndexDef. The alias is used in code: orders.Index("by_status")
		"by_status": dynamodb.IndexDef{
			Name:       "gsi-status-date",
			Projection: dynamodb.ProjectionIncludeAttrs("total", "created_at"),
		},
		"by_date": dynamodb.IndexDef{
			Name:       "gsi-date",
			Projection: dynamodb.ProjectionAllAttrs(),
		},
	},
}

var UsersTable = dynamodb.Table[User]{Name: "users"}

func main() {
	ctx := context.Background()

	// ── 1. Root client ──────────────────────────────────────────────────────
	root, err := dkmigo.New(dkmigo.Config{
		Region: "us-east-1",
		// For local DynamoDB: EndpointURL: "http://localhost:8000"
		CircuitBreaker: &dkmigo.CircuitBreakerConfig{
			FailureThreshold: 5,
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	// ── 2. DynamoDB client ──────────────────────────────────────────────────
	dynamo, err := dynamodb.New(root)
	if err != nil {
		log.Fatal(err)
	}

	// ── 3. Bind table descriptors to the client ─────────────────────────────
	// Do this once at startup — the bound *Table is safe for concurrent use.
	orders := OrdersTable.Bind(dynamo)
	users := UsersTable.Bind(dynamo)

	// ── 4. GET ──────────────────────────────────────────────────────────────
	order, err := orders.Get(ctx, dynamodb.Keys{"user_id": "u1", "order_id": "o1"})
	if err != nil {
		log.Fatal(err)
	}
	if order == nil {
		fmt.Println("order not found")
	} else {
		fmt.Printf("order: %+v\n", *order)
	}

	// Get with options
	order, err = orders.Get(ctx, dynamodb.Keys{"user_id": "u1", "order_id": "o1"},
		dynamodb.WithConsistentRead(),
		dynamodb.WithProjection("total", "status"),
	)
	if err != nil {
		log.Fatal(err)
	}

	// ── 5. PUT ──────────────────────────────────────────────────────────────
	err = orders.Put(ctx, Order{
		UserID:  "u1",
		OrderID: "o2",
		Total:   99.99,
		Status:  "pending",
	})
	if err != nil {
		log.Fatal(err)
	}

	// Conditional put — fail if item already exists
	err = orders.Put(ctx, Order{UserID: "u1", OrderID: "o3", Total: 10},
		dynamodb.WithCondition(dynamodb.NotExists("user_id")),
	)
	if err != nil {
		var condErr *dynamodb.ConditionError
		if ok := isConditionError(err, &condErr); ok {
			fmt.Println("item already exists, skipping")
		} else {
			log.Fatal(err)
		}
	}

	// ── 6. UPDATE ───────────────────────────────────────────────────────────
	updated, err := orders.Update(ctx,
		dynamodb.Keys{"user_id": "u1", "order_id": "o2"},
		[]dynamodb.UpdateOp{
			dynamodb.Set(map[string]any{
				"status":      "shipped",
				"shipped_at":  "2024-06-01",
			}),
			dynamodb.Add(map[string]any{"view_count": 1}),
			dynamodb.Append(map[string]any{"events": []string{"shipped"}}),
			dynamodb.Remove("temp_field"),
		},
		dynamodb.ReturnAllNew(),
	)
	if err != nil {
		log.Fatal(err)
	}
	if updated != nil {
		fmt.Printf("updated order: %+v\n", *updated)
	}

	// Conditional update
	_, err = orders.Update(ctx,
		dynamodb.Keys{"user_id": "u1", "order_id": "o2"},
		[]dynamodb.UpdateOp{dynamodb.Set(map[string]any{"status": "delivered"})},
		dynamodb.WithCondition(dynamodb.EQ("status", "shipped")),
	)
	if err != nil {
		log.Fatal(err)
	}

	// ── 7. DELETE ───────────────────────────────────────────────────────────
	err = orders.Delete(ctx, dynamodb.Keys{"user_id": "u1", "order_id": "o2"})
	if err != nil {
		log.Fatal(err)
	}

	// ── 8. QUERY ────────────────────────────────────────────────────────────
	result, err := orders.Query(ctx, "u1").
		Where(dynamodb.SKGTE("o_100")).
		Filter(
			dynamodb.EQ("status", "shipped"),
			dynamodb.GTE("total", 50.0),
		).
		Select("order_id", "total", "status").
		Limit(10).
		ScanForward(false). // descending
		Exec()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("found %d orders\n", result.Count)
	for _, o := range result.Items {
		fmt.Printf("  %s → %.2f\n", o.OrderID, o.Total)
	}

	// Pagination
	if result.HasMore() {
		nextPage, err := orders.Query(ctx, "u1").
			StartFrom(result.LastKey).
			Exec()
		if err != nil {
			log.Fatal(err)
		}
		_ = nextPage
	}

	// Fetch all pages at once (with auto-pagination)
	allOrders, err := orders.Query(ctx, "u1").FetchAll(0)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("total orders: %d\n", len(allOrders))

	// Count
	count, err := orders.Query(ctx, "u1").Filter(dynamodb.EQ("status", "shipped")).Count()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("shipped orders: %d\n", count)

	// ── 9. SCAN ─────────────────────────────────────────────────────────────
	scanResult, err := orders.Scan(ctx).
		Filter(dynamodb.EQ("status", "pending")).
		Limit(50).
		Exec()
	if err != nil {
		log.Fatal(err)
	}
	_ = scanResult

	// ── 10. INDEX QUERY ─────────────────────────────────────────────────────
	// Query a GSI by its alias (defined in Indexes at declaration time).
	statusResult, err := orders.Index("by_status").
		Query(ctx, "shipped").
		Where(dynamodb.SKGTE("2024-01-01")).
		Select("order_id", "total").
		Exec()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("shipped since 2024: %d\n", statusResult.Count)

	// ── 11. BATCH GET ────────────────────────────────────────────────────────
	items, err := orders.BatchGet(ctx, []dynamodb.Keys{
		{"user_id": "u1", "order_id": "o1"},
		{"user_id": "u1", "order_id": "o2"},
		{"user_id": "u2", "order_id": "o3"},
	})
	if err != nil {
		log.Fatal(err)
	}
	for i, item := range items {
		if item == nil {
			fmt.Printf("item %d: not found\n", i)
		} else {
			fmt.Printf("item %d: %+v\n", i, *item)
		}
	}

	// ── 12. BATCH WRITE ──────────────────────────────────────────────────────
	err = orders.BatchWrite(ctx).
		Put(
			Order{UserID: "u3", OrderID: "o1", Total: 10},
			Order{UserID: "u3", OrderID: "o2", Total: 20},
		).
		Delete("u3", "o_old").
		Exec()
	if err != nil {
		log.Fatal(err)
	}

	// ── 13. WRITE TRANSACTION ────────────────────────────────────────────────
	err = dynamodb.WriteTransaction(dynamo, ctx, func(tx *dynamodb.TxWriter) error {
		// Put an order
		if err := dynamodb.TxPut(tx, orders, Order{
			UserID:  "u4",
			OrderID: "o1",
			Total:   50,
			Status:  "pending",
		}); err != nil {
			return err
		}

		// Update inventory atomically (different table)
		if err := dynamodb.TxUpdate(tx, users,
			dynamodb.Keys{"user_id": "u4"},
			[]dynamodb.UpdateOp{dynamodb.Add(map[string]any{"order_count": 1})},
		); err != nil {
			return err
		}

		// Condition check — abort if user is inactive
		if err := dynamodb.TxConditionCheck(tx, users, "u4", nil,
			dynamodb.EQ("status", "active"),
		); err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	// ── 14. READ TRANSACTION ─────────────────────────────────────────────────
	txResult, err := dynamodb.ReadTransaction(dynamo, ctx, func(tx *dynamodb.TxReader) error {
		if _, err := dynamodb.TxGetTyped(tx, orders, "u1", "o1"); err != nil {
			return err
		}
		if _, err := dynamodb.TxGetTyped(tx, users, "u1"); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}

	txOrder, err := dynamodb.TxGetResult[Order](txResult, 0)
	if err != nil {
		log.Fatal(err)
	}
	txUser, err := dynamodb.TxGetResult[User](txResult, 1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("tx order: %v, tx user: %v\n", txOrder, txUser)

	// ── 15. CIRCUIT BREAKER ──────────────────────────────────────────────────
	fmt.Println("circuit breaker state:", root.CircuitBreakerState())
}

// isConditionError is a helper for the example — in production code use errors.As directly.
func isConditionError(err error, target **dynamodb.ConditionError) bool {
	if err == nil {
		return false
	}
	if ce, ok := err.(*dynamodb.ConditionError); ok {
		*target = ce
		return true
	}
	return false
}

// ----- Using TableClient interface for testable code -----

// OrderRepository demonstrates how to use the TableClient interface
// so your business logic can be tested with mock tables.
type OrderRepository struct {
	table dynamodb.TableClient[Order]
}

func NewOrderRepository(orders *dynamodb.Table[Order]) *OrderRepository {
	return &OrderRepository{table: orders}
}

func (r *OrderRepository) GetOrder(ctx context.Context, userID, orderID string) (*Order, error) {
	return r.table.Get(ctx, dynamodb.Keys{"user_id": userID, "order_id": orderID})
}

func (r *OrderRepository) ListOrders(ctx context.Context, userID string) ([]Order, error) {
	return r.table.Query(ctx, userID).FetchAll(0)
}

func (r *OrderRepository) CreateOrder(ctx context.Context, o Order) error {
	return r.table.Put(ctx, o,
		dynamodb.WithCondition(dynamodb.NotExists("user_id")),
	)
}

// In tests, inject a mock:
//
//	type mockTable struct { ... }
//	func (m *mockTable) Get(...) (*Order, error) { ... }
//	// implement remaining TableClient[Order] methods
//
//	repo := &OrderRepository{table: &mockTable{}}
