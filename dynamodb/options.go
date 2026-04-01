package dynamodb

// ----- GetOption -----

type getConfig struct {
	projection []string
	consistent bool
}

// GetOption configures a Get or BatchGet operation.
type GetOption func(*getConfig)

// WithConsistentRead enables strongly consistent reads.
func WithConsistentRead() GetOption {
	return func(c *getConfig) { c.consistent = true }
}

// WithProjection limits which attributes are returned.
func WithProjection(attrs ...string) GetOption {
	return func(c *getConfig) { c.projection = attrs }
}

func applyGetOptions(opts []GetOption) getConfig {
	c := getConfig{}
	for _, o := range opts {
		o(&c)
	}
	return c
}

// ----- WriteOption -----

type writeConfig struct {
	conditions   []Condition
	returnValues string // "", "ALL_OLD", "ALL_NEW", "UPDATED_OLD", "UPDATED_NEW"
}

// WriteOption configures a Put, Update, or Delete operation.
type WriteOption func(*writeConfig)

// WithCondition adds AND-joined condition expressions to a write operation.
// The write is aborted and ConditionError is returned if the condition fails.
func WithCondition(conds ...Condition) WriteOption {
	return func(c *writeConfig) { c.conditions = append(c.conditions, conds...) }
}

// ReturnAllOld returns the item's attributes as they were before the operation.
// Supported by Put (returns old item), Update, Delete.
func ReturnAllOld() WriteOption {
	return func(c *writeConfig) { c.returnValues = "ALL_OLD" }
}

// ReturnAllNew returns the item's attributes after the update.
// Only valid for Update.
func ReturnAllNew() WriteOption {
	return func(c *writeConfig) { c.returnValues = "ALL_NEW" }
}

// ReturnUpdatedNew returns only the updated attributes after the update.
// Only valid for Update.
func ReturnUpdatedNew() WriteOption {
	return func(c *writeConfig) { c.returnValues = "UPDATED_NEW" }
}

// ReturnUpdatedOld returns only the updated attributes before the update.
// Only valid for Update.
func ReturnUpdatedOld() WriteOption {
	return func(c *writeConfig) { c.returnValues = "UPDATED_OLD" }
}

func applyWriteOptions(opts []WriteOption) writeConfig {
	c := writeConfig{}
	for _, o := range opts {
		o(&c)
	}
	return c
}
