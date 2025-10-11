# Date Arithmetic Extension Implementation Plan

## Overview
Add support for Sequel's `date_arithmetic` extension to the DuckDB adapter, enabling database-independent date/timestamp interval arithmetic operations.

## Research Summary

### Sequel's date_arithmetic Extension
The extension provides two primary methods:
- `Sequel.date_add(expr, interval, opts)` - Adds interval to date/timestamp
- `Sequel.date_sub(expr, interval, opts)` - Subtracts interval from date/timestamp

**Supported interval units:**
- `years`, `months`, `weeks`, `days`, `hours`, `minutes`, `seconds`

**Input formats:**
1. Hash: `{years: 1, months: 2, days: 3}`
2. ActiveSupport::Duration: `1.year + 2.months + 3.days`

**Key features:**
- Database-independent API
- Optional cast type override via `:cast` option
- Weeks automatically converted to days (weeks × 7)
- SQL injection protection (rejects String values)
- Accumulates multiple values for same unit

### DuckDB's Interval Arithmetic

**Native interval syntax:**
```sql
-- Direct addition with INTERVAL keyword
DATE '2024-01-15' + INTERVAL 1 YEAR
DATE '2024-01-15' + INTERVAL 30 DAY

-- Multiple interval components
DATE '2024-01-15' + INTERVAL '1 year 2 months 3 days'

-- Dynamic intervals with expressions
DATE '2024-01-15' + INTERVAL (value) YEAR
```

**Important characteristics:**
1. Adding INTERVAL to DATE returns TIMESTAMP (even for day-only intervals)
2. Three basis units: months, days, microseconds
3. Supports both unit keywords and string literals
4. Parentheses required for variable/expression values

**Interval construction patterns:**
- Single unit: `INTERVAL 5 DAY`
- Multiple units in string: `INTERVAL '1 month 5 days 3 hours'`
- Expression-based: `INTERVAL (column_value) MONTH`

## Implementation Strategy

### 1. Add DuckDB-specific date_add_sql_append Method

**Location:** `lib/sequel/adapters/shared/duckdb.rb`

**Approach:** Follow the pattern established in Sequel's date_arithmetic extension by adding a `date_add_sql_append` method to `DatabaseMethods` module.

### 2. Implementation Pattern

The method should follow DuckDB's two supported approaches:

#### Option A: Multiple INTERVAL additions (Recommended)
Build expression by chaining interval additions:
```ruby
# For date_add(:created_at, years: 1, months: 2, days: 5)
# Generate: created_at + INTERVAL 1 YEAR + INTERVAL 2 MONTH + INTERVAL 5 DAY
```

**Advantages:**
- Simple, straightforward SQL generation
- Handles dynamic/expression values naturally
- Each interval component is explicit
- Follows DuckDB's native syntax

#### Option B: Single composite INTERVAL string
Build a single interval string with multiple components:
```ruby
# For date_add(:created_at, years: 1, months: 2, days: 5)
# Generate: created_at + INTERVAL '1 years 2 months 5 days'
```

**Challenges:**
- Requires literal values only (no expressions)
- More complex string building
- Less flexible for dynamic intervals

### 3. Unit Mapping

**Sequel → DuckDB unit mapping:**
```ruby
DUCKDB_DURATION_UNITS = {
  years: 'YEAR',
  months: 'MONTH',
  days: 'DAY',
  hours: 'HOUR',
  minutes: 'MINUTE',
  seconds: 'SECOND'
}.freeze
```

Note: Weeks already converted to days by `DateAdd` initialization.

### 4. Handling Cast Types

DuckDB returns TIMESTAMP when adding intervals to dates. Handle the `:cast` option:
- If `cast_type` specified: wrap result in `CAST(... AS cast_type)`
- Default cast_type: `Time` (TIMESTAMP)
- For date-only results: user can specify `cast: :date`

### 5. Value Handling

Support both literal values and SQL expressions:
- Numeric literals: `1`, `2.5` → direct interpolation
- SQL expressions: `Sequel.lit(...)`, column references → use parentheses

### 6. Implementation Code Structure

**Important:** No registration needed in sequel-duckdb! Sequel's date_arithmetic extension already handles registration (line 253 of date_arithmetic.rb):
```ruby
Dataset.register_extension(:date_arithmetic, SQL::DateAdd::DatasetMethods)
```

The default implementation checks for adapter overrides via `if defined?(super)` (line 94-96), so the DuckDB adapter just needs to provide `date_add_sql_append` in its `DatasetMethods`, and Sequel will automatically call it.

**Key Implementation Details:**

1. **DateAdd structure** (confirmed from source):
   - `da.expr` - the expression/column being added to
   - `da.interval` - Hash with symbol keys (e.g., `{hours: 5, minutes: -2}`)
   - `da.cast_type` - nil or a symbol (e.g., `:date`, `:timestamptz`)

2. **date_sub handling**: Sequel's `date_sub` automatically negates values before creating DateAdd:
   - Numeric values: negated directly (`hours: 5` → `hours: -5`)
   - Expressions: wrapped in negation (`Sequel::SQL::NumericExpression.new(:*, v, -1)`)
   - Adapter only needs to implement `date_add_sql_append` - negation is handled upstream

3. **Value types**:
   - `Numeric` (includes Integer, Float, BigDecimal) - can be used directly
   - Expressions (Sequel::LiteralString, Sequel::SQL::Expression, etc.) - need parentheses

4. **Sequel.lit with placeholders**: For SQL injection prevention, use array syntax:
   ```ruby
   Sequel.lit(["INTERVAL ", " HOUR"], value)  # Safe - value is parameterized
   # NOT: Sequel.lit("INTERVAL #{value} HOUR")  # Unsafe string interpolation
   ```

```ruby
module Sequel
  module DuckDB
    module DatabaseMethods
      # ... existing methods ...
    end

    # Dataset methods for DuckDB-specific SQL generation
    # These methods will be mixed into DuckDB::Database datasets
    module DatasetMethods
      DUCKDB_DURATION_UNITS = {
        years: 'YEAR',
        months: 'MONTH',
        days: 'DAY',
        hours: 'HOUR',
        minutes: 'MINUTE',
        seconds: 'SECOND'
      }.freeze

      # DuckDB-specific implementation of date arithmetic
      # This will be called by Sequel's date_arithmetic extension
      # via the `super` mechanism when the extension is loaded
      def date_add_sql_append(sql, da)
        expr = da.expr
        interval_hash = da.interval
        cast_type = da.cast_type

        # Build expression with chained interval additions
        result = expr
        interval_hash.each do |unit, value|
          sql_unit = DUCKDB_DURATION_UNITS[unit]
          next unless sql_unit

          # Create interval addition
          interval = build_interval_literal(value, sql_unit)
          result = Sequel.+(result, interval)
        end

        # Apply cast if specified or default to Time (TIMESTAMP)
        # Note: DuckDB returns TIMESTAMP when adding intervals to DATE
        result = Sequel.cast(result, cast_type || Time)

        literal_append(sql, result)
      end

      private

      def build_interval_literal(value, unit)
        # If value is numeric, use direct syntax with placeholder
        # If value is expression, wrap in parentheses with placeholder
        if value.is_a?(Numeric)
          # Direct numeric: INTERVAL 5 HOUR
          Sequel.lit(["INTERVAL ", " ", ""], value, unit)
        else
          # Expression: INTERVAL (column_name) HOUR
          # Note: expressions already include negation from date_sub
          Sequel.lit(["INTERVAL (", ") ", ""], value, unit)
        end
      end
    end
  end

  # Make DatasetMethods available to DuckDB datasets
  # This allows the date_add_sql_append override to be found
  DuckDB::Database.dataset_module(DuckDB::DatasetMethods)
end
```

## Testing Strategy

### Unit Tests Required

1. **Basic addition/subtraction:**
   - Single unit intervals (years, months, days, hours, minutes, seconds)
   - Multiple unit intervals combined
   - Verify weeks → days conversion

2. **Value types:**
   - Numeric literals
   - SQL expressions (column references, calculations)
   - Zero values (should be skipped)

3. **Cast handling:**
   - Default cast to timestamp
   - Explicit cast to date
   - Explicit cast to timestamptz

4. **ActiveSupport::Duration support:**
   - Verify `1.year + 2.months` syntax works
   - Mixed duration objects

5. **Edge cases:**
   - Empty intervals
   - Negative values (via date_sub)
   - Large values

### Integration Tests

Test against actual DuckDB database:
```ruby
DB.extension :date_arithmetic

# Test basic addition
result = DB[:events]
  .select(Sequel.date_add(:created_at, days: 5).as(:future_date))
  .first

# Test subtraction
result = DB[:events]
  .where(Sequel.date_sub(:expires_at, hours: 24) < Sequel::CURRENT_TIMESTAMP)
  .all

# Test complex intervals
result = DB[:events]
  .select(Sequel.date_add(:start_date, {years: 1, months: 2, days: 15}).as(:end_date))
  .first

# Test with cast option
result = DB[:events]
  .select(Sequel.date_add(:date_only, {days: 7}, cast: :date).as(:next_week))
  .first
```

## SQL Output Examples

**Input:**
```ruby
Sequel.date_add(:created_at, years: 1, months: 2, days: 5)
```

**Generated SQL:**
```sql
CAST(created_at + INTERVAL 1 YEAR + INTERVAL 2 MONTH + INTERVAL 5 DAY AS TIMESTAMP)
```

**Input:**
```ruby
Sequel.date_sub(:expires_at, hours: 12, minutes: 30)
```

**Generated SQL:**
```sql
CAST(expires_at + INTERVAL -12 HOUR + INTERVAL -30 MINUTE AS TIMESTAMP)
```

**Input:**
```ruby
Sequel.date_add(:start_date, {days: :duration_column}, cast: :date)
```

**Generated SQL:**
```sql
CAST(start_date + INTERVAL (duration_column) DAY AS DATE)
```

## Files to Modify

1. **`lib/sequel/adapters/shared/duckdb.rb`**
   - Add `DatasetMethods` module with `date_add_sql_append`
   - Add `DUCKDB_DURATION_UNITS` constant
   - Add `build_interval_literal` helper method
   - Use `DuckDB::Database.dataset_module(DuckDB::DatasetMethods)` to make methods available
   - NO registration needed - Sequel's extension handles it

2. **`spec/sequel/adapters/date_arithmetic_spec.rb`** (create new)
   - Unit tests for date_add_sql_append
   - Integration tests with actual database
   - Edge case coverage

3. **`README.md`** (optional documentation)
   - Add date_arithmetic to supported extensions list
   - Include usage examples

## Potential Issues & Considerations

1. **Type casting behavior:**
   - DuckDB returns TIMESTAMP when adding intervals to DATE
   - Users expecting DATE output need explicit `cast: :date`
   - Document this behavior clearly

2. **Expression vs literal handling:**
   - Literals can be used directly: `INTERVAL 5 DAY`
   - Expressions need parentheses: `INTERVAL (column) DAY`
   - Distinguish between Numeric and other values

3. **Interval order:**
   - Hash iteration order in Ruby 1.9+ is insertion order
   - DateAdd initializes with `Hash.new(0)` then converts to Hash
   - Order shouldn't matter for interval addition (commutative)

4. **PostgreSQL compatibility:**
   - DuckDB's interval syntax is similar to PostgreSQL
   - Can reference PostgreSQL implementation for patterns
   - Differences: DuckDB doesn't have make_interval function

5. **Extension loading:**
   - Extension must be loaded at Database level: `DB.extension :date_arithmetic`
   - Sequel's extension registers its `DatasetMethods` automatically
   - The `if defined?(super)` check in Sequel's implementation will call DuckDB's override
   - No registration needed in sequel-duckdb - just provide the override method

## Implementation Checklist

- [ ] Create `DatasetMethods` module in shared/duckdb.rb
- [ ] Implement `date_add_sql_append` method
- [ ] Add `DUCKDB_DURATION_UNITS` constant mapping
- [ ] Implement `build_interval_literal` helper
- [ ] Use `dataset_module` to make methods available (NO explicit registration needed)
- [ ] Write comprehensive specs
- [ ] Test with literal numeric values
- [ ] Test with SQL expression values
- [ ] Test cast option handling
- [ ] Test ActiveSupport::Duration integration
- [ ] Test date_sub (negative intervals)
- [ ] Verify all 6 interval units work
- [ ] Document any DuckDB-specific behaviors
- [ ] Update README if needed

## Success Criteria

1. All Sequel date_arithmetic API methods work correctly
2. Both numeric literals and SQL expressions supported as interval values
3. Cast option properly controls output type
4. ActiveSupport::Duration objects handled correctly
5. Generated SQL is clean and efficient
6. No SQL injection vulnerabilities
7. Comprehensive test coverage (>95%)
8. Consistent with other DuckDB adapter patterns
