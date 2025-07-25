# Design Document: DuckDB SQL Syntax Compatibility

## Overview

This design addresses SQL generation issues in the sequel-duckdb adapter where the adapter is generating non-standard SQL syntax that doesn't match Sequel's established conventions. The issues include unwanted ESCAPE clauses in LIKE statements, missing parentheses in complex expressions, incorrect table alias syntax, and wrong qualified column reference format.

The solution is to fix the adapter's SQL generation methods to produce clean, standard SQL that follows Sequel's patterns while being fully compatible with DuckDB.

## Architecture

### Design Philosophy

1. **Fix Root Causes**: Address SQL generation issues in the adapter rather than working around them in tests
2. **Standard SQL Generation**: Generate clean, standard SQL that follows Sequel's established patterns
3. **Minimal Adapter Changes**: Make targeted fixes to specific SQL generation methods

### Current Structure (Targeted Fixes)

```
lib/sequel/adapters/
├── duckdb.rb                    # Main adapter (unchanged)
└── shared/
    └── duckdb.rb               # DatasetMethods (fix SQL generation methods)

test/
├── sql_test.rb                 # Fix dataset creation issues
├── dataset_test.rb             # Tests should pass with fixed adapter
└── spec_helper.rb              # No changes needed
```

## Components and Interfaces

### 1. LIKE Clause Generation Fix

**Location**: `lib/sequel/adapters/shared/duckdb.rb` - DatasetMethods

Override LIKE handling to prevent unwanted ESCAPE clauses:

```ruby
def complex_expression_sql_append(sql, op, args)
  case op
  when :LIKE
    # Generate clean LIKE without ESCAPE clause
    sql << "("
    literal_append(sql, args[0])
    sql << " LIKE "
    literal_append(sql, args[1])
    sql << ")"
  when :ILIKE
    # Convert ILIKE to UPPER() LIKE UPPER() with proper parentheses
    sql << "(UPPER("
    literal_append(sql, args[0])
    sql << ") LIKE UPPER("
    literal_append(sql, args[1])
    sql << "))"
  when :~
    # Regex with proper parentheses
    sql << "("
    literal_append(sql, args[0])
    sql << " ~ "
    literal_append(sql, args[1])
    sql << ")"
  else
    super
  end
end
```

### 2. Table Alias Generation Fix

**Location**: `lib/sequel/adapters/shared/duckdb.rb` - DatasetMethods

Override alias handling to use standard AS syntax:

```ruby
def table_alias_sql_append(sql, table, alias_name)
  sql << table.to_s
  sql << " AS "
  sql << alias_name.to_s
end
```

### 3. Qualified Column Reference Fix

**Location**: `lib/sequel/adapters/shared/duckdb.rb` - DatasetMethods

Override qualified identifier handling to use dot notation:

```ruby
def qualified_identifier_sql_append(sql, table, column)
  sql << table.to_s
  sql << "."
  sql << column.to_s
end
```

### 4. SQL Test Infrastructure Fix

**Location**: `test/sql_test.rb`

Fix dataset creation issues in SQL tests:

```ruby
class SqlTest < SequelDuckDBTest::TestCase
  def setup
    super
    # Use proper mock dataset creation instead of subclasses
    @dataset = mock_dataset(:users)
  end

  def test_like_clause_generation
    dataset = @dataset.where(Sequel.like(:name, "%John%"))
    expected_sql = "SELECT * FROM users WHERE (name LIKE '%John%')"
    assert_sql expected_sql, dataset
  end

  def test_ilike_clause_generation
    dataset = @dataset.where(Sequel.ilike(:name, "%john%"))
    expected_sql = "SELECT * FROM users WHERE (UPPER(name) LIKE UPPER('%john%'))"
    assert_sql expected_sql, dataset
  end
end
```

## Data Models

No new data models needed. The existing adapter structure handles SQL generation correctly.

## Error Handling

Use existing Sequel error handling patterns. No special error handling needed for syntax variations since they are all valid SQL.

## Testing Strategy

### 1. Update Existing Tests
- Modify failing tests to accept DuckDB's valid syntax variations
- Use the new `assert_sql_matches_any` helper method
- Keep all existing test coverage

### 2. Integration Tests
- Ensure actual database operations work correctly
- Verify functional correctness over syntax exactness

### 3. Documentation Tests
- Document the syntax differences that are accepted
- Provide examples of valid variations

## Design Decisions and Rationales

### 1. Fix Adapter SQL Generation
**Decision**: Fix the root cause SQL generation issues in the adapter
**Rationale**: The adapter is generating non-standard SQL that doesn't follow Sequel conventions. Tests are correct to expect standard SQL.

### 2. Targeted Method Overrides
**Decision**: Override specific SQL generation methods in DatasetMethods
**Rationale**: Surgical fixes to specific issues without disrupting the overall adapter architecture.

### 3. Standard SQL Compliance
**Decision**: Generate SQL that follows standard SQL and Sequel conventions
**Rationale**: Ensures compatibility with existing Sequel patterns and makes the adapter more predictable.

### 4. Maintain Test Coverage
**Decision**: Keep all existing tests, fix the adapter to make them pass
**Rationale**: Tests are validating correct behavior; the adapter should conform to expected patterns.

## Implementation Phases

### Phase 1: Fix LIKE and Complex Expression Generation
- Override `complex_expression_sql_append` to fix LIKE, ILIKE, and regex generation
- Add proper parentheses to all complex expressions
- Remove unwanted ESCAPE clauses from LIKE statements

### Phase 2: Fix Table Alias Generation
- Override table alias methods to use standard `AS` syntax
- Ensure aliases work correctly in JOIN operations

### Phase 3: Fix Qualified Column References
- Override qualified identifier methods to use dot notation
- Ensure subqueries use proper column references

### Phase 4: Fix SQL Test Infrastructure
- Fix dataset creation issues in SQL tests
- Ensure tests use proper mock datasets

### Phase 5: Verification and Documentation
- Run all tests to ensure they pass with fixed adapter
- Document the SQL generation patterns used by the adapter

This design fixes the root causes of the SQL generation issues rather than working around them, resulting in a more robust and standards-compliant adapter.