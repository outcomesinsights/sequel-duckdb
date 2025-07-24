# Design Document: SQL Expression Handling Fix

## Overview

This design addresses critical SQL expression handling issues in the sequel-duckdb adapter where SQL expressions, functions, and literal strings are incorrectly treated as regular string literals and quoted when they should be rendered as raw SQL. The fix involves implementing proper type detection and handling in the `literal_append` method to distinguish between different SQL object types.

The core issue is that the current adapter implementation doesn't properly handle Sequel's expression objects (`Sequel::LiteralString`, `Sequel::SQL::Function`, etc.) and treats them as regular Ruby strings, causing them to be quoted inappropriately in the generated SQL.

## Architecture

### Current Problem

The existing `literal_append` method in the DuckDB adapter handles `String` objects without first checking if they are `Sequel::LiteralString` objects. This causes `LiteralString` objects (created by `Sequel.lit()`) to be treated as regular strings and quoted inappropriately. The method correctly handles `Time` and `DateTime` objects but falls through to `literal_string_append` for all `String` objects, including `LiteralString`.

### Solution Architecture

The solution follows Sequel core's established pattern by checking for `LiteralString` as a special case of `String` before applying string quoting:

1. **Sequel Core Pattern Compliance**: Follows the exact pattern used in Sequel core's `literal_append` method
2. **LiteralString Special Handling**: Checks for `LiteralString` before regular `String` processing
3. **Minimal Change**: Only adds the missing `LiteralString` check to existing logic
4. **Parent Delegation**: Continues to delegate `SQL::Function` and other expressions to parent class

### Design Pattern

Following Sequel's adapter pattern, the fix will be implemented in the `DatasetMethods` module within `lib/sequel/adapters/shared/duckdb.rb`, allowing the main `Dataset` class to inherit the corrected behavior through the include mechanism.

## Components and Interfaces

### Core Component: Enhanced literal_append Method

**Location**: `Sequel::DuckDB::DatasetMethods` module

**Interface**:
```ruby
def literal_append(sql, v)
  case v
  when Time
    literal_datetime_append(sql, v)
  when DateTime
    literal_datetime_append(sql, v)
  when String
    case v
    when LiteralString
      sql << v  # Append directly without quoting
    else
      if v.encoding == Encoding::ASCII_8BIT
        literal_blob_append(sql, v)
      else
        literal_string_append(sql, v)
      end
    end
  else
    super
  end
end
```

### Type Handling Components

#### 1. LiteralString Handler
- **Purpose**: Process `Sequel.lit()` expressions as raw SQL following Sequel core pattern
- **Behavior**: Append string content directly without quoting (`sql << v`)
- **Input**: `Sequel::LiteralString` objects (subclass of `String`)
- **Output**: Raw SQL string appended to query

#### 2. Regular String Handler
- **Purpose**: Process regular Ruby strings with appropriate encoding handling
- **Behavior**: Apply existing binary/text string logic
- **Input**: Regular `String` objects (excluding `LiteralString`)
- **Output**: Properly quoted and escaped string literals

#### 3. Parent Class Delegation
- **Purpose**: Handle all other data types including `SQL::Function`
- **Behavior**: Delegate to parent class implementation (which already works correctly)
- **Input**: All other Ruby/Sequel objects including `SQL::Expression` subclasses
- **Output**: Appropriately formatted literals using Sequel core logic

### Integration Points

#### Dataset Class Integration
The enhanced `literal_append` method integrates with:
- **SQL Generation Pipeline**: Core Sequel SQL building process
- **Query Context Handling**: SELECT, WHERE, ORDER BY, GROUP BY, HAVING clauses
- **Expression Composition**: Nested and complex expressions

#### Sequel Framework Integration
- **Parent Class Delegation**: Leverages existing Sequel functionality
- **Type System Compatibility**: Works with Sequel's expression type hierarchy
- **Error Handling**: Integrates with Sequel's error reporting system

## Data Models

### Input Data Types

#### Sequel::LiteralString
```ruby
# Created by: Sequel.lit("YEAR(created_at)")
# Properties:
#   - Contains raw SQL string
#   - Should not be quoted
#   - Used for expressions, functions, literals
```

#### Sequel::SQL::Function
```ruby
# Created by: Sequel.function(:count, :*)
# Properties:
#   - Function name and arguments
#   - Requires special rendering logic
#   - May contain nested expressions
```

#### Regular Data Types
```ruby
# String, Integer, Float, Date, Time, etc.
# Properties:
#   - Standard Ruby types
#   - Require appropriate SQL formatting
#   - Should be quoted/escaped as needed
```

### Output Data Model

#### SQL String Buffer
- **Type**: String (mutable)
- **Content**: Accumulated SQL query text
- **Modification**: Appended to by `literal_append`

## Error Handling

### Error Categories

#### 1. Unsupported Expression Types
- **Scenario**: Unknown SQL expression object encountered
- **Response**: Clear error message with object type information
- **Error Type**: `Sequel::Error` or subclass

#### 2. Expression Rendering Failures
- **Scenario**: Function or expression rendering fails
- **Response**: Context-aware error with failing expression details
- **Error Type**: `Sequel::DatabaseError`

#### 3. SQL Generation Failures
- **Scenario**: Overall SQL generation fails due to expression issues
- **Response**: Categorized error with query context
- **Error Type**: `Sequel::DatabaseError`

### Error Handling Strategy

```ruby
def literal_append(sql, v)
  case v
  when Time
    literal_datetime_append(sql, v)
  when DateTime
    literal_datetime_append(sql, v)
  when String
    case v
    when LiteralString
      sql << v
    else
      if v.encoding == Encoding::ASCII_8BIT
        literal_blob_append(sql, v)
      else
        literal_string_append(sql, v)
      end
    end
  else
    super
  end
rescue => e
  raise Sequel::DatabaseError, "Failed to render SQL literal: #{e.message}"
end
```

## Testing Strategy

### Test Categories

#### 1. Unit Tests - SQL Generation
- **Framework**: Sequel mock database
- **Purpose**: Test SQL generation without database connections
- **Coverage**: All expression types and combinations
- **Location**: `test/sql_test.rb`

#### 2. Integration Tests - Database Operations
- **Framework**: Real DuckDB in-memory databases
- **Purpose**: End-to-end expression functionality
- **Coverage**: Query execution with expressions
- **Location**: `test/dataset_test.rb`

#### 3. Regression Tests
- **Purpose**: Ensure backward compatibility
- **Coverage**: Existing functionality continues working
- **Location**: Multiple test files

### Test Implementation Approach

#### Test-Driven Development
1. **Write failing tests** for each expression type
2. **Implement minimal fix** to make tests pass
3. **Refactor** while maintaining green tests
4. **Add edge case tests** and handle them

#### Test Structure
```ruby
def test_literal_string_handling
  # Test Sequel.lit() expressions
  dataset = @db[:users].select(Sequel.lit("YEAR(created_at)"))
  assert_equal "SELECT YEAR(created_at) FROM users", dataset.sql
end

def test_function_handling_still_works
  # Test that Sequel.function() calls continue to work (already working)
  dataset = @db[:users].select(Sequel.function(:count, :*))
  assert_equal "SELECT count(*) FROM users", dataset.sql
end

def test_regular_string_still_quoted
  # Test that regular strings are still properly quoted
  dataset = @db[:users].where(name: "John's")
  assert_equal "SELECT * FROM users WHERE (name = 'John''s')", dataset.sql
end
```

### Coverage Requirements
- **100% line coverage** for new `literal_append` method
- **Branch coverage** for all case statements
- **Edge case coverage** for error conditions
- **Integration coverage** for all SQL clause types

## Design Decisions and Rationales

### Decision 1: Follow Sequel Core Pattern Exactly
**Rationale**: Sequel core already has the correct pattern for handling `LiteralString` as a special case of `String`. Following this established pattern ensures compatibility and maintainability.

**Evidence**: Sequel core's `literal_append` method in `/sequel/dataset/sql.rb` shows the exact pattern:
```ruby
when String
  case v
  when LiteralString
    sql << v
  when SQL::Blob
    literal_blob_append(sql, v)
  else
    literal_string_append(sql, v)
  end
```

### Decision 2: Minimal Modification Approach
**Rationale**: The current implementation already works correctly for most types. Only the `LiteralString` handling is missing, so we add the minimal necessary check.

**Alternatives Considered**:
- Complete rewrite of literal_append (rejected - unnecessary and risky)
- Separate method for LiteralString (rejected - doesn't follow Sequel patterns)

### Decision 3: No Changes to Function Handling
**Rationale**: Testing revealed that `SQL::Function` objects already work correctly through parent class delegation. The issue is specifically with `LiteralString` objects being treated as regular strings.

**Evidence**:
- `db[:test].select(Sequel.function(:count, :*)).sql` produces correct `"SELECT count(*) FROM test"`
- `db[:test].select(Sequel.lit('YEAR(created_at)')).sql` incorrectly produces `"SELECT 'YEAR(created_at)' FROM test"`

### Decision 4: Preserve Existing Time/DateTime/Binary Handling
**Rationale**: The current implementation correctly handles `Time`, `DateTime`, and binary string encoding. These should remain unchanged.

**Benefits**:
- Maintains backward compatibility for existing functionality
- Focuses fix on the specific problem area
- Reduces risk of regression in working features

### Decision 5: Integration in DatasetMethods Module
**Rationale**: Following the established sequel-duckdb pattern where shared functionality is implemented in modules and included in main classes.

**Benefits**:
- Consistent with existing codebase architecture
- Allows for easy testing and maintenance
- Follows Sequel adapter conventions

## Implementation Phases

### Phase 1: Fix LiteralString Handling
- Modify existing `literal_append` method to check for `LiteralString` before regular `String`
- Add comprehensive unit tests for `LiteralString` handling
- Verify existing functionality remains intact

### Phase 2: Comprehensive Testing
- Add integration tests with real database operations
- Test all SQL clause contexts (SELECT, WHERE, ORDER BY, etc.)
- Add regression tests to ensure no existing functionality breaks

### Phase 3: Edge Cases and Error Handling
- Test complex nested expressions
- Verify error handling for malformed expressions
- Performance verification with existing benchmarks

This design provides a focused, low-risk solution that addresses the core SQL expression handling issues while maintaining full backward compatibility and following established Sequel adapter patterns.