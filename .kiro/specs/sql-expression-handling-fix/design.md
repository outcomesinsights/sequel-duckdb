# Design Document: SQL Expression Handling Fix

## Overview

This design addresses critical SQL expression handling issues in the sequel-duckdb adapter where SQL expressions, functions, and literal strings are incorrectly treated as regular string literals and quoted when they should be rendered as raw SQL. The fix involves implementing proper type detection and handling in the `literal_append` method to distinguish between different SQL object types.

The core issue is that the current adapter implementation doesn't properly handle Sequel's expression objects (`Sequel::LiteralString`, `Sequel::SQL::Function`, etc.) and treats them as regular Ruby strings, causing them to be quoted inappropriately in the generated SQL.

## Architecture

### Current Problem

The existing `literal_append` method in the DuckDB adapter lacks proper type discrimination for SQL expression objects. All objects are being processed through a generic string handling path that applies SQL string quoting, breaking the intended behavior of SQL expressions and functions.

### Solution Architecture

The solution implements a type-aware `literal_append` method that:

1. **Type Detection**: Identifies the specific type of SQL object being processed
2. **Delegated Handling**: Routes each type to appropriate handling logic
3. **Raw SQL Preservation**: Ensures SQL expressions maintain their unquoted form
4. **Backward Compatibility**: Preserves existing behavior for regular data types

### Design Pattern

Following Sequel's adapter pattern, the fix will be implemented in the `DatasetMethods` module within `lib/sequel/adapters/shared/duckdb.rb`, allowing the main `Dataset` class to inherit the corrected behavior through the include mechanism.

## Components and Interfaces

### Core Component: Enhanced literal_append Method

**Location**: `Sequel::DuckDB::DatasetMethods` module

**Interface**:
```ruby
def literal_append(sql, value)
  case value
  when Sequel::LiteralString
    # Handle raw SQL expressions
  when Sequel::SQL::Function
    # Handle function calls
  else
    # Delegate to parent class for other types
  end
end
```

### Type Handling Components

#### 1. LiteralString Handler
- **Purpose**: Process `Sequel.lit()` expressions as raw SQL
- **Behavior**: Append string content directly without quoting
- **Input**: `Sequel::LiteralString` objects
- **Output**: Raw SQL string appended to query

#### 2. Function Handler
- **Purpose**: Process `Sequel.function()` calls
- **Behavior**: Delegate to Sequel's built-in function rendering
- **Input**: `Sequel::SQL::Function` objects
- **Output**: Properly formatted function calls (e.g., `count(*)`, `sum(amount)`)

#### 3. Fallback Handler
- **Purpose**: Handle all other data types
- **Behavior**: Delegate to parent class implementation
- **Input**: All other Ruby/Sequel objects
- **Output**: Appropriately formatted literals (quoted strings, numbers, etc.)

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
def literal_append(sql, value)
  case value
  when Sequel::LiteralString
    sql << value
  when Sequel::SQL::Function
    super
  else
    super
  end
rescue => e
  raise Sequel::DatabaseError, "Failed to render SQL expression: #{e.message}"
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

def test_function_handling
  # Test Sequel.function() calls
  dataset = @db[:users].select(Sequel.function(:count, :*))
  assert_equal "SELECT count(*) FROM users", dataset.sql
end
```

### Coverage Requirements
- **100% line coverage** for new `literal_append` method
- **Branch coverage** for all case statements
- **Edge case coverage** for error conditions
- **Integration coverage** for all SQL clause types

## Design Decisions and Rationales

### Decision 1: Override literal_append Method
**Rationale**: This is the core method responsible for converting Ruby objects to SQL literals in Sequel. Overriding it provides the most direct and comprehensive solution.

**Alternatives Considered**:
- Overriding specific SQL generation methods (rejected - too many methods to override)
- Preprocessing expressions before SQL generation (rejected - complex and error-prone)

### Decision 2: Case-Based Type Discrimination
**Rationale**: Ruby's case statement provides clear, readable type checking that's easy to extend and maintain.

**Alternatives Considered**:
- Method dispatch based on object type (rejected - more complex setup)
- Hash-based type mapping (rejected - less readable)

### Decision 3: Minimal Implementation Approach
**Rationale**: Only handle the specific problematic types (`LiteralString`, `Function`) and delegate everything else to the parent class to maintain compatibility.

**Alternatives Considered**:
- Complete reimplementation of literal handling (rejected - high risk of breaking existing functionality)
- Monkey-patching Sequel core (rejected - not maintainable)

### Decision 4: Preserve Parent Class Delegation
**Rationale**: Sequel's existing literal handling is robust and well-tested. We only need to fix the specific expression handling issues.

**Benefits**:
- Maintains backward compatibility
- Leverages existing Sequel functionality
- Reduces implementation complexity
- Minimizes risk of introducing new bugs

### Decision 5: Integration in DatasetMethods Module
**Rationale**: Following the established sequel-duckdb pattern where shared functionality is implemented in modules and included in main classes.

**Benefits**:
- Consistent with existing codebase architecture
- Allows for easy testing and maintenance
- Follows Sequel adapter conventions

## Implementation Phases

### Phase 1: Core Expression Handling
- Implement basic `literal_append` override
- Handle `Sequel::LiteralString` objects
- Add comprehensive unit tests

### Phase 2: Function Support
- Add `Sequel::SQL::Function` handling
- Test function call generation
- Verify nested function support

### Phase 3: Integration and Testing
- Add integration tests with real database
- Test all SQL clause contexts
- Verify backward compatibility

### Phase 4: Error Handling and Edge Cases
- Implement comprehensive error handling
- Add edge case tests
- Performance verification

This design provides a focused, low-risk solution that addresses the core SQL expression handling issues while maintaining full backward compatibility and following established Sequel adapter patterns.