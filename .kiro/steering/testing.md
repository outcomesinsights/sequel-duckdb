# Testing Requirements and Standards

## CRITICAL RULE: Test-Driven Development (TDD) is MANDATORY

**Every single piece of code implementation must follow strict Test-Driven Development:**

1. **Write Tests FIRST** - Before writing any implementation code, comprehensive tests must be written
2. **Red-Green-Refactor** - Follow the TDD cycle: failing test → minimal implementation → refactor
3. **No Code Without Tests** - Implementation without corresponding tests is not acceptable
4. **100% Test Coverage** - All implemented functionality must have test coverage

## Test Structure (Following sequel-hexspace Pattern)

### Required Test Files
```
test/
├── all.rb                       # Test runner - loads all test files
├── spec_helper.rb               # Test configuration, setup, and shared utilities
├── database_test.rb             # Database connection, transactions, and basic functionality
├── dataset_test.rb              # Comprehensive SQL generation and query execution tests
├── schema_test.rb               # Schema operations, introspection, and DDL tests
├── prepared_statement_test.rb   # Prepared statement functionality and parameter binding
├── sql_test.rb                  # SQL generation syntax and correctness tests
└── type_test.rb                 # Data type handling, conversion, and mapping tests
```

### Test Categories

#### 1. SQL Generation Tests (Unit Tests)
- **Purpose**: Test SQL generation without database connections
- **Tool**: Use Sequel's mock database functionality
- **Requirements**:
  - Every SQL generation method must have tests
  - Verify correct SQL syntax and structure
  - Test edge cases and parameter handling
  - Must be fast and isolated
  - Test all SQL operations: SELECT, INSERT, UPDATE, DELETE, DDL

#### 2. Integration Tests
- **Purpose**: Test actual database operations
- **Tool**: Use real DuckDB in-memory databases
- **Requirements**:
  - Test actual database operations end-to-end
  - Verify data persistence and retrieval
  - Test connection management and lifecycle
  - Test transaction behavior (commit, rollback)
  - Test error handling with real database errors

#### 3. Schema Tests
- **Purpose**: Test schema operations and introspection
- **Requirements**:
  - Test table creation, modification, and deletion
  - Test index operations
  - Test schema introspection accuracy
  - Test constraint handling
  - Test DuckDB-specific schema features

#### 4. Type Conversion Tests
- **Purpose**: Test Ruby ↔ DuckDB type mapping
- **Requirements**:
  - Test all supported data types
  - Test edge cases and null handling
  - Test precision and scale for numeric types
  - Test date/time handling and timezone considerations
  - Test binary data and text encoding

#### 5. Error Handling Tests
- **Purpose**: Test proper exception mapping and error scenarios
- **Requirements**:
  - Test Sequel exception mapping
  - Test connection failure scenarios
  - Test SQL syntax error handling
  - Test constraint violation handling
  - Test timeout and resource limit scenarios

## Implementation Workflow

### For Every Task Involving Code:

1. **Step 1: Write Tests First**
   - Create comprehensive test cases covering the functionality
   - Include both positive and negative test cases
   - Test edge cases and error conditions
   - Ensure tests fail initially (Red phase)

2. **Step 2: Minimal Implementation**
   - Write the minimal code needed to make tests pass
   - Focus on making tests green, not on perfect implementation
   - Avoid over-engineering at this stage

3. **Step 3: Refactor**
   - Improve code quality while keeping tests green
   - Optimize performance if needed
   - Ensure code follows style guidelines

4. **Step 4: Verify Coverage**
   - Ensure all implemented functionality has test coverage
   - Add additional tests if gaps are found

## Test Quality Standards

### Test Code Quality
- Tests must be clear and readable
- Test names should describe what is being tested
- Tests should be independent and isolated
- Tests should be deterministic (no flaky tests)
- Tests should run quickly (especially unit tests)

### Test Coverage Requirements
- **100% line coverage** for all implemented functionality
- **Branch coverage** for all conditional logic
- **Edge case coverage** for error conditions
- **Integration coverage** for database operations

### Test Documentation
- Each test file should have a header explaining its purpose
- Complex test setups should be documented
- Test utilities should be well-documented
- Tests serve as executable documentation

## Tools and Utilities

### Mock Database Testing
```ruby
# Example of mock database testing for SQL generation
DB = Sequel.mock
dataset = DB[:users].where(name: 'John')
assert_equal "SELECT * FROM users WHERE (name = 'John')", dataset.sql
```

### Integration Testing Setup
```ruby
# Example of integration testing with real DuckDB
def setup
  @db = Sequel.connect("duckdb::memory:")
  @db.create_table(:test_table) do
    primary_key :id
    String :name
  end
end
```

## Continuous Integration

### Test Execution
- All tests must pass before any code is merged
- Tests should be run on multiple Ruby versions
- Tests should be run on different operating systems
- Performance regression tests should be included

### Test Reporting
- Test results should be clearly reported
- Coverage reports should be generated
- Failed tests should provide clear error messages
- Test execution time should be monitored

## Common Testing Patterns

### Testing SQL Generation
```ruby
def test_select_with_where
  dataset = @db[:users].where(name: 'John')
  assert_equal "SELECT * FROM users WHERE (name = 'John')", dataset.sql
end
```

### Testing Database Operations
```ruby
def test_insert_and_select
  @db[:users].insert(name: 'John', email: 'john@example.com')
  user = @db[:users].where(name: 'John').first
  assert_equal 'john@example.com', user[:email]
end
```

### Testing Error Conditions
```ruby
def test_connection_error
  assert_raises(Sequel::DatabaseConnectionError) do
    Sequel.connect("duckdb:/invalid/path/database.db")
  end
end
```

## Remember: NO CODE WITHOUT TESTS

This is not optional. Every implementation task must begin with writing comprehensive tests. This ensures:
- Functionality works as expected
- Regressions are caught early
- Code is maintainable and refactorable
- Documentation through executable examples
- Confidence in the codebase