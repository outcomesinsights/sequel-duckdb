# Requirements Document

## Introduction

This specification addresses critical SQL expression handling issues in the sequel-duckdb adapter. The adapter is currently incorrectly treating SQL expressions, functions, and literal strings as regular string literals, causing them to be quoted when they should be rendered as raw SQL. This breaks fundamental SQL generation functionality and prevents proper use of database functions, expressions, and literal SQL.

## Requirements

### Requirement 1: Sequel::LiteralString Handling

**User Story:** As a developer using Sequel with DuckDB, I want to use `Sequel.lit()` to include raw SQL expressions in my queries, so that I can use database functions and complex expressions without them being quoted as strings.

#### Acceptance Criteria

1. WHEN I use `Sequel.lit("YEAR(created_at)")` in a SELECT clause THEN the generated SQL SHALL contain `YEAR(created_at)` without quotes
2. WHEN I use `Sequel.lit("LENGTH(name) > 5")` in a WHERE clause THEN the generated SQL SHALL contain `LENGTH(name) > 5` without quotes
3. WHEN I use `Sequel.lit("CURRENT_TIMESTAMP")` in an UPDATE clause THEN the generated SQL SHALL contain `CURRENT_TIMESTAMP` without quotes
4. WHEN I use `Sequel.lit("name || ' ' || email AS full_info")` in a SELECT clause THEN the generated SQL SHALL contain the expression without quotes

### Requirement 2: Sequel::SQL::Function Handling

**User Story:** As a developer using Sequel with DuckDB, I want to use `Sequel.function()` to call database functions, so that function calls are properly generated in SQL without being quoted as strings.

#### Acceptance Criteria

1. WHEN I use `Sequel.function(:count, :*)` THEN the generated SQL SHALL contain `count(*)`
2. WHEN I use `Sequel.function(:sum, :amount)` THEN the generated SQL SHALL contain `sum(amount)`
3. WHEN I use `Sequel.function(:year, :created_at)` THEN the generated SQL SHALL contain `year(created_at)`
4. WHEN I use nested functions like `Sequel.function(:count, Sequel.function(:distinct, :name))` THEN the generated SQL SHALL contain `count(distinct(name))`

### Requirement 3: Complex Expression Handling

**User Story:** As a developer using Sequel with DuckDB, I want to use complex SQL expressions with operators and functions, so that they are properly rendered as SQL without being quoted.

#### Acceptance Criteria

1. WHEN I use expressions with mathematical operators THEN they SHALL be rendered as raw SQL
2. WHEN I use expressions with string concatenation operators THEN they SHALL be rendered as raw SQL
3. WHEN I use expressions with comparison operators THEN they SHALL be rendered as raw SQL
4. WHEN I use expressions with logical operators THEN they SHALL be rendered as raw SQL

### Requirement 4: Literal Append Method Override

**User Story:** As a developer maintaining the sequel-duckdb adapter, I want the `literal_append` method to properly distinguish between different types of SQL objects, so that each type is handled appropriately.

#### Acceptance Criteria

1. WHEN `literal_append` receives a `Sequel::LiteralString` object THEN it SHALL append the string content without quotes
2. WHEN `literal_append` receives a `Sequel::SQL::Function` object THEN it SHALL delegate to the appropriate function rendering method
3. WHEN `literal_append` receives a regular String object THEN it SHALL quote it as a string literal
4. WHEN `literal_append` receives other SQL expression objects THEN it SHALL delegate to the parent class method

### Requirement 5: Expression Context Preservation

**User Story:** As a developer using Sequel with DuckDB, I want SQL expressions to maintain their context when used in different parts of queries, so that they work correctly in SELECT, WHERE, ORDER BY, GROUP BY, and HAVING clauses.

#### Acceptance Criteria

1. WHEN I use expressions in SELECT clauses THEN they SHALL be rendered as raw SQL
2. WHEN I use expressions in WHERE clauses THEN they SHALL be rendered as raw SQL
3. WHEN I use expressions in ORDER BY clauses THEN they SHALL be rendered as raw SQL
4. WHEN I use expressions in GROUP BY clauses THEN they SHALL be rendered as raw SQL
5. WHEN I use expressions in HAVING clauses THEN they SHALL be rendered as raw SQL
6. WHEN I use expressions in UPDATE SET clauses THEN they SHALL be rendered as raw SQL

### Requirement 6: Backward Compatibility

**User Story:** As a developer using the sequel-duckdb adapter, I want existing functionality to continue working after expression handling is fixed, so that my current code doesn't break.

#### Acceptance Criteria

1. WHEN I use regular string values in queries THEN they SHALL still be properly quoted as string literals
2. WHEN I use numeric values in queries THEN they SHALL still be rendered as numeric literals
3. WHEN I use boolean values in queries THEN they SHALL still be rendered as boolean literals
4. WHEN I use date/time values in queries THEN they SHALL still be rendered with proper formatting
5. WHEN I use binary data in queries THEN it SHALL still be rendered as hex literals

### Requirement 7: Error Handling

**User Story:** As a developer using the sequel-duckdb adapter, I want clear error messages when expression handling fails, so that I can debug issues effectively.

#### Acceptance Criteria

1. WHEN an unsupported expression type is encountered THEN a clear error message SHALL be provided
2. WHEN expression rendering fails THEN the error SHALL include context about the failing expression
3. WHEN SQL generation fails due to expression issues THEN the error SHALL be properly categorized as a DatabaseError