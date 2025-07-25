# Requirements Document

## Introduction

This specification addresses SQL generation issues in the sequel-duckdb adapter where the adapter is generating non-standard SQL syntax that doesn't match expected Sequel conventions. The adapter should generate clean, standard SQL that follows Sequel's established patterns while being compatible with DuckDB. The issues are in the adapter's SQL generation logic, not in DuckDB's SQL support.

## Requirements

### Requirement 1: LIKE Clause Clean Generation

**User Story:** As a developer using Sequel with DuckDB, I want LIKE clauses to generate clean SQL without unnecessary ESCAPE clauses, so that the SQL matches standard Sequel patterns.

#### Acceptance Criteria

1. WHEN I use `Sequel.like(:name, "%John%")` THEN the generated SQL SHALL be `(name LIKE '%John%')` without ESCAPE clause
2. WHEN I use LIKE patterns with special characters THEN they SHALL work without requiring explicit ESCAPE clauses
3. WHEN I use ILIKE patterns THEN they SHALL be converted to `(UPPER(column) LIKE UPPER(pattern))` with proper parentheses
4. WHEN tests check LIKE clause generation THEN they SHALL expect clean SQL without ESCAPE additions

### Requirement 2: Complex Expression Parentheses

**User Story:** As a developer using Sequel with DuckDB, I want complex expressions to be properly parenthesized in generated SQL, so that the SQL follows standard Sequel formatting conventions.

#### Acceptance Criteria

1. WHEN I use ILIKE expressions THEN they SHALL be wrapped in parentheses: `(UPPER(column) LIKE UPPER(pattern))`
2. WHEN I use regex expressions THEN they SHALL be wrapped in parentheses: `(column ~ 'pattern')`
3. WHEN I use boolean comparisons with `=~` THEN they SHALL generate proper `IS` syntax with parentheses
4. WHEN tests check complex expressions THEN they SHALL expect properly parenthesized SQL

### Requirement 3: Standard Table Alias Generation

**User Story:** As a developer using Sequel with DuckDB, I want table aliases to use standard SQL `AS` syntax, so that the generated SQL follows established SQL conventions.

#### Acceptance Criteria

1. WHEN I use `table___alias` syntax THEN it SHALL generate `table AS alias` in the SQL
2. WHEN I use table aliases in JOINs THEN they SHALL use proper `AS` syntax consistently
3. WHEN I reference aliased tables THEN they SHALL use the alias name correctly
4. WHEN tests check table aliases THEN they SHALL expect standard `AS` syntax

### Requirement 4: Standard Qualified Column References

**User Story:** As a developer using Sequel with DuckDB, I want qualified column references to use standard SQL dot notation, so that the generated SQL follows established SQL conventions.

#### Acceptance Criteria

1. WHEN I reference columns across tables THEN they SHALL use `table.column` syntax
2. WHEN I use qualified column names in subqueries THEN they SHALL use proper dot notation
3. WHEN I use schema-qualified names THEN they SHALL use standard SQL format
4. WHEN tests check qualified identifiers THEN they SHALL expect standard dot notation

### Requirement 5: Proper Regular Expression Formatting

**User Story:** As a developer using Sequel with DuckDB, I want regular expression matching to generate properly formatted SQL with parentheses, so that the SQL follows Sequel's formatting conventions.

#### Acceptance Criteria

1. WHEN I use regex matching with `~` operator THEN it SHALL generate `(column ~ 'pattern')` with parentheses
2. WHEN I use case-insensitive regex THEN it SHALL be properly formatted with parentheses
3. WHEN I use complex regex patterns THEN they SHALL be properly formatted and parenthesized
4. WHEN tests check regex syntax THEN they SHALL expect properly parenthesized expressions

### Requirement 6: Standard Subquery Column References

**User Story:** As a developer using Sequel with DuckDB, I want subqueries to properly reference outer query columns using standard SQL dot notation, so that correlated subqueries follow SQL conventions.

#### Acceptance Criteria

1. WHEN I use correlated subqueries THEN column references SHALL use `table.column` syntax
2. WHEN I reference outer query columns THEN they SHALL be properly qualified with dot notation
3. WHEN I use EXISTS subqueries THEN column references SHALL use standard SQL format
4. WHEN tests check subquery references THEN they SHALL expect standard dot notation

### Requirement 7: Consistent SQL Generation Testing

**User Story:** As a developer maintaining the sequel-duckdb adapter, I want tests to verify that the adapter generates consistent, standard SQL, so that the adapter follows Sequel's established patterns.

#### Acceptance Criteria

1. WHEN tests check SQL generation THEN they SHALL expect standard SQL syntax
2. WHEN the adapter generates SQL THEN it SHALL be consistent with Sequel's conventions
3. WHEN SQL generation issues are found THEN they SHALL be fixed in the adapter, not worked around in tests
4. WHEN SQL correctness is verified THEN both functional and syntactic correctness SHALL be maintained

### Requirement 8: Documentation of SQL Generation Patterns

**User Story:** As a developer using the sequel-duckdb adapter, I want to understand how the adapter generates SQL for DuckDB, so that I can write queries that work optimally with the adapter.

#### Acceptance Criteria

1. WHEN the adapter generates specific SQL patterns THEN they SHALL be documented
2. WHEN SQL generation differs from other Sequel adapters THEN the differences SHALL be explained
3. WHEN DuckDB-specific optimizations are used THEN they SHALL be documented with examples
4. WHEN SQL generation patterns change THEN documentation SHALL be updated accordingly