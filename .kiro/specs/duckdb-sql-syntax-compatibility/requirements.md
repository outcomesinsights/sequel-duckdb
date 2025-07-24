# Requirements Document

## Introduction

This specification addresses SQL syntax differences between DuckDB and standard SQL expectations in the sequel-duckdb adapter tests. DuckDB has legitimate syntax variations that are correct for the database but differ from generic SQL expectations. The adapter should generate DuckDB-compatible SQL, and tests should be updated to accept these valid syntax differences rather than forcing generic SQL patterns.

## Requirements

### Requirement 1: LIKE Clause ESCAPE Handling

**User Story:** As a developer using Sequel with DuckDB, I want LIKE clauses to work correctly with DuckDB's automatic ESCAPE clause addition, so that pattern matching works as expected without syntax errors.

#### Acceptance Criteria

1. WHEN I use `Sequel.like(:name, "%John%")` THEN the generated SQL SHALL include `ESCAPE '\\'` if DuckDB adds it automatically
2. WHEN I use LIKE patterns with special characters THEN DuckDB's escape handling SHALL be preserved
3. WHEN I use ILIKE patterns THEN they SHALL be converted to DuckDB-compatible UPPER() LIKE UPPER() syntax
4. WHEN tests check LIKE clause generation THEN they SHALL accept DuckDB's ESCAPE clause addition

### Requirement 2: Boolean Comparison Syntax

**User Story:** As a developer using Sequel with DuckDB, I want boolean comparisons to use DuckDB's preferred syntax, so that queries execute correctly without forcing non-standard SQL patterns.

#### Acceptance Criteria

1. WHEN I compare a column to a boolean value THEN DuckDB MAY use `=` instead of `IS` for some comparisons
2. WHEN I use `column =~ true` syntax THEN it SHALL generate appropriate DuckDB boolean comparison syntax
3. WHEN I use `column =~ false` syntax THEN it SHALL generate appropriate DuckDB boolean comparison syntax
4. WHEN tests check boolean comparisons THEN they SHALL accept both `IS` and `=` syntax as valid

### Requirement 3: Table Alias Syntax

**User Story:** As a developer using Sequel with DuckDB, I want table aliases to work with DuckDB's supported syntax variations, so that joins and complex queries work correctly.

#### Acceptance Criteria

1. WHEN I use `table___alias` syntax THEN it SHALL be handled appropriately for DuckDB
2. WHEN I use explicit `AS` alias syntax THEN it SHALL work correctly
3. WHEN DuckDB uses different alias representations THEN they SHALL be accepted as valid
4. WHEN tests check table aliases THEN they SHALL accept DuckDB's alias syntax variations

### Requirement 4: Identifier Quoting Differences

**User Story:** As a developer using Sequel with DuckDB, I want identifier references to use DuckDB's syntax for cross-table references, so that joins and subqueries work correctly.

#### Acceptance Criteria

1. WHEN I reference columns across tables THEN DuckDB MAY use `table__column` instead of `table.column` syntax
2. WHEN I use qualified column names THEN they SHALL work with DuckDB's identifier syntax
3. WHEN I use schema-qualified names THEN they SHALL use DuckDB's preferred format
4. WHEN tests check identifier quoting THEN they SHALL accept DuckDB's syntax variations

### Requirement 5: Regular Expression Syntax

**User Story:** As a developer using Sequel with DuckDB, I want regular expression matching to use DuckDB's native regex syntax, so that pattern matching works efficiently.

#### Acceptance Criteria

1. WHEN I use regex matching with `~` operator THEN it SHALL generate DuckDB-compatible regex syntax
2. WHEN I use case-insensitive regex THEN it SHALL use DuckDB's preferred approach
3. WHEN I use complex regex patterns THEN they SHALL be properly escaped for DuckDB
4. WHEN tests check regex syntax THEN they SHALL accept DuckDB's regex operator format

### Requirement 6: Subquery Column References

**User Story:** As a developer using Sequel with DuckDB, I want subqueries to properly reference outer query columns using DuckDB's syntax, so that correlated subqueries work correctly.

#### Acceptance Criteria

1. WHEN I use correlated subqueries THEN column references SHALL use DuckDB's preferred syntax
2. WHEN I reference outer query columns THEN they SHALL be properly qualified for DuckDB
3. WHEN I use EXISTS subqueries THEN column references SHALL work with DuckDB syntax
4. WHEN tests check subquery references THEN they SHALL accept DuckDB's column reference format

### Requirement 7: Test Flexibility Framework

**User Story:** As a developer maintaining the sequel-duckdb adapter, I want tests to be flexible about DuckDB syntax differences, so that valid DuckDB SQL is not rejected due to minor syntax variations.

#### Acceptance Criteria

1. WHEN tests check SQL generation THEN they SHALL use flexible matching for DuckDB syntax differences
2. WHEN DuckDB generates functionally equivalent but syntactically different SQL THEN tests SHALL accept it
3. WHEN new DuckDB syntax variations are discovered THEN tests SHALL be easily updatable
4. WHEN SQL correctness is verified THEN functional correctness SHALL be prioritized over exact string matching

### Requirement 8: Documentation of Syntax Differences

**User Story:** As a developer using the sequel-duckdb adapter, I want to understand DuckDB's SQL syntax differences, so that I can write queries that work optimally with DuckDB.

#### Acceptance Criteria

1. WHEN DuckDB uses different syntax than standard SQL THEN it SHALL be documented
2. WHEN syntax differences affect query behavior THEN examples SHALL be provided
3. WHEN workarounds are needed for compatibility THEN they SHALL be documented
4. WHEN new syntax differences are discovered THEN documentation SHALL be updated