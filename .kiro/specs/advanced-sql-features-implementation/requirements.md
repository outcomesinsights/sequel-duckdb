# Requirements Document

## Introduction

This specification addresses missing advanced SQL features in the sequel-duckdb adapter that are currently causing test failures. These features represent important SQL capabilities that should be implemented to provide a complete DuckDB adapter experience. The features include JOIN USING clauses, recursive Common Table Expressions (CTEs), complex query optimizations, and other advanced SQL constructs.

## Requirements

### Requirement 1: JOIN USING Clause Support

**User Story:** As a developer using Sequel with DuckDB, I want to use JOIN USING syntax for joins on columns with the same name, so that I can write more concise and readable join queries.

#### Acceptance Criteria

1. WHEN I use `dataset.join(:other_table, nil, using: :column_name)` THEN it SHALL generate `JOIN other_table USING (column_name)`
2. WHEN I use `dataset.join(:other_table, nil, using: [:col1, :col2])` THEN it SHALL generate `JOIN other_table USING (col1, col2)`
3. WHEN I use USING with different join types THEN it SHALL work with INNER, LEFT, RIGHT, and FULL joins
4. WHEN I use USING with table aliases THEN it SHALL properly reference the aliased tables

### Requirement 2: Recursive Common Table Expressions (CTEs)

**User Story:** As a developer using Sequel with DuckDB, I want to create recursive CTEs for hierarchical queries, so that I can efficiently query tree structures and recursive relationships.

#### Acceptance Criteria

1. WHEN I use `dataset.with_recursive(:cte_name, base_case.union(recursive_case))` THEN it SHALL generate `WITH RECURSIVE cte_name AS (...)`
2. WHEN I create a recursive CTE THEN the base case SHALL be properly combined with the recursive case using UNION
3. WHEN I reference the CTE within itself THEN it SHALL generate valid recursive SQL
4. WHEN I use recursive CTEs with termination conditions THEN they SHALL execute without infinite loops

### Requirement 3: Window Function Enhancements

**User Story:** As a developer using Sequel with DuckDB, I want comprehensive window function support, so that I can perform advanced analytical queries efficiently.

#### Acceptance Criteria

1. WHEN I use `Sequel.function(:row_number).over(partition: :column, order: :other_column)` THEN it SHALL generate proper OVER clause syntax
2. WHEN I use ranking functions like RANK, DENSE_RANK THEN they SHALL work with proper window specifications
3. WHEN I use aggregate functions as window functions THEN they SHALL generate proper windowed aggregation syntax
4. WHEN I use LAG and LEAD functions THEN they SHALL support offset and default value parameters

### Requirement 4: Complex Query Optimization

**User Story:** As a developer using Sequel with DuckDB, I want complex queries with multiple clauses to be optimized and execute efficiently, so that analytical workloads perform well.

#### Acceptance Criteria

1. WHEN I create queries with multiple JOINs, WHERE, GROUP BY, HAVING, and ORDER BY clauses THEN they SHALL generate optimized SQL
2. WHEN I use subqueries in complex queries THEN they SHALL be properly integrated without syntax errors
3. WHEN I use aggregate functions in complex queries THEN they SHALL work correctly with grouping and filtering
4. WHEN I use window functions in complex queries THEN they SHALL integrate properly with other query clauses

### Requirement 5: Advanced Subquery Support

**User Story:** As a developer using Sequel with DuckDB, I want comprehensive subquery support in all SQL contexts, so that I can write sophisticated analytical queries.

#### Acceptance Criteria

1. WHEN I use correlated subqueries THEN column references SHALL properly resolve to outer query tables
2. WHEN I use EXISTS and NOT EXISTS subqueries THEN they SHALL generate correct SQL with proper correlation
3. WHEN I use scalar subqueries in SELECT clauses THEN they SHALL return single values correctly
4. WHEN I use subqueries in FROM clauses THEN they SHALL be treated as derived tables

### Requirement 6: Set Operations Enhancement

**User Story:** As a developer using Sequel with DuckDB, I want comprehensive set operations (UNION, INTERSECT, EXCEPT) with proper options, so that I can combine query results in various ways.

#### Acceptance Criteria

1. WHEN I use `dataset1.union(dataset2, all: true)` THEN it SHALL generate `UNION ALL`
2. WHEN I use `dataset1.intersect(dataset2)` THEN it SHALL generate `INTERSECT`
3. WHEN I use `dataset1.except(dataset2)` THEN it SHALL generate `EXCEPT`
4. WHEN I chain multiple set operations THEN they SHALL be properly parenthesized and ordered

### Requirement 7: Advanced Expression Support

**User Story:** As a developer using Sequel with DuckDB, I want to use advanced SQL expressions and operators, so that I can leverage DuckDB's full analytical capabilities.

#### Acceptance Criteria

1. WHEN I use CASE expressions THEN they SHALL generate proper CASE WHEN syntax
2. WHEN I use array operations THEN they SHALL use DuckDB's array syntax
3. WHEN I use JSON operations THEN they SHALL use DuckDB's JSON functions
4. WHEN I use mathematical and statistical functions THEN they SHALL generate appropriate DuckDB function calls

### Requirement 8: Query Hint and Optimization Support

**User Story:** As a developer using Sequel with DuckDB, I want to provide query hints and optimization directives, so that I can fine-tune query performance for specific use cases.

#### Acceptance Criteria

1. WHEN I provide query hints THEN they SHALL be properly integrated into generated SQL
2. WHEN I specify optimization preferences THEN they SHALL be respected in query generation
3. WHEN I use DuckDB-specific optimizations THEN they SHALL be available through the adapter
4. WHEN I need to bypass optimizations THEN options SHALL be available to do so

### Requirement 9: Error Handling for Advanced Features

**User Story:** As a developer using advanced SQL features with the sequel-duckdb adapter, I want clear error messages when features are not supported or used incorrectly, so that I can debug and fix issues quickly.

#### Acceptance Criteria

1. WHEN I use unsupported advanced features THEN clear error messages SHALL indicate what is not supported
2. WHEN I use features incorrectly THEN error messages SHALL provide guidance on correct usage
3. WHEN DuckDB returns errors for advanced queries THEN they SHALL be properly mapped to Sequel exceptions
4. WHEN feature limitations are encountered THEN workarounds SHALL be suggested where possible