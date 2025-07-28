# Implementation Plan

- [x] 1. Identify failing tests and analyze DuckDB SQL generation
  - Run existing test suite to identify tests failing due to SQL syntax differences
  - Analyze what SQL the adapter currently generates vs what tests expect
  - Document the specific DuckDB syntax patterns that are being generated
  - Determine if adapter changes are needed or just test expectation updates
  - _Requirements: 7.1, 7.2_

- [x] 2. Fix LIKE clause ESCAPE handling in adapter
  - Current issue: LIKE generates `(name LIKE '%John%' ESCAPE '\')` instead of `(name LIKE '%John%')`
  - Override LIKE handling in `complex_expression_sql_append` to remove ESCAPE clause
  - Ensure LIKE clauses generate clean `(name LIKE '%John%')` without ESCAPE clause
  - Write tests to verify LIKE functionality works correctly without the ESCAPE clause
  - _Requirements: 1.1, 1.2, 1.4_

- [x] 3. Fix parentheses in complex expression SQL generation
  - Current issue: ILIKE generates `UPPER(name) LIKE UPPER('%john%')` without outer parentheses
  - Current issue: Regex generates `name ~ 'pattern'` without outer parentheses
  - Update `complex_expression_sql_append` to add proper parentheses around expressions
  - Ensure ILIKE generates `(UPPER(name) LIKE UPPER('%john%'))` with parentheses
  - Ensure regex generates `(name ~ '^John')` with parentheses around the expression
  - Write tests to verify all complex expressions have consistent parentheses
  - _Requirements: 1.3, 5.1, 5.2, 5.3, 5.4_

- [x] 4. ~~Fix table alias syntax to use AS instead of triple underscore~~ (REMOVED - Not a core Sequel feature)
  - The `table___alias` syntax is not a core Sequel feature and requires an extension
  - Removed implementation and tests as this is not standard Sequel functionality
  - Standard Sequel table aliases use `.as()` method: `db[:users].as(:u)`

- [x] 5. Fix qualified column references to use dot notation
  - Current issue: Qualified columns generate `users__id` instead of `users.id`
  - Override qualified identifier handling to generate proper dot notation
  - Implement `qualified_identifier_sql_append` method to use dot notation
  - Ensure subqueries use correct `users.id` column reference format
  - Write tests to verify qualified column references work correctly in complex queries
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 6.1, 6.2, 6.3, 6.4_

- [x] 6. Fix SQL test infrastructure issues
  - Current issue: SQL tests failing with dataset type assertion errors (`Expected #<Sequel::Dataset::_Subclass>`)
  - Fix `SqlTest` class to use proper dataset creation instead of mock subclasses
  - Update SQL generation tests to work with actual DuckDB adapter behavior
  - Ensure tests create proper datasets for SQL generation testing
  - Fix dataset type assertion issues in SQL tests
  - _Requirements: 6.1, 6.2_

- [x] 7. Fix recursive CTE SQL generation
  - Current issue: `WITH RECURSIVE` generates incorrect SQL without RECURSIVE keyword
  - Implement proper recursive CTE handling in dataset methods
  - Ensure recursive CTEs generate `WITH RECURSIVE` syntax correctly
  - Test recursive CTE functionality with proper SQL generation
  - _Requirements: 5.1, 5.2, 5.3, 5.4_

- [x] 8. Fix JOIN USING clause generation
  - Current issue: `JOIN USING` clause not generating USING syntax correctly
  - Implement proper USING clause handling in JOIN operations
  - Ensure JOIN USING generates correct `INNER JOIN table USING (column)` syntax
  - Test JOIN USING functionality with various column combinations
  - _Requirements: 4.1, 4.2, 4.3, 4.4_

- [x] 9. Fix regex functionality integration
  - Current issue: Regex matching returning 0 results instead of expected matches
  - Debug regex operator implementation in complex_expression_sql_append
  - Ensure regex patterns work correctly with DuckDB's regex syntax
  - Test regex functionality with actual data and pattern matching
  - _Requirements: 1.3, 5.1, 5.2, 5.3, 5.4_

- [x] 10. Fix model integration issues
  - Current issue: Model tests failing with type mapping and update detection
  - Fix time type mapping (currently returning :datetime instead of :time)
  - Fix model update detection to properly track changed fields
  - Ensure model DELETE operations work correctly with proper ID handling
  - _Requirements: 2.1, 2.2, 2.3, 2.4_

- [x] 11. Fix table creation and schema issues
  - Current issue: Tests failing because tables don't exist during SQL execution
  - Ensure proper table creation in test setup for integration tests
  - Fix primary key handling for tables without explicit primary keys
  - Handle NOT NULL constraint violations properly in INSERT operations
  - _Requirements: 3.1, 3.2, 3.3, 3.4_

- [x] 12. Verify all tests pass with consistent DuckDB SQL generation
  - Run complete test suite to ensure all SQL generation tests pass
  - Verify that adapter generates consistent, predictable SQL for DuckDB
  - Test integration scenarios to ensure functional correctness
  - Fix any remaining test expectation mismatches
  - _Requirements: 6.1, 6.2, 6.3, 6.4_

- [x] 13. Document DuckDB-specific SQL syntax patterns
  - Document the specific SQL syntax that the adapter generates for DuckDB
  - Provide examples of DuckDB SQL patterns used by the adapter
  - Explain why certain DuckDB syntax choices were made
  - Create reference documentation for developers using the adapter
  - _Requirements: 7.1, 7.2, 7.3_
