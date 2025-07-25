# Implementation Plan

- [x] 1. Identify failing tests and analyze DuckDB SQL generation
  - Run existing test suite to identify tests failing due to SQL syntax differences
  - Analyze what SQL the adapter currently generates vs what tests expect
  - Document the specific DuckDB syntax patterns that are being generated
  - Determine if adapter changes are needed or just test expectation updates
  - _Requirements: 7.1, 7.2_

- [ ] 2. Fix LIKE clause ESCAPE handling in adapter
  - Current issue: LIKE generates `(name LIKE '%John%' ESCAPE '\')` instead of `(name LIKE '%John%')`
  - Override LIKE handling in `complex_expression_sql_append` to remove ESCAPE clause
  - Ensure LIKE clauses generate clean `(name LIKE '%John%')` without ESCAPE clause
  - Write tests to verify LIKE functionality works correctly without the ESCAPE clause
  - _Requirements: 1.1, 1.2, 1.4_

- [ ] 3. Fix parentheses in complex expression SQL generation
  - Current issue: ILIKE generates `UPPER(name) LIKE UPPER('%john%')` without outer parentheses
  - Current issue: Regex generates `name ~ 'pattern'` without outer parentheses
  - Update `complex_expression_sql_append` to add proper parentheses around expressions
  - Ensure ILIKE generates `(UPPER(name) LIKE UPPER('%john%'))` with parentheses
  - Ensure regex generates `(name ~ '^John')` with parentheses around the expression
  - Write tests to verify all complex expressions have consistent parentheses
  - _Requirements: 1.3, 5.1, 5.2, 5.3, 5.4_

- [ ] 4. Fix table alias syntax to use AS instead of triple underscore
  - Current issue: Table aliases generate `users___u` instead of `users AS u`
  - Override table alias handling in DatasetMethods to generate proper AS syntax
  - Implement `table_alias_sql_append` method to use standard `AS` syntax
  - Ensure JOIN operations use correct `users AS u` alias format
  - Write tests to verify alias functionality works correctly with AS syntax
  - _Requirements: 3.1, 3.2, 3.3, 3.4_

- [ ] 5. Fix qualified column references to use dot notation
  - Current issue: Qualified columns generate `users__id` instead of `users.id`
  - Override qualified identifier handling to generate proper dot notation
  - Implement `qualified_identifier_sql_append` method to use dot notation
  - Ensure subqueries use correct `users.id` column reference format
  - Write tests to verify qualified column references work correctly in complex queries
  - _Requirements: 4.1, 4.2, 4.3, 4.4, 6.1, 6.2, 6.3, 6.4_

- [ ] 6. Fix SQL test infrastructure issues
  - Current issue: SQL tests failing with dataset type assertion errors
  - Fix `SqlTest` class to use proper dataset creation instead of mock subclasses
  - Update SQL generation tests to work with actual DuckDB adapter behavior
  - Ensure tests create proper datasets for SQL generation testing
  - Fix dataset type assertion issues in SQL tests
  - _Requirements: 7.1, 7.2_

- [ ] 7. Update boolean comparison test expectations (if needed)
  - Investigate if boolean comparison tests need updates for DuckDB syntax
  - Modify boolean comparison tests to accept both `=` and `IS` for boolean values if needed
  - Update tests to expect DuckDB's preference for `=` over `IS` in some cases if applicable
  - Use flexible assertion helper to accept both valid boolean comparison syntaxes
  - Test various boolean scenarios with correct DuckDB syntax expectations
  - _Requirements: 2.1, 2.2, 2.3, 2.4_

- [ ] 8. Verify all tests pass with consistent DuckDB SQL generation
  - Run complete test suite to ensure all SQL generation tests pass
  - Verify that adapter generates consistent, predictable SQL for DuckDB
  - Test integration scenarios to ensure functional correctness
  - Fix any remaining test expectation mismatches
  - _Requirements: 7.1, 7.2, 7.3_

- [ ] 9. Document DuckDB-specific SQL syntax patterns
  - Document the specific SQL syntax that the adapter generates for DuckDB
  - Provide examples of DuckDB SQL patterns used by the adapter
  - Explain why certain DuckDB syntax choices were made
  - Create reference documentation for developers using the adapter
  - _Requirements: 8.1, 8.2, 8.3, 8.4_