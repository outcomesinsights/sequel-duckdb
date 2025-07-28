# Implementation Plan

Most advanced SQL features are already implemented. Remaining tasks based on current code analysis:

- [x] 1. Test window functions
  - ~~Add tests to `test/dataset_test.rb` for ROW_NUMBER, RANK, DENSE_RANK~~ ✅ Already implemented
  - ~~Test LAG/LEAD functions with offset and default parameters~~ ✅ Already implemented
  - _Requirements: 1.1, 1.2_

- [x] 2. Test advanced expressions
  - Add tests to `test/sql_test.rb` for DuckDB array syntax `[1, 2, 3]`
  - Test JSON functions like `json_extract`
  - _Requirements: 2.1, 2.2_

- [x] 3. Add configuration convenience methods
  - Add `set_pragma(key, value)` method to `DatabaseMethods` (user-friendly wrapper)
  - Add `configure_duckdb(options)` method for batch configuration
  - Add tests to `test/database_test.rb`
  - _Requirements: 3.1, 3.2_

- [x] 4. Integration testing
  - ~~Add integration tests to existing test files using actual DuckDB databases~~ ✅ Already implemented
  - ~~Test that all advanced features work together correctly~~ ✅ Already implemented
  - _Requirements: 4.1, 4.2_