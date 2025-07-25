# Implementation Plan

- [x] 1. Write tests for LiteralString handling
  - Add test in `test/sql_test.rb` to verify `Sequel.lit()` expressions are not quoted
  - Test LiteralString in SELECT clause: `db[:test].select(Sequel.lit('YEAR(created_at)')).sql`
  - Test LiteralString in WHERE clause: `db[:test].where(Sequel.lit('age > 18')).sql`
  - Add regression test to ensure regular strings are still quoted properly
  - Add test to ensure SQL::Function continues working (should be unchanged)
  - _Requirements: 1.1, 1.2, 4.1, 6.1_

- [x] 2. Fix literal_append method to handle LiteralString
  - Modify existing `literal_append` method in `lib/sequel/adapters/shared/duckdb.rb`
  - Add `LiteralString` check as special case of `String` following Sequel core pattern
  - Change `when String` to nested case with `when LiteralString` that does `sql << v`
  - Preserve all existing logic for Time, DateTime, and binary strings
  - _Requirements: 1.1, 4.1, 4.2, 4.4, 6.1_

- [x] 3. Add integration test with real database
  - Add test in `test/dataset_test.rb` using real DuckDB database
  - Test that LiteralString expressions actually execute correctly
  - Verify no regression in existing database operations
  - _Requirements: 1.2, 5.1, 6.1_
