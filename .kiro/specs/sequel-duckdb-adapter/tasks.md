# Implementation Plan

- [x] 1. Set up project structure following sequel-hexspace pattern
  - Create lib/sequel/adapters/duckdb.rb for main Database and Dataset classes
  - Create lib/sequel/adapters/shared/duckdb.rb for DatabaseMethods and DatasetMethods modules
  - Set up proper require structure and module organization
  - _Requirements: 10.3, 13.8_

- [x] 2. Implement basic connection management in shared module
  - [x] 2.1 Create DatabaseMethods module with connection handling
    - Implement connect method for file and in-memory databases
    - Implement disconnect_connection and valid_connection? methods
    - Add proper error handling for connection failures
    - _Requirements: 1.1, 1.2, 1.4, 1.6_

  - [x] 2.2 Create Database class with adapter registration
    - Define Sequel::DuckDB::Database class including DatabaseMethods
    - Set adapter scheme to :duckdb
    - Implement dataset_class_default method
    - Register adapter with Sequel
    - _Requirements: 1.1, 10.2, 13.1_

  - [ ] 2.3 Fix adapter registration (CRITICAL BUG)
    - Fix incorrect adapter registration in lib/sequel/adapters/duckdb.rb
    - Change from `Sequel::Database.set_shared_adapter_scheme :duckdb, self` to proper registration
    - Use `Database.adapter_scheme :duckdb, DuckDB::Database` pattern
    - _Requirements: 1.1, 13.1_

- [ ] 3. Set up comprehensive test infrastructure (CRITICAL - TDD REQUIREMENT)
  - [ ] 3.1 Create test infrastructure following sequel-hexspace pattern
    - Create test/all.rb test runner
    - Create test/spec_helper.rb with test configuration and DuckDB setup
    - Set up test database helpers and utilities for both mock and real DuckDB testing
    - Configure test environment with proper require statements
    - _Requirements: 11.8, 10.4, 13.1_

  - [ ] 3.2 Create core test files with initial structure
    - Create test/database_test.rb for connection and basic functionality tests
    - Create test/dataset_test.rb for comprehensive SQL generation testing
    - Create test/schema_test.rb for schema operations and introspection
    - Create test/sql_test.rb for SQL generation and syntax verification
    - Create test/type_test.rb for data type handling and conversion
    - _Requirements: 11.1, 11.2, 11.4, 11.3_

- [ ] 4. Implement basic SQL generation in shared module (TDD - TESTS FIRST)
  - [ ] 4.1 Write tests for core SQL generation methods
    - Write comprehensive tests for select_sql method using mock database
    - Write tests for insert_sql method with various parameter combinations
    - Write tests for update_sql method with WHERE clauses
    - Write tests for delete_sql method with conditions
    - Ensure all tests fail initially (Red phase of TDD)
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 11.1, 11.2_

  - [ ] 4.2 Implement DatasetMethods module with core SQL generation
    - Implement select_sql method for basic SELECT statements
    - Implement insert_sql method for INSERT operations
    - Implement update_sql method for UPDATE operations
    - Implement delete_sql method for DELETE operations
    - Ensure all tests pass (Green phase of TDD)
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

  - [ ] 4.3 Write tests for Dataset class functionality
    - Write tests for fetch_rows method using real DuckDB in-memory database
    - Write tests for DuckDB capability flags (window functions, CTE support, etc.)
    - Write integration tests for basic query execution
    - _Requirements: 2.1, 6.1, 6.2, 11.3_

  - [ ] 4.4 Implement Dataset class with shared functionality
    - Implement fetch_rows method for query execution
    - Add DuckDB capability flags (window functions, CTE support, etc.)
    - Ensure proper integration with DatabaseMethods
    - _Requirements: 2.1, 6.1, 6.2_

- [ ] 5. Implement data type handling and literal conversion (TDD - TESTS FIRST)
  - [ ] 5.1 Write tests for literal conversion methods
    - Write tests for literal_string_append with string escaping scenarios
    - Write tests for literal_date, literal_datetime, literal_time methods
    - Write tests for literal_boolean method with true/false values
    - Write tests for NULL value handling and edge cases
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.8, 11.4_

  - [ ] 5.2 Add literal conversion methods to DatasetMethods
    - Implement literal_string_append for string escaping
    - Implement literal_date, literal_datetime, literal_time methods
    - Implement literal_boolean method for boolean values
    - Add support for NULL value handling
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.8_

  - [ ] 5.3 Write tests for binary data and numeric type support
    - Write tests for BLOB type mapping for binary data
    - Write tests for integer and float type handling
    - Write tests for Ruby to DuckDB type conversion edge cases
    - _Requirements: 3.2, 3.3, 3.9, 11.4_

  - [ ] 5.4 Add binary data and numeric type support
    - Implement BLOB type mapping for binary data
    - Add proper integer and float type handling
    - Ensure proper Ruby to DuckDB type conversion
    - _Requirements: 3.2, 3.3, 3.9_

- [ ] 6. Implement schema introspection in DatabaseMethods (TDD - TESTS FIRST)
  - [ ] 6.1 Write tests for schema introspection methods
    - Write tests for schema_parse_tables method for table listing
    - Write tests for schema_parse_table method for column information
    - Write tests for schema_parse_indexes method for index introspection
    - Write tests for views and foreign key detection
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 11.5_

  - [ ] 6.2 Add table and schema discovery methods
    - Implement schema_parse_tables method for table listing
    - Implement schema_parse_table method for column information
    - Implement schema_parse_indexes method for index introspection
    - Add support for views and foreign key detection
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6_

  - [ ] 6.3 Write tests for schema metadata methods
    - Write tests for tables method using schema_parse_tables
    - Write tests for schema method using schema_parse_table
    - Write tests for indexes method using schema_parse_indexes
    - Write tests for column default and nullable status reporting
    - _Requirements: 4.7, 4.8, 11.5_

  - [ ] 6.4 Add schema metadata methods
    - Implement tables method using schema_parse_tables
    - Implement schema method using schema_parse_table
    - Implement indexes method using schema_parse_indexes
    - Add proper column default and nullable status reporting
    - _Requirements: 4.7, 4.8_

- [ ] 7. Implement transaction support in DatabaseMethods
  - [ ] 7.1 Add basic transaction methods
    - Implement transaction block handling
    - Add automatic commit on successful completion
    - Add automatic rollback on exceptions
    - Implement explicit rollback method
    - _Requirements: 5.1, 5.2, 5.3, 5.4_

  - [ ] 6.2 Add advanced transaction features
    - Implement savepoint support if available in DuckDB
    - Add transaction isolation level support
    - Implement manual transaction control for autocommit mode
    - _Requirements: 5.5, 5.6, 5.7_

- [ ] 8. Implement advanced SQL generation features
  - [ ] 8.1 Add complex query support to DatasetMethods
    - Implement proper WHERE clause generation
    - Add ORDER BY, LIMIT, and OFFSET support
    - Implement GROUP BY and HAVING clause generation
    - Add JOIN statement generation
    - _Requirements: 6.4, 6.5, 6.6, 6.7, 6.8, 6.9_

  - [ ] 8.2 Add DuckDB-specific SQL features
    - Implement window function support
    - Add Common Table Expression (CTE) support
    - Implement subquery generation
    - Add aggregate function support
    - _Requirements: 2.6, 2.7, 2.8_

- [ ] 9. Implement SQL execution methods in DatabaseMethods
  - [ ] 9.1 Add core SQL execution methods
    - Implement execute method with connection synchronization
    - Implement execute_insert and execute_update methods
    - Add execute_statement private method for actual SQL execution
    - Implement proper result handling and iteration
    - _Requirements: 2.1, 2.2, 2.3, 2.4_

  - [ ] 9.2 Add dataset operation support
    - Implement count, first, and all methods in DatasetMethods
    - Add proper result set handling and conversion
    - Implement streaming result support where possible
    - _Requirements: 6.1, 6.2, 6.3, 9.5_

- [ ] 10. Implement error handling and logging
  - [ ] 10.1 Add error mapping in DatabaseMethods
    - Implement database_error_classes method
    - Add database_exception_sqlstate method for SQL state extraction
    - Map DuckDB errors to appropriate Sequel exceptions
    - Implement proper constraint violation error handling
    - _Requirements: 8.1, 8.2, 8.3, 8.7_

  - [ ] 9.2 Add logging and debugging support
    - Implement SQL query logging using Sequel's logging mechanism
    - Add timing information for slow operations
    - Implement connection pooling error handling
    - Add EXPLAIN functionality access for query plans
    - _Requirements: 8.4, 8.5, 8.6, 9.6_

- [ ] 11. Implement performance optimizations
  - [ ] 11.1 Add efficient result fetching
    - Optimize fetch_rows method for large result sets
    - Implement prepared statement support if beneficial
    - Add bulk insert optimization methods
    - Implement efficient connection pooling
    - _Requirements: 9.1, 9.2, 9.3, 9.4_

  - [ ] 10.2 Add memory and query optimizations
    - Implement streaming result options for memory efficiency
    - Add index-aware query generation
    - Optimize for DuckDB's columnar storage advantages
    - Implement parallel query execution support
    - _Requirements: 9.5, 9.7_



- [ ] 12. Implement Sequel::Model integration support
  - [ ] 12.1 Add model compatibility methods
    - Ensure automatic schema introspection works with models
    - Implement proper INSERT statement generation for model creation
    - Add UPDATE statement generation for model updates
    - Implement DELETE statement generation for model deletion
    - _Requirements: 7.1, 7.2, 7.3, 7.4_

  - [ ] 12.2 Add association and validation support
    - Implement foreign key relationship handling for associations
    - Ensure model validations work correctly with DuckDB constraints
    - Add model callback support during DuckDB operations
    - _Requirements: 7.5, 7.6, 7.7_

- [ ] 13. Create documentation and examples
  - [ ] 13.1 Write comprehensive README
    - Create usage examples with connection strings
    - Add sample code for common operations
    - Document DuckDB-specific features and optimizations
    - Include troubleshooting section for common issues
    - _Requirements: 12.1, 12.2, 12.5_

  - [ ] 13.2 Add API documentation and migration examples
    - Generate complete YARD documentation for all public methods
    - Create Sequel migration examples for DuckDB
    - Document performance tuning techniques
    - Add version compatibility documentation
    - _Requirements: 12.2, 12.3, 12.4, 12.6_

- [ ] 14. Final integration and compatibility verification
  - [ ] 14.1 Verify Sequel conventions compliance
    - Ensure adapter follows Sequel's standard exception hierarchy
    - Verify configuration options follow Sequel patterns
    - Test compatibility with Ruby 3.1+ requirements
    - Validate gem packaging follows Ruby conventions
    - _Requirements: 13.2, 13.3, 13.4, 13.5, 13.7_

  - [ ] 14.2 Complete end-to-end testing
    - Run comprehensive test suite with real DuckDB databases
    - Verify all SQL generation produces valid DuckDB syntax
    - Test performance with large datasets
    - Validate memory usage and connection handling
    - _Requirements: 11.7, 9.1, 9.4, 9.5_