# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Performance optimization documentation
- Migration examples and patterns
- Comprehensive API documentation with YARD
- Advanced error handling with specific exception mapping
- Support for DuckDB-specific features (JSON, arrays, window functions)

### Changed
- Enhanced README with comprehensive usage examples
- Improved documentation structure and organization

## [0.1.0] - 2025-07-21

### Added
- Initial release of Sequel DuckDB adapter
- Complete Database and Dataset class implementation
- Connection management for file-based and in-memory databases
- Full SQL generation for SELECT, INSERT, UPDATE, DELETE operations
- Schema introspection (tables, columns, indexes, constraints)
- Data type mapping between Ruby and DuckDB types
- Transaction support with commit/rollback capabilities
- Comprehensive error handling and exception mapping
- Performance optimizations for analytical workloads
- Support for DuckDB-specific SQL features:
  - Window functions
  - Common Table Expressions (CTEs)
  - Array and JSON data types
  - Analytical functions and aggregations
- Bulk operations support (multi_insert, batch processing)
- Connection pooling and memory management
- Comprehensive test suite with 100% coverage
- YARD documentation for all public APIs
- Migration examples and best practices
- Performance tuning guide

### Database Features
- File-based database support with automatic creation
- In-memory database support for testing and temporary data
- Connection validation and automatic reconnection
- Proper connection cleanup and resource management
- Support for DuckDB configuration options (memory_limit, threads, etc.)

### SQL Generation
- Complete SQL generation for all standard operations
- DuckDB-optimized query generation
- Support for complex queries with JOINs, subqueries, and CTEs
- Window function support for analytical queries
- Proper identifier quoting and SQL injection prevention
- Parameter binding for prepared statements

### Schema Operations
- Table creation, modification, and deletion
- Column operations (add, drop, modify, rename)
- Index management (create, drop, unique, partial indexes)
- Constraint support (PRIMARY KEY, FOREIGN KEY, UNIQUE, CHECK, NOT NULL)
- View creation and management
- Schema introspection with detailed metadata

### Data Types
- Complete Ruby ↔ DuckDB type mapping
- Support for all standard SQL types
- DuckDB-specific types (JSON, ARRAY, MAP)
- Proper handling of NULL values and defaults
- Date/time type conversion with timezone support
- Binary data (BLOB) support
- UUID type support

### Performance Features
- Columnar storage optimization awareness
- Vectorized execution support
- Memory-efficient result set processing
- Bulk insert optimizations
- Connection pooling for concurrent access
- Query plan analysis support (EXPLAIN)
- Streaming result sets for large datasets

### Error Handling
- Comprehensive error mapping to Sequel exceptions
- Detailed error messages with context
- Proper handling of constraint violations
- Connection error recovery
- SQL syntax error reporting
- Database-specific error categorization

### Testing
- Complete test suite using Minitest
- Mock database testing for SQL generation
- Integration testing with real DuckDB databases
- Performance benchmarking tests
- Error condition testing
- Schema operation testing
- Data type conversion testing

### Documentation
- Comprehensive README with usage examples
- Complete API documentation with YARD
- Migration examples and patterns
- Performance optimization guide
- Troubleshooting documentation
- Version compatibility matrix

### Dependencies
- Ruby 3.1.0+ support
- Sequel 5.0+ compatibility
- DuckDB 0.8.0+ support
- ruby-duckdb 1.0.0+ integration

### Fixed
- Proper adapter registration with Sequel
- Connection string parsing for file paths
- Memory management for large result sets
- Transaction rollback handling
- Schema introspection edge cases
- Data type conversion accuracy
- Error message formatting and context

### Security
- SQL injection prevention through parameter binding
- Proper identifier quoting
- Connection string sanitization
- File path validation for database files
- Read-only database connection support
