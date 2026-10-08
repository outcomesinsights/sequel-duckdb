# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1] - 2026-10-02

### Fixed

- Point homepage and changelog links at outcomesinsights/sequel-duckdb ([69fa0db](https://github.com/outcomesinsights/sequel-duckdb/commit/69fa0db723f6a579cc5451df450f568452968ce0))

## [0.3.0] - 2026-10-01

### Breaking changes

- Drop Ruby 3.2 support; require Ruby >= 3.3 ([9e0d2bd](https://github.com/outcomesinsights/sequel-duckdb/commit/9e0d2bda53aae129ee02773316e8c75a1142c480)): sequel-duckdb no longer installs on Ruby 3.2.

### Fixed

- Ship only lib/ and three docs; stop tracking the beads key ([c45dfc4](https://github.com/outcomesinsights/sequel-duckdb/commit/c45dfc4636917269f6333da1be2b21ed8855a0c2))

## [0.2.1] - 2026-10-01

No changes to the gem's behaviour. The first release published by the
tag-based release workflow.

## [0.2.0] - 2026-09-30

### ⚠ BREAKING CHANGES

- Removed parameterized query support, custom error handling methods, and other over-engineered features. Adapter now follows Sequel conventions using built-in features.
- Removed custom error message formatting methods (database_exception_message, database_exception_class, handle_constraint_violation) in favor of Sequel's built-in patterns.

### Features

- add Database#copy_to support ([17ce698](https://github.com/outcomesinsights/sequel-duckdb/commit/17ce6981fca43653ecd50c29f83b50bcf7e5b23b))
- add schema methods ([fcd11fd](https://github.com/outcomesinsights/sequel-duckdb/commit/fcd11fd99b267b925888ef8accfc7bc7dbad87ad))
- add support for date_arithmetic ([fc0dde7](https://github.com/outcomesinsights/sequel-duckdb/commit/fc0dde71c542f3a1cb23310a12b8529065b9baf6))
- additional options for create_view ([7f19ad2](https://github.com/outcomesinsights/sequel-duckdb/commit/7f19ad2efe3ea7c1074b5e9e8a1c918216958701))
- share DuckDB::Database instance and support cross-database schema queries ([564cb47](https://github.com/outcomesinsights/sequel-duckdb/commit/564cb4767471f0a5183e7219bf6008e57e644fec))
- support different read\_\* functions in CREATE VIEW ([98eba2e](https://github.com/outcomesinsights/sequel-duckdb/commit/98eba2ed486d1f775508ea4bac0dc6f7cd62a37e))

### Bug Fixes

- bump minimum Ruby to 3.2 and upgrade minitest to 6.x ([55b874d](https://github.com/outcomesinsights/sequel-duckdb/commit/55b874d96f49684f3a4f13eccb7c6af5fc83e6b8))
- **ci:** install DuckDB C library for native extension ([#7](https://github.com/outcomesinsights/sequel-duckdb/issues/7)) ([7af7437](https://github.com/outcomesinsights/sequel-duckdb/commit/7af7437c8b4db151e913087e558d3950bed34601))
- formatting for date_arithmetic ([835ad7e](https://github.com/outcomesinsights/sequel-duckdb/commit/835ad7e6a9de7fd0b6c441ee394aa8ff54cc0612))
- move DuckDB::Database init from connect to adapter_initialize ([e06e0f2](https://github.com/outcomesinsights/sequel-duckdb/commit/e06e0f2958b958ae368948a80234af058a24b7cd))
- remove hard duckdb C extension dependency from gemspec ([7c598ac](https://github.com/outcomesinsights/sequel-duckdb/commit/7c598ac53534c7d29567bdf6c2fe1170162fa216))
- use nested module syntax for Helpers to support mock adapter loading ([7962262](https://github.com/outcomesinsights/sequel-duckdb/commit/7962262dd9c45369815404a2c325f67f2820295f))

### Code Refactoring

- simplify adapter execution and error handling ([e812777](https://github.com/outcomesinsights/sequel-duckdb/commit/e812777acd815466cebbd58cc1f54b8254fcabfd))

### Tests

- remove tests for deleted features and fix remaining failures ([b056ec8](https://github.com/outcomesinsights/sequel-duckdb/commit/b056ec89ddc48957a35f8ecf302eddc07e47b959))

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

[0.1.0]: https://github.com/outcomesinsights/sequel-duckdb/releases/tag/v0.1.0
[0.2.0]: https://github.com/outcomesinsights/sequel-duckdb/compare/v0.1.0...v0.2.0
[0.2.1]: https://github.com/outcomesinsights/sequel-duckdb/compare/v0.2.0...v0.2.1
[0.3.0]: https://github.com/outcomesinsights/sequel-duckdb/compare/v0.2.1...v0.3.0
[0.3.1]: https://github.com/outcomesinsights/sequel-duckdb/compare/v0.3.0...v0.3.1
