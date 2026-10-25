# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **copy:** `Database#copy_to` and the new `Dataset#copy_to_sql` write partition columns
  as quoted identifiers. `partition_by: "year"` gave `PARTITION_BY YEAR` and now gives
  `PARTITION_BY ("year")`. DuckDB runs both forms, so the result does not change. Tests
  that compare COPY strings must update their expected strings. Word values are
  unchanged, for example `FORMAT PARQUET` and `COMPRESSION ZSTD`. Values that are not a
  single word, such as `delimiter: "|"`, are now quoted strings. The old text for those
  values was a DuckDB syntax error.
- **adapter (breaking):** TIME and TIME_NS result values are now `Sequel::SQLTime`, not
  `Time`. Sequel's postgres and sqlite adapters use the same class for a time of day. The
  value keeps the hour, minute, second and fraction, and TIME_NS keeps its nanoseconds. A
  value assigned to a TIME model column is also a `Sequel::SQLTime` on today's date. It
  was a `Time` on 1970-01-01 before. So a model that is assigned the time it already has
  no longer reports the column as changed. A `Sequel::SQLTime` in a query is written as
  `'HH:MM:SS.ffffff'`, so writes keep the fraction. `Sequel::SQLTime` is a subclass of
  `Time`, so `is_a?(Time)` is still true. Code that compares the class exactly with
  `Time`, or that expects the date 1970-01-01, must change.
- **adapter (breaking):** TIMETZ columns keep their UTC offset. `db.schema` reports
  them as `:time_timezone`, not `:time`. Result values are the new class
  `Sequel::DuckDB::SQLTimeTZ`, a subclass of `Sequel::SQLTime` on today's date that
  keeps the offset. A value assigned to a TIMETZ model column becomes the same class,
  so assigning a row its own value no longer reports the column as changed. A
  `SQLTimeTZ` in a query is written as `'HH:MM:SS.ffffff+HH:MM'`. Before, a model wrote
  a TIMETZ value without its offset, so DuckDB applied the session time zone:
  08:09:10.5+02:00 was stored as 08:09:10.5-07:00. Two values with the same instant
  and different offsets are not equal. A plain `Time` in a dataset query is still
  written as a timestamp without an offset.
- **adapter:** a `Time` dated 1970-01-01 in a query is now written as a timestamp, for
  example `'1970-01-01 09:30:00'`, like any other `Time`. It was written as the time of
  day `'09:30:00'` before. DuckDB casts both forms to the same TIME value, so a
  comparison with a TIME column gives the same result. Use `Sequel::SQLTime` for a time
  of day.

### Added

- **capabilities:** the adapter now declares Sequel's flags for DISTINCT ON, GROUP BY
  ROLLUP, CUBE and GROUPING SETS, LATERAL subqueries, MERGE, the WINDOW clause, regular
  expressions, and `CREATE TABLE IF NOT EXISTS` / `DROP TABLE IF EXISTS`. Sequel refused
  most of these features before, or left the WINDOW clause out of the SQL. Negated
  regexps (`exclude(name: /x/)`) are now written as `NOT regexp_matches(...)`.
  `create_table?` and `drop_table?` now emit `IF NOT EXISTS` / `IF EXISTS`. Other SQL is
  unchanged.
- **adapter:** `Database#add_conversion_proc(type, callable = nil, &block)` registers a
  converter for result values of one DuckDB column type. Dataset fetches pass each
  non-NULL value of a column of that type to the converter. The converter receives the
  whole value, so a LIST of STRUCT arrives as one Array. `type` is the type name that
  ruby-duckdb reports, for example `:time` or `:list`. A column with a type alias is
  looked up by its alias first. JSON columns report the type `:varchar` with the alias
  `JSON`, so you register them as `:json`. A column type without a converter keeps the
  value that ruby-duckdb returns. The adapter registers converters for `:time` and
  `:time_ns`, and a registration of your own replaces them.
- **extensions:** the opt-in `duckdb_array` extension writes and reads DuckDB LIST and
  ARRAY columns, as Sequel's `pg_array` does for PostgreSQL. `Sequel.duckdb_array([1, 2])`
  returns a `Sequel::DuckDB::DuckDBArray`, which Sequel writes as the list `[1, 2]`. A
  plain Ruby Array still means an IN list or a condition. Load the extension with
  `DB.extension :duckdb_array`. LIST and ARRAY result values are then `DuckDBArray`
  objects. A value nested inside another list, struct or map is not wrapped. `db.schema`
  reports an `INTEGER[]` or `INTEGER[3]` column as `:integer_array`. A list of an element
  type that has no Sequel type, for example a STRUCT, is reported as `:array`. A model
  typecasts an assigned Array to a `DuckDBArray` and typecasts each element to the
  element type. Without the extension, the SQL and the values do not change.
- **extensions:** the opt-in `duckdb_struct` extension writes and reads DuckDB STRUCT
  columns. Sequel's `pg_row` is the PostgreSQL counterpart. DuckDB structs have no type
  name, so you do not register a type. `Sequel.duckdb_struct(a: 1, b: "x")` returns a
  `Sequel::DuckDB::DuckDBStruct` with Symbol keys. Sequel writes it as the struct
  `{'a': 1, 'b': 'x'}`. A plain Ruby Hash still means a condition. A struct or list
  inside a struct is written only when it is a wrapper too. Load the extension with
  `DB.extension :duckdb_struct`. STRUCT result values are then `DuckDBStruct` objects.
  `db.schema` reports a STRUCT column as `:struct`. A model typecasts an assigned Hash to
  a `DuckDBStruct`. With `duckdb_array` also loaded, a list of structs is reported as
  `:struct_array`. Without the extension, the SQL and the values do not change.
- **extensions:** the opt-in `duckdb_map` extension writes and reads DuckDB MAP columns.
  Sequel's `pg_hstore` is the PostgreSQL counterpart, but a DuckDB MAP has typed keys
  and values. `Sequel.duckdb_map("k" => 1)` returns a `Sequel::DuckDB::DuckDBMap`, which
  Sequel writes as the map `MAP {'k': 1}`. An empty Hash gives `MAP {}`. The keys keep
  their Ruby type, so an INTEGER-keyed map has Integer keys. A plain Ruby Hash still
  means a condition. Load the extension with `DB.extension :duckdb_map`. MAP result
  values are then `DuckDBMap` objects. `db.schema` reports a MAP column as `:map`. A
  model typecasts an assigned Hash to a `DuckDBMap`. With `duckdb_array` also loaded, a
  list of maps is reported as `:map_array`. Without the extension, the SQL and the
  values do not change.
- **extensions:** the opt-in `duckdb_json` extension writes and reads DuckDB JSON
  columns, as Sequel's `pg_json` does for PostgreSQL. `Sequel.duckdb_json("a" => 1)`
  returns a `Sequel::DuckDB::DuckDBJSONHash`, and an Array gives a `DuckDBJSONArray`.
  Sequel writes each one as JSON text cast to JSON, for example `'{"a":1}'::JSON`. A
  plain Ruby Hash or Array still means a condition or an IN list. Load the extension
  with `DB.extension :duckdb_json`. JSON result values are then parsed. A JSON object
  becomes a `DuckDBJSONHash` with String keys, and a JSON array becomes a
  `DuckDBJSONArray`. A JSON string, number, boolean or null becomes the plain Ruby
  value. A VARCHAR column is not parsed, even when it holds JSON text. `db.schema`
  reports a JSON column as `:json`. A model typecasts an assigned Hash or Array to the
  wrapper and parses an assigned String as JSON. With `duckdb_array` also loaded, a
  JSON list is reported as `:json_array`. Without the extension, the SQL and the values
  do not change.

### Fixed

- **dataset:** a CTE now works in INSERT, UPDATE and DELETE. `supports_cte?` took no
  argument, so an INSERT from a dataset with a CTE raised `ArgumentError`. An UPDATE or a
  DELETE on a dataset with a CTE left the WITH clause out of the SQL, so DuckDB could not
  find the CTE's table. All three statements now start with the WITH clause.
- **connection:** a frozen `Sequel::Database` can open and close connections and drop a
  schema. Sequel recommends `Database#freeze` for production, but every disconnect and
  every `drop_schema` raised `FrozenError` after it, and later queries failed.
- **connection:** the `config:` option was documented but ignored. Its DuckDB settings,
  for example `config: { threads: 4, memory_limit: "2GB" }`, now reach DuckDB. A file that
  is already open in this process keeps the settings of the first `Sequel::Database` that
  opened it. Opening it with different settings raises `Sequel::DatabaseConnectionError`,
  as DuckDB's Python, Java and R clients do. No `config:` and some `config:` count as
  different.
- **schema:** `table_exists?` returned false for some tables that exist: a
  `Sequel.identifier` name, an unqualified name on the `search_path`, a table in an
  attached catalog, and a `Sequel.identifier` name with the `schema:` option. DuckDB now
  resolves the name, as it does for a query. `schema` and `indexes` raised "does not
  exist" for the same names. They now read the table or view that a query with that
  name reads, including a temporary table that has the name of a `main` table.
- **schema:** `db.schema` reported type `:string` for FLOAT and REAL, unsigned and
  128-bit integer, VARINT, TIMESTAMPTZ, TIMESTAMP_S/MS/NS, TIMETZ, TIME_NS and INTERVAL
  columns. Sequel models then typecast an assigned value to a String, for example 1.5 to
  "1.5" for a FLOAT column. These columns now report `:float`, `:integer`, `:bigint`,
  `:datetime`, `:time` or `:interval`. List, struct, map, union, enum, JSON and BIT
  columns still report `:string`.
- **schema:** a schema, table or catalog name with a single quote in it broke
  `tables`, `views`, `table_exists?`, `schema`, `indexes`, `schemas` and
  `schema_exists?` with a syntax error. `set_pragma` had the same defect for a value that
  is not a String, Boolean or Number.
- **date_arithmetic:** a query that held both `Sequel.date_add(d, days: 30)` and
  `Sequel.date_add(d, months: 1)` returned the same date for both. DuckDB judges
  `INTERVAL 30 DAY` equal to `INTERVAL 1 MONTH`, and its optimizer computed only one of
  the two expressions. The same happened with 360 days and 1 year, and inside aggregates.
  Year and month intervals are now written as a subtraction of the negated value:
  `months: 1` gives `"d" - INTERVAL (-1) MONTH`, and `months: -1` gives
  `"d" - INTERVAL 1 MONTH`. The same merge happened between days and hours of equal
  length, for example 1 day and 24 hours, which differ for a TIMESTAMPTZ across a
  daylight-saving change. Hour, minute and second intervals are now written with the
  interval first: `hours: 24` gives `INTERVAL 24 HOUR + "d"`. Day intervals are
  unchanged. Tests that compare date arithmetic SQL strings must update their expected
  strings for every unit except days.
- **connection:** two `Sequel::Database` objects on one DuckDB file in one process opened
  two DuckDB instances, and writes through one did not reliably reach the other. Every
  `Sequel::Database` on a file now shares one DuckDB instance. `Database#disconnect` now
  gives the file back once that database has no open connection. The file closes when no
  `Sequel::Database` in the process uses it, and the next query opens it again.
  In-memory databases are unchanged: each is separate and stays open after `disconnect`.

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
