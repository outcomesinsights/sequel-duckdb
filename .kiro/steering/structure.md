# Project Structure

## Root Directory

- **Gemfile**: Dependency specification
- **Rakefile**: Build tasks and automation
- **sequel-duckdb.gemspec**: Gem specification
- **.rubocop.yml**: Code style configuration
- **README.md**: Project documentation
- **CHANGELOG.md**: Version history

## Core Library Structure (Following sequel-hexspace Pattern)

```text
lib/
└── sequel/
    └── adapters/
        ├── duckdb.rb              # Main adapter file (Database & Dataset classes)
        └── shared/
            └── duckdb.rb          # Shared DuckDB-specific functionality
```

## Organization Patterns

- **Namespace**: `Sequel::DuckDB` - follows Sequel's adapter pattern exactly like sequel-hexspace
- **Adapter Structure**: Main adapter in `lib/sequel/adapters/duckdb.rb` with Database and Dataset classes
- **Shared Functionality**: Common methods in `lib/sequel/adapters/shared/duckdb.rb` with DatabaseMethods and DatasetMethods modules
- **Database Class**: `Sequel::DuckDB::Database` includes `Sequel::DuckDB::DatabaseMethods` for connection management
- **Dataset Class**: `Sequel::DuckDB::Dataset` includes `Sequel::DuckDB::DatasetMethods` for SQL generation
- **Module Pattern**: Use include pattern exactly like sequel-hexspace to separate concerns between main classes and shared functionality

## Development Structure

- **bin/**: Development executables (console, setup)
- **test/**: Comprehensive test suite following sequel-hexspace pattern
- **sig/**: RBS type signatures for Ruby 3+ type checking
- **.kiro/**: AI assistant configuration and steering rules

## File Naming Conventions

- Use snake_case for file names
- Match file names to adapter names (duckdb.rb for DuckDB adapter)
- Follow sequel-hexspace directory structure exactly
- Standard Ruby gem layout with adapter-specific organization

## Key Architectural Decisions

- **Database Adapter Pattern**: Full Sequel database adapter implementation following sequel-hexspace
- **Reference-Driven Design**: Mirror sequel-hexspace structure and patterns exactly
- **Test-First Development**: Comprehensive test coverage before implementation
- **Incremental Implementation**: Small, clearly defined implementation pieces
- **AI-Agent Friendly**: Thorough documentation for autonomous development

## Implementation Order (based on sequel-hexspace git history)

1. **Connection Management**: Database connection and basic connectivity
2. **Schema Operations**: Table creation, modification, introspection
3. **Basic SQL Generation**: SELECT, INSERT, UPDATE, DELETE operations
4. **Advanced SQL Features**: JOINs, subqueries, window functions
5. **DuckDB-Specific Features**: Specialized functions and optimizations
6. **Performance Optimizations**: Bulk operations, prepared statements

## Adding New Features

- **Research First**: Study sequel-hexspace implementation patterns directly
- **Document Thoroughly**: Create detailed specifications before coding
- **Test-Driven**: Write comprehensive tests first
- **Incremental**: Implement in small, testable pieces
- **Follow Conventions**: Adhere to sequel-hexspace coding standards exactly
- Place main Database and Dataset classes in `lib/sequel/adapters/duckdb.rb`
- Place DatabaseMethods and DatasetMethods modules in `lib/sequel/adapters/shared/duckdb.rb`
- Use include pattern to mix shared functionality into main classes
- Mirror sequel-hexspace file organization and module structure exactly

## Testing Structure (Following sequel-hexspace Pattern)

- **test/all.rb**: Test runner
- **test/spec_helper.rb**: Test configuration and setup
- **test/database_test.rb**: Database connection and basic functionality tests
- **test/dataset_test.rb**: Comprehensive SQL generation tests
- **test/schema_test.rb**: Schema operations and introspection tests
- **test/prepared_statement_test.rb**: Prepared statement functionality
- **test/sql_test.rb**: SQL generation and syntax tests
- **test/type_test.rb**: Data type handling tests
- Mirror sequel-hexspace test organization exactly
- Test all SQL generation comprehensively
- Include integration tests with actual DuckDB instances
- Follow TDD methodology throughout development