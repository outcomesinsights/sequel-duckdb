# Technology Stack

## Language & Runtime
- **Ruby**: 3.1+ required
- **Gem**: Standard Ruby gem structure

## Dependencies
- **Sequel**: Database toolkit (core dependency)
- **duckdb**: Official DuckDB client gem for connections
- **DuckDB**: Target database system
- **Bundler**: Dependency management
- **Rake**: Build automation
- **RuboCop**: Code linting and style enforcement

## Testing Framework
- **Minitest**: Ruby's built-in testing framework (following sequel-hexspace pattern)
- **Sequel Mock Database**: For SQL generation testing without database connections
- **DuckDB In-Memory**: For integration testing with actual database instances

## Development Tools
- **IRB**: Interactive Ruby console
- **Git**: Version control
- **GitHub Actions**: CI/CD (configured)

## Code Style & Quality
- **RuboCop** configuration enforces:
  - Double quotes for string literals
  - Ruby 3.1 target version
  - Standard Ruby style guidelines

## Common Commands

### Setup & Installation
```bash
# Install dependencies
bundle install

# Setup development environment
bin/setup

# Interactive console
bin/console
```

### Development Workflow
```bash
# Run tests (primary development command)
bundle exec rake test
# or
ruby test/all.rb

# Run linting
bundle exec rake rubocop
# or simply
rake

# Run both tests and linting
bundle exec rake

# Install gem locally
bundle exec rake install

# Build gem
bundle exec rake build

# Release new version
bundle exec rake release
```

### Testing Commands
```bash
# Run all tests
ruby test/all.rb

# Run specific test file
ruby test/database_test.rb

# Run tests with verbose output
ruby test/all.rb -v

# Run specific test method
ruby test/database_test.rb -n test_connection
```

### Code Quality
```bash
# Run RuboCop with auto-correct
bundle exec rubocop -a

# Check specific files
bundle exec rubocop lib/
```

## Coding Standards & References

### Primary Code Style Reference (in order of precedence)
1. **jeremyevans/sequel**: Official Sequel repository - follow all conventions and idioms
2. **sequel-hexspace**: Secondary reference for adapter patterns
3. **sequel_impala**: Additional reference for implementation approaches

### Implementation Guidelines
- Study git history of reference projects to understand implementation order
- Focus on incremental, testable implementations
- **ALWAYS write tests BEFORE implementing functionality (TDD)**
- Follow Test-Driven Development (TDD) approach strictly
- All SQL generation must have corresponding tests
- Every public method must have test coverage
- Integration tests must use actual DuckDB instances
- Unit tests must use Sequel's mock database for SQL generation testing

### Testing Requirements (MANDATORY)
- **Test Structure**: Follow sequel-hexspace test organization exactly
- **Test Files**: Mirror sequel-hexspace test files (database_test.rb, dataset_test.rb, schema_test.rb, etc.)
- **SQL Generation Tests**: Every SQL generation method must have unit tests verifying correct SQL output
- **Integration Tests**: Database operations must have tests using actual DuckDB databases
- **Error Handling Tests**: All error conditions must have corresponding test coverage
- **Type Conversion Tests**: All data type mappings must be thoroughly tested
- **Mock Database Tests**: Use Sequel's mock database for testing SQL generation without database connections
- **Test Coverage**: Aim for 100% test coverage of all implemented functionality

### Connection Management
- Use **duckdb** gem exclusively for database connections
- Follow Sequel's connection pooling patterns
- Implement proper error handling and connection lifecycle management