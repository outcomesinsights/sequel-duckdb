# Technology Stack

## Language & Runtime
- **Ruby**: 3.1+ required
- **Gem**: Standard Ruby gem structure

## Dependencies
- **Sequel**: Database toolkit (core dependency)
- **ruby-duckdb**: Official DuckDB client gem for connections
- **DuckDB**: Target database system
- **Bundler**: Dependency management
- **Rake**: Build automation
- **RuboCop**: Code linting and style enforcement

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
# Run linting
bundle exec rake rubocop
# or simply
rake

# Install gem locally
bundle exec rake install

# Build gem
bundle exec rake build

# Release new version
bundle exec rake release
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
- Generate comprehensive tests before implementation
- Follow Test-Driven Development (TDD) approach
- All SQL generation must have corresponding tests

### Connection Management
- Use **ruby-duckdb** gem exclusively for database connections
- Follow Sequel's connection pooling patterns
- Implement proper error handling and connection lifecycle management