# frozen_string_literal: true

require_relative "lib/sequel/duckdb/version"

Gem::Specification.new do |spec|
  spec.name = "sequel-duckdb"
  spec.version = Sequel::DuckDB::VERSION
  spec.authors = [ "Ryan Duryea" ]
  spec.email = [ "aguynamedryan@gmail.com" ]

  spec.summary = "Sequel database adapter for DuckDB"
  spec.description = "A Ruby gem that provides a complete database adapter for the Sequel toolkit to work with DuckDB, enabling Ruby applications to connect to and interact with DuckDB databases through Sequel's comprehensive ORM and database abstraction interface."
  spec.homepage = "https://github.com/outcomesinsights/sequel-duckdb"
  spec.license = "MIT"
  spec.required_ruby_version = ">= 3.3.0"

  spec.metadata["allowed_push_host"] = "https://rubygems.org"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = spec.homepage
  spec.metadata["changelog_uri"] = "https://github.com/outcomesinsights/sequel-duckdb/blob/main/CHANGELOG.md"
  spec.metadata["rubygems_mfa_required"] = "true"

  # An allowlist, not `git ls-files`: the gem ships the library and three docs,
  # nothing else. Listing the repository shipped .beads/ (including its
  # credential key), .kiro/, plans/ and AGENTS.md in 0.2.0 and 0.2.1.
  spec.files = %w[CHANGELOG.md LICENSE README.md] + Dir["lib/**/*.rb"]
  spec.require_paths = [ "lib" ]

  # Core dependencies
  # NOTE: the duckdb C extension is NOT a gemspec dependency.  The shared
  # adapter (SQL dialect, quoting, mock support) is pure Ruby and useful
  # without the native library.  The actual `require "duckdb"` only happens
  # in lib/sequel/adapters/duckdb.rb when establishing a real connection.
  # Consumers that need live DuckDB connections should add `gem "duckdb"` to
  # their own Gemfile.
  spec.add_dependency "sequel", "~> 5.0", ">= 5.0"

  # Development dependencies
  spec.add_development_dependency "irb"
  spec.add_development_dependency "logger"
  spec.add_development_dependency "minitest", "~> 6.0"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rubocop", "~> 1.21"
  spec.add_development_dependency "rubocop-rails-omakase", "~> 1.1"
  spec.add_development_dependency "rubocop-minitest", "~> 0.40.0"
  spec.add_development_dependency "rubocop-rake", "~> 0.7.1"
  spec.add_development_dependency "rubocop-sequel", "~> 0.4.1"
  spec.add_development_dependency "simplecov", "~> 1.0"
end
