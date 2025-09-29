# frozen_string_literal: true

require_relative "lib/sequel/duckdb/version"

Gem::Specification.new do |spec|
  spec.name = "sequel-duckdb"
  spec.version = Sequel::DuckDB::VERSION
  spec.authors = ["Ryan Duryea"]
  spec.email = ["aguynamedryan@gmail.com"]

  spec.summary = "Sequel database adapter for DuckDB"
  spec.description = "A Ruby gem that provides a complete database adapter for the Sequel toolkit to work with DuckDB, enabling Ruby applications to connect to and interact with DuckDB databases through Sequel's comprehensive ORM and database abstraction interface."
  spec.homepage = "https://github.com/sequel/sequel-duckdb"
  spec.required_ruby_version = ">= 3.1.0"

  spec.metadata["allowed_push_host"] = "https://rubygems.org"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/sequel/sequel-duckdb"
  spec.metadata["changelog_uri"] = "https://github.com/sequel/sequel-duckdb/blob/main/CHANGELOG.md"
  spec.metadata["rubygems_mfa_required"] = "true"

  # Specify which files should be added to the gem when it is released.
  # The `git ls-files -z` loads the files in the RubyGem that have been added into git.
  gemspec = File.basename(__FILE__)
  spec.files = IO.popen(%w[git ls-files -z], chdir: __dir__, err: IO::NULL) do |ls|
    ls.readlines("\x0", chomp: true).reject do |f|
      (f == gemspec) ||
        f.start_with?(*%w[bin/ test/ spec/ features/ .git .github appveyor Gemfile])
    end
  end
  spec.bindir = "exe"
  spec.executables = spec.files.grep(%r{\Aexe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  # Core dependencies
  spec.add_dependency "duckdb", ">= 1.0.0"
  spec.add_dependency "sequel", ">= 5.0"

  # Development dependencies
  spec.add_development_dependency "irb"
  spec.add_development_dependency "logger"
  spec.add_development_dependency "minitest", "~> 5.0"
  spec.add_development_dependency "rake", "~> 13.0"
  spec.add_development_dependency "rubocop", "~> 1.21"
  spec.add_development_dependency "rubocop-minitest", "~> 0.38.2"
  spec.add_development_dependency "rubocop-rake", "~> 0.7.1"
  spec.add_development_dependency "rubocop-sequel", "~> 0.4.1"
end
