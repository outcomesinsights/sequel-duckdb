# frozen_string_literal: true

module Sequel
  module DuckDB
    # Version information for the sequel-duckdb gem
    #
    # This constant contains the current version of the sequel-duckdb adapter.
    # It follows semantic versioning (SemVer) conventions.
    #
    # @example Getting the version
    #   puts Sequel::DuckDB::VERSION
    #
    # @since 0.1.0
    #
    # release-please bumps this file by rewriting the FIRST quoted version
    # string in it. Keep VERSION's value the only quoted version here.
    VERSION = "0.2.0"
  end
end
