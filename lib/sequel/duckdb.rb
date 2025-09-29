# frozen_string_literal: true

require_relative "duckdb/version"

# Sequel is the database toolkit for Ruby
module Sequel
  # DuckDB integration module for Sequel
  #
  # This module provides the entry point for DuckDB integration with Sequel.
  # It includes version information and common error classes used throughout
  # the DuckDB adapter implementation.
  #
  # The actual adapter implementation is located in the adapters directory,
  # which provides the Database and Dataset classes with full DuckDB support.
  #
  # @example Basic usage
  #   require 'sequel'
  #   require 'sequel/duckdb'
  #
  #   db = Sequel.connect('duckdb::memory:')
  #   db = Sequel.connect('duckdb:///path/to/database.duckdb')
  #
  # @see Sequel::DuckDB::Database
  # @see Sequel::DuckDB::Dataset
  # @since 0.1.0
  module DuckDB
    # Base error class for DuckDB-related exceptions
    #
    # This class serves as the base for all DuckDB-specific errors that may
    # occur during adapter operation. It extends StandardError to provide
    # a consistent error hierarchy for the DuckDB adapter.
    #
    # @example Catching DuckDB errors
    #   begin
    #     db.execute("INVALID SQL")
    #   rescue Sequel::DuckDB::Error => e
    #     puts "DuckDB error: #{e.message}"
    #   end
    #
    # @since 0.1.0
    class Error < StandardError; end
  end
end
