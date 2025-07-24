# frozen_string_literal: true

require "sequel"
require_relative "shared/duckdb"

# Sequel is a database toolkit for Ruby that provides a powerful ORM and database abstraction layer.
# This module extends Sequel to support DuckDB, a high-performance analytical database engine.
#
# @example Basic connection
#   db = Sequel.connect('duckdb::memory:')
#   db = Sequel.connect('duckdb:///path/to/database.duckdb')
#
# @example Connection with options
#   db = Sequel.connect(
#     adapter: 'duckdb',
#     database: '/path/to/database.duckdb',
#     config: { memory_limit: '2GB', threads: 4 }
#   )
#
# @see https://sequel.jeremyevans.net/ Sequel Documentation
# @see https://duckdb.org/ DuckDB Documentation
module Sequel
  # DuckDB adapter module for Sequel
  #
  # This module provides complete integration between Sequel and DuckDB, including:
  # - Connection management for file-based and in-memory databases
  # - SQL generation optimized for DuckDB's analytical capabilities
  # - Schema introspection and metadata access
  # - Data type mapping between Ruby and DuckDB types
  # - Transaction support with proper error handling
  # - Performance optimizations for analytical workloads
  #
  # @example Creating tables
  #   db.create_table :users do
  #     primary_key :id
  #     String :name, null: false
  #     String :email, unique: true
  #     Integer :age
  #     Boolean :active, default: true
  #     DateTime :created_at
  #   end
  #
  # @example Analytical queries
  #   sales_summary = db[:sales]
  #     .select(
  #       :product_category,
  #       Sequel.function(:sum, :amount).as(:total_sales),
  #       Sequel.function(:avg, :amount).as(:avg_sale)
  #     )
  #     .group(:product_category)
  #     .order(Sequel.desc(:total_sales))
  #
  # @since 0.1.0
  module DuckDB
    # Database class for DuckDB adapter
    #
    # This class extends Sequel::Database to provide DuckDB-specific functionality.
    # It handles connection management, schema operations, and SQL execution for
    # DuckDB databases. The class includes DatabaseMethods from the shared module
    # to provide the core database functionality.
    #
    # @example Connecting to different database types
    #   # In-memory database (data lost when connection closes)
    #   db = Sequel::DuckDB::Database.new(database: ':memory:')
    #
    #   # File-based database (persistent storage)
    #   db = Sequel::DuckDB::Database.new(database: '/path/to/database.duckdb')
    #
    #   # With configuration options
    #   db = Sequel::DuckDB::Database.new(
    #     database: '/path/to/database.duckdb',
    #     config: { memory_limit: '4GB', threads: 8 }
    #   )
    #
    # @example Schema operations
    #   # List all tables
    #   db.tables  # => [:users, :products, :orders]
    #
    #   # Get table schema
    #   db.schema(:users)  # => [[:id, {...}], [:name, {...}], ...]
    #
    #   # Check if table exists
    #   db.table_exists?(:users)  # => true
    #
    # @see DatabaseMethods
    # @since 0.1.0
    class Database < Sequel::Database
      include Sequel::DuckDB::DatabaseMethods

      # Set the adapter scheme for DuckDB
      # This allows Sequel.connect('duckdb://...') to work
      set_adapter_scheme :duckdb

      # Return the default dataset class for this database
      #
      # This method is called by Sequel to determine which Dataset class
      # to use when creating new datasets for this database connection.
      #
      # @return [Class] The Dataset class to use for this database (always DuckDB::Dataset)
      # @see Dataset
      def dataset_class_default
        Dataset
      end
    end

    # Dataset class for DuckDB adapter
    #
    # This class extends Sequel::Dataset to provide DuckDB-specific SQL generation
    # and query execution functionality. It includes DatasetMethods from the shared
    # module for SQL generation and other dataset operations.
    #
    # The Dataset class is responsible for:
    # - Generating DuckDB-compatible SQL for all operations (SELECT, INSERT, UPDATE, DELETE)
    # - Executing queries against DuckDB databases
    # - Handling result set processing and type conversion
    # - Supporting DuckDB-specific features like window functions and CTEs
    #
    # @example Basic queries
    #   users = db[:users]
    #   users.where(active: true).all
    #   users.where { age > 25 }.count
    #   users.order(:name).limit(10).each { |user| puts user[:name] }
    #
    # @example Analytical queries with DuckDB features
    #   # Window functions
    #   db[:sales].select(
    #     :product_id,
    #     :amount,
    #     Sequel.function(:rank).over(partition: :category, order: Sequel.desc(:amount)).as(:rank)
    #   )
    #
    #   # Common Table Expressions (CTEs)
    #   db.with(:high_spenders,
    #     db[:orders].group(:user_id).having { sum(:total) > 1000 }.select(:user_id)
    #   ).from(:high_spenders).join(:users, id: :user_id)
    #
    # @example Data modification
    #   # Insert single record
    #   users.insert(name: 'John', email: 'john@example.com', age: 30)
    #
    #   # Bulk insert
    #   users.multi_insert([
    #     {name: 'Alice', email: 'alice@example.com'},
    #     {name: 'Bob', email: 'bob@example.com'}
    #   ])
    #
    #   # Update records
    #   users.where(active: false).update(active: true, updated_at: Time.now)
    #
    #   # Delete records
    #   users.where { created_at < Date.today - 365 }.delete
    #
    # @see DatasetMethods
    # @since 0.1.0
    class Dataset < Sequel::Dataset
      include Sequel::DuckDB::DatasetMethods
    end
  end

  # Register the DuckDB adapter with Sequel
  # This registration allows Sequel.connect("duckdb://...") to automatically
  # use the DuckDB adapter and create DuckDB::Database instances.
  #
  # @example Connection string usage
  #   db = Sequel.connect('duckdb::memory:')
  #   db = Sequel.connect('duckdb:///path/to/database.duckdb')
  #
  # @see Database
  Database.adapter_scheme :duckdb, DuckDB::Database
end