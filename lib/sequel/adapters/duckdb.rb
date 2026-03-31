# frozen_string_literal: true

require "sequel"
require "duckdb"
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
    module DriverDatabaseMethods
      private

      # Open the DuckDB::Database once during Sequel::Database initialization.
      # DuckDB's ATTACH/schema state is per-Database, not per-Connection, so all
      # pool connections must share one Database instance.
      def adapter_initialize
        database_path = opts[:database]

        @duckdb_database = if database_path == ":memory:" || database_path.nil?
                             ::DuckDB::Database.open(":memory:")
                           else
                             if database_path.match?(/^[a-zA-Z]/) && !database_path.start_with?(":")
                               database_path = "/#{database_path}"
                             end
                             ::DuckDB::Database.open(database_path)
                           end
      rescue ::DuckDB::Error => e
        raise Sequel::DatabaseConnectionError, "Failed to connect to DuckDB database: #{e.message}"
      rescue StandardError => e
        raise Sequel::DatabaseConnectionError, "Unexpected error connecting to DuckDB: #{e.message}"
      end

      public

      def connect(_server)
        @duckdb_database.connect
      rescue ::DuckDB::Error => e
        raise Sequel::DatabaseConnectionError, "Failed to connect to DuckDB database: #{e.message}"
      end

      def disconnect_connection(conn)
        return unless conn

        begin
          conn.close
        rescue ::DuckDB::Error # rubocop:disable Lint/SuppressedException -- best-effort cleanup on disconnect
        end
      end

      def valid_connection?(conn)
        return false unless conn

        begin
          conn.query("SELECT 1")
          true
        rescue ::DuckDB::Error
          false
        end
      end

      def execute(sql, opts = OPTS, &)
        _execute(:select, sql, opts, &)
      end

      def execute_dui(sql, opts = OPTS)
        _execute(:update, sql, opts)
      end

      def execute_insert(sql, opts = OPTS)
        _execute(:insert, sql, opts)
      end

      def dataset_class_default
        Dataset
      end

      private

      def database_error_classes
        [::DuckDB::Error]
      end

      def _execute(type, sql, opts, &block)
        synchronize(opts[:server]) do |conn|
          case type
          when :select
            execute_select(sql, conn, &block)
          when :insert, :update
            log_connection_yield(sql, conn) { conn.query(sql).rows_changed }
          end
        end
      rescue ::DuckDB::Error => e
        raise_error(e, opts)
      end

      def execute_select(sql, conn, &block)
        log_connection_yield(sql, conn) do
          result = conn.query(sql)
          yield_rows(result, &block) if block
          result
        end
      end

      def yield_rows(result)
        columns = result.columns
        result.each do |row_array|
          row_hash = {}
          columns.each_with_index do |column, index|
            column_name = column.respond_to?(:name) ? column.name : column.to_s
            row_hash[column_name.to_sym] = row_array[index]
          end
          yield row_hash
        end
      end

      def result_column_names(result)
        result.columns.map do |column|
          column.respond_to?(:name) ? column.name.to_s : column.to_s
        end
      end
    end

    module DriverDatasetMethods
      def fetch_rows(sql)
        db.synchronize(opts[:server]) do |conn|
          result = db.send(:log_connection_yield, sql, conn) { conn.query(sql) }

          col_names = db.send(:result_column_names, result)
          self.columns = col_names.map { |c| output_identifier(c) }

          result.each do |row_array|
            row_hash = {}
            col_names.each_with_index do |col_name, i|
              row_hash[output_identifier(col_name)] = row_array[i]
            end
            yield row_hash
          end
        end
      rescue ::DuckDB::Error => e
        raise Sequel::DatabaseError, e.message
      end
    end

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
      include Sequel::DuckDB::DriverDatabaseMethods

      # Set the adapter scheme for DuckDB
      # This allows Sequel.connect('duckdb://...') to work
      set_adapter_scheme :duckdb
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
      include Sequel::DuckDB::DriverDatasetMethods
    end
  end
end
