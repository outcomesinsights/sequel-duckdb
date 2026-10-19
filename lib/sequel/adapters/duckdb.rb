# frozen_string_literal: true

require "sequel"
require "duckdb"
require_relative "shared/duckdb"
require_relative "../duckdb/instance_registry"

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
      # The open ::DuckDB::Database and the number of open connections to it. Connecting
      # and disconnecting change both, so they live in this object and not in instance
      # variables, which a frozen Sequel::Database cannot change.
      ConnectionState = Struct.new(:database, :connections)

      private

      # Open the DuckDB::Database once during Sequel::Database initialization.
      # DuckDB's ATTACH/schema state is per-Database, not per-Connection, so all
      # pool connections must share one Database instance. A database file is also
      # shared with every other Sequel::Database on that file in this process
      # (InstanceRegistry), and #disconnect gives it back once no connection is open.
      def adapter_initialize
        @conversion_procs = {}
        @primary_keys = {}
        @primary_key_sequences = {}
        register_default_conversion_procs
        @duckdb_mutex = Mutex.new
        @duckdb_file = duckdb_file_path(opts[:database])
        @duckdb_settings = InstanceRegistry.normalize_settings(opts[:config])
        @duckdb_state = ConnectionState.new(open_duckdb_database, 0)
      rescue Sequel::DatabaseConnectionError
        raise
      rescue ::DuckDB::Error => e
        raise Sequel::DatabaseConnectionError, "Failed to connect to DuckDB database: #{e.message}"
      rescue StandardError => e
        raise Sequel::DatabaseConnectionError, "Unexpected error connecting to DuckDB: #{e.message}"
      end

      # ruby-duckdb returns TIME, TIME_NS and TIMETZ values as a Time on today's date.
      # Sequel's convention for a time of day is Sequel::SQLTime, which is also what
      # Sequel typecasts a value assigned to a TIME model column to. TIMETZ values become
      # Sequel::DuckDB::SQLTimeTZ, which keeps the value's UTC offset.
      def register_default_conversion_procs
        to_sqltime = ->(value) { Sequel::SQLTime.create(value.hour, value.min, value.sec, Rational(value.nsec, 1000)) }
        add_conversion_proc(:time, to_sqltime)
        add_conversion_proc(:time_ns, to_sqltime)
        add_conversion_proc(:time_tz, SQLTimeTZ.method(:from_time))
      end

      # nil for an in-memory database, which is never shared.
      def duckdb_file_path(database_path)
        return nil if database_path.nil? || database_path == ":memory:"
        return "/#{database_path}" if database_path.match?(/^[a-zA-Z]/) && !database_path.start_with?(":")

        database_path
      end

      # opts[:config] holds DuckDB settings, for example { threads: 4, memory_limit: "2GB" }.
      def open_duckdb_database
        if @duckdb_file
          InstanceRegistry.acquire(@duckdb_file, @duckdb_settings)
        else
          InstanceRegistry.open(":memory:", @duckdb_settings)
        end
      end

      public

      # Registers a converter for result values of one DuckDB column type. Dataset
      # fetches pass each non-NULL value of a column of that type to the converter and
      # return what it gives back. A converter receives the whole column value, so a
      # LIST of STRUCT gives it the complete Array.
      #
      # +type+ is the type name that ruby-duckdb reports, for example :time or :list.
      # A column with a type alias is looked up by its alias first: JSON columns report
      # the type :varchar with the alias "JSON", so register them as :json. Register
      # converters before you run queries. A later registration for the same type
      # replaces the earlier one, including the built-in converters for :time and
      # :time_ns, which return Sequel::SQLTime, and for :time_tz, which returns
      # Sequel::DuckDB::SQLTimeTZ.
      #
      # @example
      #   db.add_conversion_proc(:json) { |value| JSON.parse(value) }
      def add_conversion_proc(type, callable = nil, &block)
        @conversion_procs[conversion_proc_key(type)] = callable || block
      end

      def connect(_server)
        @duckdb_mutex.synchronize do
          state = @duckdb_state
          state.database ||= open_duckdb_database
          conn = state.database.connect
          state.connections += 1
          conn
        end
      rescue ::DuckDB::Error => e
        raise Sequel::DatabaseConnectionError, "Failed to connect to DuckDB database: #{e.message}"
      end

      # Closes the pool's idle connections. When no connection of this Sequel::Database
      # is still open, it also gives back its reference to the shared database file, so
      # the file closes once no Sequel::Database in the process uses it. The next query
      # opens it again. An in-memory database stays open, so its data survives.
      def disconnect(opts = OPTS)
        super
        @duckdb_mutex.synchronize do
          state = @duckdb_state
          next unless @duckdb_file && state.database && state.connections.zero?

          InstanceRegistry.release(@duckdb_file)
          state.database = nil
        end
      end

      def disconnect_connection(conn)
        return unless conn

        begin
          conn.close
        rescue ::DuckDB::Error # rubocop:disable Lint/SuppressedException -- best-effort cleanup on disconnect
        end
        @duckdb_mutex.synchronize { @duckdb_state.connections -= 1 }
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
        [ ::DuckDB::Error ]
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

      def conversion_proc_key(type)
        type.to_s.downcase.to_sym
      end

      # The converter for each result column, in column order, or nil for a column
      # with none. Empty when nothing is registered, so rows stay exactly as
      # ruby-duckdb returns them.
      def result_conversion_procs(result)
        return [] if @conversion_procs.empty?

        result.columns.map do |column|
          type_alias = column.logical_type.alias
          (type_alias && @conversion_procs[conversion_proc_key(type_alias)]) || @conversion_procs[column.type]
        end
      end
    end

    module DriverDatasetMethods
      def fetch_rows(sql)
        db.synchronize(opts[:server]) do |conn|
          result = db.send(:log_connection_yield, sql, conn) { conn.query(sql) }

          col_names = db.send(:result_column_names, result)
          self.columns = col_names.map { |c| output_identifier(c) }
          converters = db.send(:result_conversion_procs, result)

          result.each do |row_array|
            row_hash = {}
            col_names.each_with_index do |col_name, i|
              value = row_array[i]
              converter = converters[i]
              value = converter.call(value) if converter && !value.nil?
              row_hash[output_identifier(col_name)] = value
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
