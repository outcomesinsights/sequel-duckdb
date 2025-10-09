# frozen_string_literal: true

require "duckdb"

# Sequel is the database toolkit for Ruby
module Sequel
  module DuckDB
    # DatabaseMethods module provides shared database functionality for DuckDB adapter
    #
    # This module is included by the main Database class to provide connection management,
    # schema introspection, and SQL execution capabilities. It implements the core
    # database operations required by Sequel's adapter interface.
    #
    # Key responsibilities:
    # - Connection management (connect, disconnect, validation)
    # - SQL execution with proper error handling and logging
    # - Schema introspection (tables, columns, indexes, constraints)
    # - Transaction support with commit/rollback capabilities
    # - Data type mapping between Ruby and DuckDB types
    # - Performance optimizations for analytical workloads
    #
    # @example Connection management
    #   db = Sequel.connect('duckdb:///path/to/database.duckdb')
    #   db.test_connection  # => true
    #   db.disconnect
    #
    # @example Schema introspection
    #   db.tables                    # => [:users, :products, :orders]
    #   db.schema(:users)            # => [[:id, {...}], [:name, {...}]]
    #   db.indexes(:users)           # => {:users_email_index => {...}}
    #   db.table_exists?(:users)     # => true
    #
    # @example SQL execution
    #   db.execute("SELECT COUNT(*) FROM users")
    #   db.execute("INSERT INTO users (name) VALUES (?)", ["John"])
    #
    # @example Transactions
    #   db.transaction do
    #     db[:users].insert(name: 'Alice')
    #     db[:orders].insert(user_id: db[:users].max(:id), total: 100)
    #   end
    #
    # @see Database
    # @since 0.1.0
    module DatabaseMethods
      # DuckDB uses the :duckdb database type.
      def database_type
        :duckdb
      end

      # DuckDB doesn't support AUTOINCREMENT
      def supports_autoincrement?
        false
      end

      # Whether to quote identifiers by default for this database
      def quote_identifiers_default # rubocop:disable Naming/PredicateMethod
        true
      end

      private

      # DuckDB doesn't fold unquoted identifiers to uppercase
      def folds_unquoted_identifiers_to_uppercase?
        false
      end

      # Error classification using DATABASE_ERROR_REGEXPS following SQLite pattern
      DATABASE_ERROR_REGEXPS = {
        /NOT NULL constraint failed/i => Sequel::NotNullConstraintViolation,
        /UNIQUE constraint failed|PRIMARY KEY|duplicate/i => Sequel::UniqueConstraintViolation,
        /FOREIGN KEY constraint failed/i => Sequel::ForeignKeyConstraintViolation,
        /CHECK constraint failed/i => Sequel::CheckConstraintViolation,
        /constraint failed/i => Sequel::ConstraintViolation
      }.freeze

      def database_error_regexps
        DATABASE_ERROR_REGEXPS
      end

      # Schema introspection methods

      # Parse table list from database
      #
      # @param opts [Hash] Options for table parsing
      # @return [Array<Symbol>] Array of table names as symbols
      def schema_parse_tables(opts = {})
        schema_name = opts[:schema] || "main"

        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = '#{schema_name}' AND table_type = 'BASE TABLE'"

        tables = []
        execute(sql) do |row|
          tables << row[:table_name].to_sym
        end

        tables
      end

      # Parse table schema information
      #
      # @param table_name [Symbol, String] Name of the table
      # @param opts [Hash] Options for schema parsing
      # @return [Array<Array>] Array of [column_name, column_info] pairs
      def schema_parse_table(table_name, opts = {})
        schema_name = opts[:schema] || "main"

        # First check if table exists
        raise Sequel::DatabaseError, "Table '#{table_name}' does not exist" unless table_exists?(table_name, opts)

        # Use information_schema.columns for detailed column information
        sql = <<~SQL
          SELECT
            column_name,
            ordinal_position,
            column_default,
            is_nullable,
            data_type,
            character_maximum_length,
            numeric_precision,
            numeric_scale
          FROM information_schema.columns
          WHERE table_schema = '#{schema_name}' AND table_name = '#{table_name}'
          ORDER BY ordinal_position
        SQL

        columns = []
        execute(sql) do |row|
          column_name = row[:column_name].to_sym

          # Map DuckDB types to Sequel types
          sequel_type = map_duckdb_type_to_sequel(row[:data_type])

          # Parse nullable flag
          allow_null = row[:is_nullable] == "YES"

          # Parse default value
          default_value = parse_default_value(row[:column_default])

          column_info = {
            type: sequel_type,
            db_type: row[:data_type],
            allow_null: allow_null,
            default: default_value,
            primary_key: false # Will be updated below
          }

          # Add size information for string types
          column_info[:max_length] = row[:character_maximum_length] if row[:character_maximum_length]

          # Add precision/scale for numeric types
          column_info[:precision] = row[:numeric_precision] if row[:numeric_precision]
          column_info[:scale] = row[:numeric_scale] if row[:numeric_scale]

          columns << [column_name, column_info]
        end

        # Update primary key information
        update_primary_key_info(table_name, columns, opts)

        columns
      end

      # Parse index information for a table
      #
      # @param table_name [Symbol, String] Name of the table
      # @param opts [Hash] Options for index parsing
      # @return [Hash] Hash of index_name => index_info
      def schema_parse_indexes(table_name, opts = {})
        schema_name = opts[:schema] || "main"

        # First check if table exists
        raise Sequel::DatabaseError, "Table '#{table_name}' does not exist" unless table_exists?(table_name, opts)

        # Use duckdb_indexes() function to get index information
        sql = <<~SQL
          SELECT
            index_name,
            is_unique,
            is_primary,
            expressions,
            sql
          FROM duckdb_indexes()
          WHERE schema_name = '#{schema_name}' AND table_name = '#{table_name}'
        SQL

        indexes = {}
        execute(sql) do |row|
          index_name = row[:index_name].to_sym

          # Parse column expressions - DuckDB returns them as JSON array strings
          columns = parse_index_columns(row[:expressions])

          index_info = {
            columns: columns,
            unique: row[:is_unique],
            primary: row[:is_primary]
          }

          indexes[index_name] = index_info
        end

        indexes
      end

      public

      # Configuration convenience methods (Requirements 3.1, 3.2)

      # Set a DuckDB PRAGMA setting
      #
      # This method provides a user-friendly wrapper around DuckDB's PRAGMA statements.
      # PRAGMA statements are used to configure various DuckDB settings and behaviors.
      #
      # @param key [String, Symbol] The pragma setting name
      # @param value [Object] The value to set (will be converted to appropriate format)
      # @return [void]
      #
      # @raise [Sequel::DatabaseError] If the pragma setting is invalid or fails
      #
      # @example Set memory limit
      #   db.set_pragma("memory_limit", "2GB")
      #   db.set_pragma(:memory_limit, "1GB")
      #
      # @example Set thread count
      #   db.set_pragma("threads", 4)
      #
      # @example Enable/disable features
      #   db.set_pragma("enable_progress_bar", true)
      #   db.set_pragma("enable_profiling", false)
      #
      # @see configure_duckdb
      # @since 0.1.0
      def set_pragma(key, value)
        # Convert key to string for consistency
        pragma_key = key.to_s

        # Format value appropriately for SQL
        formatted_value = case value
                          when String
                            "'#{value.gsub("'", "''")}'" # Escape single quotes
                          when TrueClass, FalseClass, Numeric
                            value.to_s
                          else
                            "'#{value}'"
                          end

        # Execute PRAGMA statement
        pragma_sql = "PRAGMA #{pragma_key} = #{formatted_value}"

        begin
          execute(pragma_sql)
        rescue StandardError => e
          raise Sequel::DatabaseError, "Failed to set pragma #{pragma_key}: #{e.message}"
        end
      end

      # Configure multiple DuckDB settings at once
      #
      # This method allows batch configuration of multiple DuckDB PRAGMA settings
      # in a single method call. It's a convenience wrapper around multiple set_pragma calls.
      #
      # @param options [Hash] Hash of pragma_name => value pairs
      # @return [void]
      #
      # @raise [Sequel::DatabaseError] If any pragma setting fails
      #
      # @example Configure multiple settings
      #   db.configure_duckdb(
      #     memory_limit: "2GB",
      #     threads: 8,
      #     enable_progress_bar: true,
      #     default_order: "ASC"
      #   )
      #
      # @example Configure with string keys
      #   db.configure_duckdb(
      #     "memory_limit" => "1GB",
      #     "threads" => 4
      #   )
      #
      # @see set_pragma
      # @since 0.1.0
      def configure_duckdb(options = {})
        return if options.empty?

        # Apply each configuration option
        options.each do |key, value|
          set_pragma(key, value)
        end
      end

      # Check if table exists
      #
      # @param table_name [Symbol, String] Name of the table
      # @param opts [Hash] Options
      # @return [Boolean] true if table exists
      def table_exists?(table_name, opts = {})
        schema_name = opts[:schema] || "main"

        sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = '#{schema_name}' AND table_name = '#{table_name}' LIMIT 1"

        result = nil
        execute(sql) do |_row|
          result = true
        end

        !!result
      end

      # Get list of tables
      #
      # @param opts [Hash] Options
      # @return [Array<Symbol>] Array of table names
      def tables(opts = {})
        schema_parse_tables(opts)
      end

      # Get schema information for a table
      #
      # @param table_name [Symbol, String, Dataset] Name of the table or dataset
      # @param opts [Hash] Options
      # @return [Array<Array>] Schema information
      def schema(table_name, opts = {})
        # Handle case where Sequel passes a Dataset object instead of table name
        if table_name.is_a?(Sequel::Dataset)
          # Extract table name from dataset
          if table_name.opts[:from]&.first
            actual_table_name = table_name.opts[:from].first
            # Handle case where table name is wrapped in an identifier
            actual_table_name = actual_table_name.value if actual_table_name.respond_to?(:value)
          else
            # Fallback: try to extract from SQL
            sql = table_name.sql
            raise Sequel::Error, "Cannot determine table name from dataset: #{table_name}" unless sql =~ /FROM\s+(\w+)/i

            actual_table_name = ::Regexp.last_match(1).to_sym

          end
        else
          actual_table_name = table_name
        end

        # Cache schema information for type conversion
        schema_info = schema_parse_table(actual_table_name, opts)
        @schema_cache ||= {}
        @schema_cache[actual_table_name] = {}

        schema_info.each do |column_name, column_info|
          @schema_cache[actual_table_name][column_name] = column_info
        end

        schema_info
      end

      # Get index information for a table
      #
      # @param table_name [Symbol, String] Name of the table
      # @param opts [Hash] Options
      # @return [Hash] Index information
      def indexes(table_name, opts = {})
        schema_parse_indexes(table_name, opts)
      end

      private

      # Map DuckDB data types to Sequel types
      #
      # @param duckdb_type [String] DuckDB data type
      # @return [Symbol] Sequel type symbol
      def map_duckdb_type_to_sequel(duckdb_type)
        case duckdb_type.upcase
        when "INTEGER", "INT", "INT4", "SMALLINT", "INT2", "TINYINT", "INT1"
          :integer
        when "BIGINT", "INT8"
          :bigint
        when "REAL", "FLOAT4", "DOUBLE", "FLOAT8"
          :float
        when /^DECIMAL/, /^NUMERIC/
          :decimal
        when "BOOLEAN", "BOOL"
          :boolean
        when "DATE"
          :date
        when "TIMESTAMP", "DATETIME"
          :datetime
        when "TIME"
          :time
        when "BLOB", "BYTEA"
          :blob
        when "UUID"
          :uuid
        else
          # when "VARCHAR", "TEXT", "STRING"
          :string # Default fallback
        end
      end

      # Parse default value from DuckDB format
      #
      # @param default_str [String, nil] Default value string from DuckDB
      # @return [Object, nil] Parsed default value
      def parse_default_value(default_str)
        return nil if default_str.nil? || default_str.empty?

        # Handle common DuckDB default formats
        case default_str
        when /^CAST\('(.+)' AS BOOLEAN\)$/
          ::Regexp.last_match(1) == "t"
        when /^'(.+)'$/
          ::Regexp.last_match(1) # String literal
        when /^\d+$/
          default_str.to_i  # Integer literal
        when /^\d+\.\d+$/
          default_str.to_f  # Float literal
        when "NULL"
          nil
        else
          default_str # Return as-is for complex expressions
        end
      end

      # Update primary key information in column schema
      #
      # @param table_name [Symbol, String] Table name
      # @param columns [Array] Array of column information
      # @param opts [Hash] Options
      def update_primary_key_info(table_name, columns, opts = {})
        schema_name = opts[:schema] || "main"

        # Query for primary key constraints
        sql = <<~SQL
          SELECT column_name
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
            AND tc.table_name = kcu.table_name
          WHERE tc.constraint_type = 'PRIMARY KEY'
            AND tc.table_schema = '#{schema_name}'
            AND tc.table_name = '#{table_name}'
        SQL

        primary_key_columns = []
        execute(sql) do |row|
          primary_key_columns << row[:column_name].to_sym
        end

        # Update primary key flag for matching columns
        columns.each do |column_name, column_info|
          if primary_key_columns.include?(column_name)
            column_info[:primary_key] = true
            column_info[:allow_null] = false # Primary keys cannot be null
          end
        end
      end

      # Parse index column expressions from DuckDB format
      #
      # @param expressions_str [String] JSON array string of column expressions
      # @return [Array<Symbol>] Array of column names
      def parse_index_columns(expressions_str)
        return [] if expressions_str.nil? || expressions_str.empty?

        # DuckDB returns expressions as JSON array like "[column_name]" or "['\"column_name\"']"
        # Remove brackets and quotes, split by comma
        cleaned = expressions_str.gsub(/^\[|\]$/, "").gsub(/['"]/, "")
        cleaned.split(",").map(&:strip).map(&:to_sym)
      end

      public

      # Transaction support - DuckDB has basic transaction support only
      def supports_savepoints?
        false
      end

      def supports_transaction_isolation_level?(_level)
        false
      end

      def supports_manual_transaction_control?
        true
      end

      # DuckDB-specific schema generation methods

      # Generate SQL for primary key column
      #
      # @param column [Symbol] Column name
      # @param _opts [Hash] Column options
      # @return [String] SQL for primary key column
      def primary_key_column_sql(column, _opts)
        # DuckDB doesn't support AUTOINCREMENT, so we just use INTEGER PRIMARY KEY
        col_sql = String.new
        quote_identifier_append(col_sql, column)
        "#{col_sql} INTEGER PRIMARY KEY"
        # Don't add AUTOINCREMENT for DuckDB
      end

      # Override to prevent AUTOINCREMENT from being added
      def auto_increment_sql
        ""
      end

      # Generate SQL for auto-incrementing column
      # DuckDB doesn't support AUTOINCREMENT, use sequences instead
      #
      # @param column [Symbol] Column name
      # @param _opts [Hash] Column options
      # @return [String] SQL for auto-incrementing column
      def auto_increment_column_sql(column, _opts)
        # DuckDB uses sequences for auto-increment, but for primary keys
        # we can just use INTEGER PRIMARY KEY without AUTOINCREMENT
        col_sql = String.new
        quote_identifier_append(col_sql, column)
        "#{col_sql} INTEGER PRIMARY KEY"
      end

      # Map Ruby types to DuckDB types
      #
      # @param opts [Hash] Column options
      # @return [String] DuckDB type
      def type_literal(opts)
        case opts[:type]
        when :primary_key, :integer
          "INTEGER"
        when :string, :text
          if opts[:size]
            "VARCHAR(#{opts[:size]})"
          else
            "VARCHAR"
          end
        when :bigint
          "BIGINT"
        when :float, :real
          "REAL"
        when :double
          "DOUBLE"
        when :decimal, :numeric
          if opts[:size]
            "DECIMAL(#{Array(opts[:size]).join(",")})"
          else
            "DECIMAL"
          end
        when :boolean
          "BOOLEAN"
        when :date
          "DATE"
        when :datetime, :timestamp
          "TIMESTAMP"
        when :time
          "TIME"
        when :blob, :binary
          "BLOB"
        else
          super
        end
      end

      public

      # Schema management methods (Requirements: schema creation, deletion, introspection)

      # Create a schema
      #
      # @param name [String, Symbol] Schema name
      # @param opts [Hash] Options
      # @option opts [Boolean] :if_not_exists Add IF NOT EXISTS clause
      # @option opts [Boolean] :or_replace Add OR REPLACE clause (mutually exclusive with :if_not_exists)
      # @return [void]
      #
      # @example Create a schema
      #   db.create_schema(:analytics)
      #
      # @example Create with IF NOT EXISTS
      #   db.create_schema(:staging, if_not_exists: true)
      #
      # @example Create with OR REPLACE
      #   db.create_schema(:temp, or_replace: true)
      def create_schema(name, opts = OPTS)
        self << create_schema_sql(name, opts)
      end

      # Generate SQL for creating a schema
      #
      # @param name [String, Symbol] Schema name
      # @param opts [Hash] Options
      # @option opts [Boolean] :if_not_exists Add IF NOT EXISTS clause
      # @option opts [Boolean] :or_replace Add OR REPLACE clause (mutually exclusive with :if_not_exists)
      # @return [String] CREATE SCHEMA SQL
      def create_schema_sql(name, opts = OPTS)
        # DuckDB doesn't support both OR REPLACE and IF NOT EXISTS together
        if opts[:or_replace] && opts[:if_not_exists]
          raise Sequel::Error, "Cannot use both :or_replace and :if_not_exists options"
        end

        sql = "CREATE"
        sql += " OR REPLACE" if opts[:or_replace]
        sql += " SCHEMA"
        sql += " IF NOT EXISTS" if opts[:if_not_exists]
        sql += " #{quote_identifier(name)}"
        sql
      end

      # Drop a schema
      #
      # @param name [String, Symbol] Schema name
      # @param opts [Hash] Options
      # @option opts [Boolean] :if_exists Add IF EXISTS clause
      # @option opts [Boolean] :cascade Add CASCADE clause to drop dependent objects
      # @return [void]
      #
      # @example Drop a schema
      #   db.drop_schema(:analytics)
      #
      # @example Drop with IF EXISTS
      #   db.drop_schema(:staging, if_exists: true)
      #
      # @example Drop with CASCADE
      #   db.drop_schema(:temp, cascade: true)
      def drop_schema(name, opts = OPTS)
        self << drop_schema_sql(name, opts)
        remove_all_cached_schemas
      end

      # Generate SQL for dropping a schema
      #
      # @param name [String, Symbol] Schema name
      # @param opts [Hash] Options
      # @option opts [Boolean] :if_exists Add IF EXISTS clause
      # @option opts [Boolean] :cascade Add CASCADE clause to drop dependent objects
      # @return [String] DROP SCHEMA SQL
      def drop_schema_sql(name, opts = OPTS)
        sql = "DROP SCHEMA"
        sql += " IF EXISTS" if opts[:if_exists]
        sql += " #{quote_identifier(name)}"
        sql += " CASCADE" if opts[:cascade]
        sql
      end

      # Remove all cached schema information
      #
      # Called after schema modifications to ensure cache consistency
      #
      # @return [void]
      def remove_all_cached_schemas
        @schema_cache = {}
        @schemas = {}
        @primary_keys = {}
        @primary_key_sequences = {}
      end

      # List all schemas in the database
      #
      # @param opts [Hash] Options
      # @option opts [String] :catalog Catalog name to filter by
      # @return [Array<Symbol>] Array of schema names as symbols
      #
      # @example List all schemas
      #   db.schemas  # => [:main, :analytics, :staging]
      def schemas(opts = OPTS)
        sql = "SELECT schema_name FROM information_schema.schemata"
        sql += " WHERE catalog_name = '#{opts[:catalog]}'" if opts[:catalog]

        schemas = []
        execute(sql) do |row|
          schemas << row[:schema_name].to_sym
        end

        schemas
      end

      # Check if a schema exists
      #
      # @param name [String, Symbol] Schema name
      # @param opts [Hash] Options (reserved for future use)
      # @return [Boolean] true if schema exists
      #
      # @example Check if schema exists
      #   db.schema_exists?(:analytics)  # => true
      def schema_exists?(name, opts = OPTS)
        sql = "SELECT 1 FROM information_schema.schemata WHERE schema_name = '#{name}' LIMIT 1"

        result = nil
        execute(sql) do |_row|
          result = true
        end

        !!result
      end

      private

      # Type conversion methods for DuckDB-specific handling

      # Convert DuckDB TIME values to Ruby time-only objects
      # DuckDB TIME columns should only contain time-of-day information
      def typecast_value_time(value)
        case value
        when Time
          # Extract only the time portion, discarding date information
          # Create a new Time object with today's date but the original time
          Time.local(1970, 1, 1, value.hour, value.min, value.sec, value.usec)
        when String
          # Parse time string and create time-only object
          if value =~ /\A(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d+))?\z/
            hour = ::Regexp.last_match(1).to_i
            min = ::Regexp.last_match(2).to_i
            sec = ::Regexp.last_match(3).to_i
            usec = (::Regexp.last_match(4) || "0").ljust(6, "0").to_i
            Time.local(1970, 1, 1, hour, min, sec, usec)
          else
            # Fallback: parse as time and extract time portion
            parsed = Time.parse(value.to_s)
            Time.local(1970, 1, 1, parsed.hour, parsed.min, parsed.sec, parsed.usec)
          end
        else
          value
        end
      end

      # Override the default type conversion to use our custom TIME handling
      # This method needs to be public for Sequel models to access it
      public

      def typecast_value(column, value)
        return value if value.nil?

        # Get column schema information to determine the correct type
        if @schema_cache && @schema_cache[column]
          column_type = @schema_cache[column][:type]
          case column_type
          when :time
            return typecast_value_time(value)
          end
        end

        # Fall back to default Sequel type conversion
        super
      end
    end

    # DatasetMethods module provides shared dataset functionality for DuckDB adapter
    # This module is included by the main Dataset class to provide SQL generation
    # and query execution capabilities.
    module DatasetMethods
      # DuckDB reserved words that must be quoted
      DUCKDB_RESERVED_WORDS = %w[
        order group select from where having limit offset union all distinct
        case when then else end and or not in like between is null true false
        join inner left right full outer on using as with recursive
        create table view index drop alter insert update delete
        primary key foreign references constraint unique check default
        auto_increment serial bigserial smallserial
        integer int bigint smallint tinyint boolean bool
        varchar char text string blob
        date time timestamp datetime interval
        float double real decimal numeric
        array struct map
      ].freeze

      private

      # DuckDB uses lowercase identifiers
      def input_identifier(value)
        value.to_s
      end

      # DuckDB uses lowercase identifiers
      def output_identifier(value)
        value == "" ? :untitled : value.to_sym
      end

      public

      # Delegate quote_identifiers_default to the database
      def quote_identifiers_default
        db.quote_identifiers_default
      end

      # Check if an identifier needs quoting
      def identifier_needs_quoting?(name)
        return true if super

        DUCKDB_RESERVED_WORDS.include?(name.to_s.downcase)
      end

      # Generate INSERT SQL statement
      #
      # @param values [Hash, Array] Values to insert
      # @return [String] The INSERT SQL statement
      def insert_sql(*values)
        return @opts[:sql] if @opts[:sql]

        # Handle empty values case
        if values.empty? || (values.length == 1 && values.first.empty?)
          return "INSERT INTO #{table_name_sql} DEFAULT VALUES"
        end

        # Handle single hash of values
        if values.length == 1 && values.first.is_a?(Hash)
          values_hash = values.first
          columns = values_hash.keys
          column_list = literal(columns)
          values_list = literal(columns.map { |k| values_hash[k] })

          return "INSERT INTO #{table_name_sql} #{column_list} VALUES #{values_list}"
        end

        # Handle array of hashes (multiple records)
        if values.length == 1 && values.first.is_a?(Array)
          records = values.first
          return "INSERT INTO #{table_name_sql} DEFAULT VALUES" if records.empty?

          first_record = records.first
          columns = first_record.keys
          column_list = literal(columns)

          values_lists = records.map do |record|
            literal(columns.map { |k| record[k] })
          end

          return "INSERT INTO #{table_name_sql} #{column_list} VALUES #{values_lists.join(", ")}"
        end

        # Fallback for other cases
        "INSERT INTO #{table_name_sql} DEFAULT VALUES"
      end

      # Generate UPDATE SQL statement
      #
      # @param values [Hash] Values to update
      # @return [String] The UPDATE SQL statement
      def update_sql(values = {})
        return @opts[:sql] if @opts[:sql]

        sql = "UPDATE #{table_name_sql} SET "

        # Add SET clause
        set_clauses = values.map do |column, value|
          col_sql = String.new
          quote_identifier_append(col_sql, column)
          "#{col_sql} = #{literal(value)}"
        end
        sql << set_clauses.join(", ")

        # Add WHERE clause
        select_where_sql(sql) if @opts[:where]

        sql
      end

      # Generate DELETE SQL statement
      #
      # @return [String] The DELETE SQL statement
      def delete_sql
        return @opts[:sql] if @opts[:sql]

        sql = "DELETE FROM #{table_name_sql}"

        # Add WHERE clause
        select_where_sql(sql) if @opts[:where]

        sql
      end

      # DuckDB capability flags
      def supports_window_functions?
        true
      end

      def supports_cte?
        true
      end

      def supports_returning?(_type = nil)
        false
      end

      def supports_select_all_and_offset?
        true
      end

      def supports_join_using?
        true
      end

      # Validate table name for SELECT operations
      def validate_table_name_for_select
        return unless @opts[:from] # Skip if no FROM clause

        @opts[:from].each do |table|
          if table.nil? || (table.respond_to?(:to_s) && table.to_s.strip.empty?)
            raise ArgumentError,
                  "Table name cannot be nil or empty"
          end
        end
      end

      # Check if a word is a SQL reserved word that needs quoting
      def reserved_word?(word)
        %w[order group select from where having limit offset].include?(word.downcase)
      end

      # Get properly quoted table name
      def table_name_sql
        raise ArgumentError, "Table name cannot be nil or empty" if @opts[:from].nil? || @opts[:from].empty?

        # Check if the table name is nil
        table_name = @opts[:from].first
        raise ArgumentError, "Table name cannot be nil" if table_name.nil?

        table_name = table_name.to_s
        raise ArgumentError, "Table name cannot be empty" if table_name.empty?

        # Use quote_identifier_append to respect quote_identifiers? setting
        sql = String.new
        quote_identifier_append(sql, table_name)
        sql
      end

      private

      # Override the WITH clause generation to support RECURSIVE keyword
      def select_with_sql(sql)
        return unless opts[:with]

        # Check if any WITH clause is recursive (either explicitly marked or auto-detected)
        has_recursive = opts[:with].any? { |w| w[:recursive] || cte_is_recursive?(w) }

        # Add WITH or WITH RECURSIVE prefix
        sql << (has_recursive ? "WITH RECURSIVE " : "WITH ")

        # Add each CTE
        opts[:with].each_with_index do |w, i|
          sql << ", " if i.positive?
          name_sql = String.new
          quote_identifier_append(name_sql, w[:name])
          sql << "#{name_sql} AS (#{w[:dataset].sql})"
        end

        sql << " "
      end

      # Auto-detect if a CTE is recursive by analyzing its SQL for self-references
      #
      # @param cte_info [Hash] CTE information hash with :name and :dataset
      # @return [Boolean] true if the CTE appears to be recursive
      def cte_is_recursive?(cte_info)
        return false unless cte_info[:dataset]

        cte_name = cte_info[:name].to_s
        cte_sql = cte_info[:dataset].sql

        # Check if the CTE SQL contains references to its own name
        # Look for patterns like "FROM table_name" or "JOIN table_name"
        # Use word boundaries to avoid false positives with partial matches
        recursive_pattern = /\b(?:FROM|JOIN)\s+#{Regexp.escape(cte_name)}\b/i

        cte_sql.match?(recursive_pattern)
      end

      public

      # Override select_from_sql to validate table names
      def select_from_sql(sql)
        if (f = @opts[:from])
          # Validate that no table names are nil
          f.each do |table|
            raise ArgumentError, "Table name cannot be nil" if table.nil?
          end
        end

        # Call parent implementation
        super
      end

      # Add JOIN clauses to SQL (Requirement 6.9)
      def select_join_sql(sql)
        return unless @opts[:join]

        @opts[:join].each do |join| # rubocop:disable Metrics/BlockLength
          # Handle different join clause types
          case join
          when Sequel::SQL::JoinOnClause
            join_type = join.join_type || :inner
            table = join.table
            conditions = join.on

            # Format join type
            join_clause = case join_type
                          when :left, :left_outer
                            "LEFT JOIN"
                          when :right, :right_outer
                            "RIGHT JOIN"
                          when :full, :full_outer
                            "FULL JOIN"
                          else
                            # when :inner
                            "INNER JOIN"
                          end

            sql << " #{join_clause} "

            # Add table name
            sql << if table.is_a?(Sequel::Dataset)
                     alias_sql = String.new
                     quote_identifier_append(alias_sql, join.table_alias || "subquery")
                     "(#{table.sql}) AS #{alias_sql}"
                   else
                     literal(table)
                   end

            # Add ON conditions
            if conditions
              sql << " ON "
              literal_append(sql, conditions)
            end

          when Sequel::SQL::JoinUsingClause
            join_type = join.join_type || :inner
            table = join.table
            using_columns = join.using

            join_clause = case join_type
                          when :left, :left_outer
                            "LEFT JOIN"
                          when :right, :right_outer
                            "RIGHT JOIN"
                          when :full, :full_outer
                            "FULL JOIN"
                          else
                            # when :inner
                            "INNER JOIN"
                          end

            sql << " #{join_clause} "

            # Handle table with alias
            sql << if table.is_a?(Sequel::Dataset)
                     # Subquery with alias
                     "(#{table.sql})"
                   else
                     # Regular table (may have alias)
                     literal(table)
                     # Add alias if present
                   end
            if join.table_alias
              sql << " AS "
              quote_identifier_append(sql, join.table_alias)
            end

            if using_columns
              sql << " USING ("
              Array(using_columns).each_with_index do |col, i|
                sql << ", " if i.positive?
                quote_identifier_append(sql, col)
              end
              sql << ")"
            end

          when Sequel::SQL::JoinClause
            join_type = join.join_type || :inner
            table = join.table

            join_clause = case join_type
                          when :cross
                            "CROSS JOIN"
                          when :natural
                            "NATURAL JOIN"
                          else
                            "INNER JOIN"
                          end

            sql << " #{join_clause} "
            sql << literal(table)
          end
        end
      end

      # Add WHERE clause to SQL (enhanced for complex conditions - Requirement 6.4)
      def select_where_sql(sql)
        return unless @opts[:where]

        sql << " WHERE "
        literal_append(sql, @opts[:where])
      end

      # Add GROUP BY clause to SQL (Requirement 6.7)
      def select_group_sql(sql)
        return unless @opts[:group]

        sql << " GROUP BY "
        if @opts[:group].is_a?(Array)
          sql << @opts[:group].map { |col| literal(col) }.join(", ")
        else
          literal_append(sql, @opts[:group])
        end
      end

      # Add HAVING clause to SQL (Requirement 6.8)
      def select_having_sql(sql)
        return unless @opts[:having]

        sql << " HAVING "
        literal_append(sql, @opts[:having])
      end

      # Add ORDER BY clause to SQL (enhanced - Requirement 6.5)
      def select_order_sql(sql)
        return unless @opts[:order]

        sql << " ORDER BY "
        sql << if @opts[:order].is_a?(Array)
                 @opts[:order].map { |col| order_column_sql(col) }.join(", ")
               else
                 order_column_sql(@opts[:order])
               end
      end

      # Format individual ORDER BY column
      def order_column_sql(column)
        case column
        when Sequel::SQL::OrderedExpression
          col_sql = literal(column.expression)
          col_sql << (column.descending ? " DESC" : " ASC")
          # Check if nulls option exists (may not be available in all Sequel versions)
          if column.respond_to?(:nulls) && column.nulls
            col_sql << (column.nulls == :first ? " NULLS FIRST" : " NULLS LAST")
          end
          col_sql
        else
          literal(column)
        end
      end

      # DuckDB-specific SQL generation enhancements

      # Override complex_expression_sql_append for DuckDB-specific handling

      def complex_expression_sql_append(sql, operator, args)
        case operator
        when :LIKE
          # Generate clean LIKE without ESCAPE clause (Requirement 1.1)
          sql << "("
          literal_append(sql, args.first)
          sql << " LIKE "
          literal_append(sql, args.last)
          sql << ")"
        when :"NOT LIKE"
          # Generate clean NOT LIKE without ESCAPE clause (Requirement 1.1)
          sql << "("
          literal_append(sql, args.first)
          sql << " NOT LIKE "
          literal_append(sql, args.last)
          sql << ")"
        when :ILIKE
          # DuckDB doesn't have ILIKE, use UPPER() workaround with proper parentheses (Requirement 1.3)
          sql << "(UPPER("
          literal_append(sql, args.first)
          sql << ") LIKE UPPER("
          literal_append(sql, args.last)
          sql << "))"
        when :"NOT ILIKE"
          # Generate clean NOT ILIKE without ESCAPE clause (Requirement 1.3)
          sql << "(UPPER("
          literal_append(sql, args.first)
          sql << ") NOT LIKE UPPER("
          literal_append(sql, args.last)
          sql << "))"
        when :~
          # Regular expression matching for DuckDB with proper parentheses (Requirement 4.1, 4.3)
          # DuckDB's ~ operator has limitations with anchors, so we use regexp_matches for reliability
          sql << "(regexp_matches("
          literal_append(sql, args.first)
          sql << ", "
          literal_append(sql, args.last)
          sql << "))"
        when :"~*"
          # Case-insensitive regular expression matching for DuckDB (Requirement 4.2)
          # Use regexp_matches with case-insensitive flag
          sql << "(regexp_matches("
          literal_append(sql, args.first)
          sql << ", "
          literal_append(sql, args.last)
          sql << ", 'i'))"
        else
          super
        end
      end

      # Override join method to support USING clause syntax
      def join(table, expr = nil, options = {})
        # Handle the case where using parameter is passed
        if options.is_a?(Hash) && options[:using]
          using_columns = Array(options[:using])
          join_type = options[:type] || :inner
          join_clause = Sequel::SQL::JoinUsingClause.new(using_columns, join_type, table)
          clone(join: (@opts[:join] || []) + [join_clause])
        else
          # Fall back to standard Sequel join behavior
          super
        end
      end

      # Override literal methods for DuckDB-specific formatting
      def literal_string_append(sql, string)
        sql << "'" << string.gsub("'", "''") << "'"
      end

      def literal_date(date)
        "'#{date.strftime("%Y-%m-%d")}'"
      end

      def literal_datetime(datetime)
        "'#{datetime.strftime("%Y-%m-%d %H:%M:%S")}'"
      end

      def literal_time(time)
        "'#{time.strftime("%H:%M:%S")}'"
      end

      # Override symbol literal handling to prevent asterisk from being quoted
      # This fixes count(*) function calls which should not quote the asterisk
      def literal_symbol_append(sql, value)
        # Special case for asterisk - don't quote it
        if value == :*
          sql << "*"
        else
          # Use standard Sequel symbol handling for all other symbols
          super
        end
      end

      def literal_boolean(value)
        value ? "TRUE" : "FALSE"
      end

      def literal_true
        "TRUE"
      end

      def literal_false
        "FALSE"
      end

      # Override literal_append to handle DuckDB-specific type conversions
      # Only handles cases that differ from Sequel's default behavior
      def literal_append(sql, value)
        case value
        when Time
          # Special handling for time-only values (year 1970 indicates time-only)
          if value.year == 1970 && value.month == 1 && value.day == 1
            # This is a time-only value, use TIME format
            sql << "'#{value.strftime("%H:%M:%S")}'"
          else
            # Use our custom datetime formatting for consistency
            literal_datetime_append(sql, value)
          end
        when DateTime
          # Use our custom datetime formatting for consistency
          literal_datetime_append(sql, value)
        when String
          # Only handle binary data differently for DuckDB's hex format
          if value.encoding == Encoding::ASCII_8BIT
            literal_blob_append(sql, value)
          else
            # Let Sequel handle LiteralString and regular strings
            super
          end
        else
          super
        end
      end

      # Helper method for datetime literal appending
      def literal_datetime_append(sql, datetime)
        sql << "'#{datetime.strftime("%Y-%m-%d %H:%M:%S")}'"
      end

      # Helper method for binary data literal appending
      def literal_blob_append(sql, blob)
        # DuckDB expects BLOB literals in hex format without \x prefix
        sql << "'#{blob.unpack1("H*")}'"
      end

      # Literal conversion for binary data (BLOB type)
      def literal_blob(blob)
        "'#{blob.unpack1("H*")}'"
      end

    end
  end

  # Setup mock adapter when using Sequel.mock(host: :duckdb)
  def self.mock_adapter_setup(db)
    db.instance_exec do
      # Just do the minimal setup like SQLite
      def schema_parse_table(*)
        []
      end
      singleton_class.send(:private, :schema_parse_table)
    end
  end

  # Register DuckDB adapter for mock databases
  # This allows Sequel.mock(host: :duckdb) to work properly
  Sequel::Database.set_shared_adapter_scheme(:duckdb, Sequel::DuckDB)
end
