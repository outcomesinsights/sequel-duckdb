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
      #   db.set_pragma(:memory_limit, "1GB")
      #
      #
      #   db.set_pragma("enable_profiling", false)
      #
      # @see configure_duckdb
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
      #     memory_limit: "2GB",
      #     threads: 8,
      #     enable_progress_bar: true,
      #     default_order: "ASC"
      #   )
      #
      #     "memory_limit" => "1GB",
      #     "threads" => 4
      #   )
      #
      # @see set_pragma
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
      #
      #
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
      #
      #
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
      def schema_exists?(name, opts = OPTS)
        sql = "SELECT 1 FROM information_schema.schemata WHERE schema_name = '#{name}' LIMIT 1"

        result = nil
        execute(sql) do |_row|
          result = true
        end

        !!result
      end

      # Override create_view_prefix_sql to support DuckDB options
      #
      # @param name [Symbol, String] View name
      # @param options [Hash] View options
      # @option options [Boolean] :temp Create a TEMPORARY view
      # @option options [Boolean] :replace Use OR REPLACE
      # @option options [Array<Symbol>] :columns Column names for the view
      # @return [String] CREATE VIEW prefix SQL
      def create_view_prefix_sql(name, options)
        sql = String.new
        sql << "CREATE "
        sql << "OR REPLACE " if options[:replace]
        sql << "TEMPORARY " if options[:temp]
        sql << "VIEW #{quote_schema_table(name)}"

        # Add columns if specified
        if options[:columns]
          sql << " ("
          schema_utility_dataset.send(:identifier_list_append, sql, options[:columns])
          sql << ")"
        end

        sql
      end

      # Override create_view_sql to handle DuckDB-specific view sources
      #
      # @param name [Symbol, String] View name
      # @param source [String, Dataset, Hash] View source - can be SQL, Dataset, or Hash for parquet files
      # @param options [Hash] View options
      # @option options [String] :using Data source type (e.g., "parquet")
      # @option options [Hash] :options Options for the data source (e.g., path for parquet)
      # @return [String] CREATE VIEW SQL
      def create_view_sql(name, source, options = OPTS)
        # Handle DuckDB-specific source patterns
        if source.is_a?(Hash) && options[:using]
          # Build read_parquet or similar function call
          case options[:using]
          when "parquet"
            path_option = options[:options]&.dig(:path)
            raise Sequel::Error, "Missing :path in :options for parquet view" unless path_option

            # Convert Pathname to string if needed
            path_str = path_option.to_s

            # Determine if we need glob pattern based on path
            # DuckDB's read_parquet handles globs automatically
            source = "read_parquet('#{path_str}')"
          else
            raise Sequel::Error, "Unsupported :using type: #{options[:using]}"
          end
        elsif source.is_a?(Dataset)
          source = source.sql
        end

        sql = String.new
        sql << "#{create_view_prefix_sql(name, options)} AS #{source}"

        if check = options[:check]
          sql << " WITH#{' LOCAL' if check == :local} CHECK OPTION"
        end

        sql
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

      # DuckDB interval unit mapping for date arithmetic
      DUCKDB_DURATION_UNITS = {
        years: "YEAR",
        months: "MONTH",
        days: "DAY",
        hours: "HOUR",
        minutes: "MINUTE",
        seconds: "SECOND"
      }.freeze

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

      # DuckDB supports multi-row inserts using VALUES syntax
      # This allows inserting multiple rows in a single INSERT statement
      def multi_insert_sql_strategy
        :values
      end

      def supports_join_using?
        true
      end

      # DuckDB requires WITH RECURSIVE if any CTE is recursive
      # This follows the same pattern as PostgreSQL
      def select_with_sql_base
        opts[:with].any? { |w| w[:recursive] } ? "WITH RECURSIVE " : "WITH "
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

      # DuckDB-specific implementation of date arithmetic
      # This will be called by Sequel's date_arithmetic extension
      # via the `super` mechanism when the extension is loaded
      def date_add_sql_append(sql, da)
        expr = da.expr
        interval_hash = da.interval
        cast_type = da.cast_type

        # Build expression with chained interval additions
        result = expr
        interval_hash.each do |unit, value|
          sql_unit = DUCKDB_DURATION_UNITS[unit]
          next unless sql_unit

          # Create interval addition
          interval = build_interval_literal(value, sql_unit)
          result = Sequel.+(result, interval)
        end

        # Apply cast if specified or default to Time (TIMESTAMP)
        # Note: DuckDB returns TIMESTAMP when adding intervals to DATE
        result = Sequel.cast(result, cast_type || Time)

        literal_append(sql, result)
      end

      private

      def build_interval_literal(value, unit)
        # If value is numeric, use direct syntax
        # If value is expression, wrap in parentheses
        if value.is_a?(Numeric)
          # Direct numeric: INTERVAL 5 HOUR or INTERVAL (-5) HOUR for negatives
          # DuckDB requires parentheses around negative numbers
          if value < 0
            Sequel.lit("INTERVAL (#{value}) #{unit}")
          else
            Sequel.lit(["INTERVAL ", " #{unit}"], value)
          end
        else
          # Expression: INTERVAL (column_name) HOUR
          # Note: expressions already include negation from date_sub
          Sequel.lit(["INTERVAL (", ") #{unit}"], value)
        end
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
