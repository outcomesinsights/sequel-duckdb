# frozen_string_literal: true

require_relative "../../../sequel/duckdb"
require_relative "../../../sequel/duckdb/helpers/pathifier"
require_relative "../../../sequel/duckdb/helpers/copier"
require_relative "../../../sequel/duckdb/sql_time_tz"

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
      def quote_identifiers_default
        true
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

      # Return a dataset that represents a VALUES clause with the given rows.
      # DuckDB supports standard SQL VALUES syntax like PostgreSQL.
      #
      #   DB.values([[1, 2], [3, 4]])
      #   # VALUES ((1, 2), (3, 4))
      def values(v)
        raise Error, "Cannot provide an empty array for values" if v.empty?

        @default_dataset.clone(values: v)
      end

      private

      # DuckDB doesn't fold unquoted identifiers to uppercase
      def folds_unquoted_identifiers_to_uppercase?
        false
      end

      # Schema introspection methods

      # Parse table list from database
      #
      # @param opts [Hash] Options for table parsing
      # @return [Array<Symbol>] Array of table names as symbols
      def schema_parse_tables(opts = {})
        schema_ref = opts[:schema] || "main"

        sql = if schema_ref.is_a?(Sequel::SQL::QualifiedIdentifier)
                "SELECT table_name FROM information_schema.tables " \
                  "WHERE table_catalog = #{literal(schema_ref.table.to_s)} AND table_schema = #{literal(schema_ref.column.to_s)} AND table_type = 'BASE TABLE'"
        else
                "SELECT table_name FROM information_schema.tables " \
                  "WHERE table_schema = #{literal(schema_ref.to_s)} AND table_type = 'BASE TABLE'"
        end

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
        resolved = resolve_table!(table_name, opts)

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
          WHERE #{resolved_table_filter(resolved, catalog: :table_catalog, schema: :table_schema, table: :table_name)}
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

          columns << [ column_name, column_info ]
        end

        # Update primary key information
        update_primary_key_info(resolved, columns)

        columns
      end

      # Parse index information for a table
      #
      # @param table_name [Symbol, String] Name of the table
      # @param opts [Hash] Options for index parsing
      # @return [Hash] Hash of index_name => index_info
      def schema_parse_indexes(table_name, opts = {})
        resolved = resolve_table!(table_name, opts)

        # Use duckdb_indexes() function to get index information
        sql = <<~SQL
          SELECT
            index_name,
            is_unique,
            is_primary,
            expressions,
            sql
          FROM duckdb_indexes()
          WHERE #{resolved_table_filter(resolved, catalog: :database_name, schema: :schema_name, table: :table_name)}
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
        when TrueClass, FalseClass, Numeric
                            value.to_s
        else
                            literal(value.to_s)
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
      # DuckDB resolves the name itself, as it does for any query, so every Sequel
      # identifier form, the search_path, temporary tables and views all work. The query
      # reads no rows. A missing table is a catalog error. It must not abort an open
      # transaction, and DuckDB has no savepoints, so see keep_transaction_on_bind_error.
      #
      # @param table_name [Symbol, String, Sequel::SQL::Identifier, Sequel::SQL::QualifiedIdentifier] Name of the table
      # @param opts [Hash] Options (:schema qualifies an unqualified name)
      # @return [Boolean] true if table exists
      def table_exists?(table_name, opts = {})
        keep_transaction_on_bind_error do
          from(qualify_with_schema_option(table_name, opts)).where(false).select(Sequel::NULL).all
          true
        rescue Sequel::DatabaseConnectionError, Sequel::DatabaseDisconnectError
          raise
        rescue Sequel::DatabaseError
          false
        end
      end

      # Get list of tables
      #
      # @param opts [Hash] Options
      # @return [Array<Symbol>] Array of table names
      def tables(opts = {})
        schema_parse_tables(opts)
      end

      # Get list of views
      #
      # @param opts [Hash] Options
      # @return [Array<Symbol>] Array of view names
      def views(opts = {})
        schema_ref = opts[:schema] || "main"

        sql = if schema_ref.is_a?(Sequel::SQL::QualifiedIdentifier)
                "SELECT table_name FROM information_schema.tables " \
                  "WHERE table_catalog = #{literal(schema_ref.table.to_s)} AND table_schema = #{literal(schema_ref.column.to_s)} AND table_type = 'VIEW'"
        else
                "SELECT table_name FROM information_schema.tables " \
                  "WHERE table_schema = #{literal(schema_ref.to_s)} AND table_type = 'VIEW'"
        end

        views = []
        execute(sql) do |row|
          views << row[:table_name].to_sym
        end

        views
      end

      # Get schema information for a table
      #
      # @param table_name [Symbol, String, Dataset, QualifiedIdentifier] Name of the table or dataset
      # @param opts [Hash] Options
      # @return [Array<Array>] Schema information
      def schema(table_name, opts = {})
        # A qualified identifier (e.g., Sequel[:schema][:table]) is parsed as given
        if table_name.is_a?(Sequel::SQL::QualifiedIdentifier)
          parse_name = table_name
          actual_table_name = table_name.column.to_s.to_sym
        elsif table_name.is_a?(Sequel::Dataset)
          # Handle case where Sequel passes a Dataset object instead of table name
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

        schema_parse_table(parse_name || actual_table_name, opts)
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

      # A value assigned to a TIMETZ model column becomes a SQLTimeTZ, which keeps the UTC
      # offset. A string without an offset gets the local offset.
      def typecast_value_time_timezone(value)
        case value
        when SQLTimeTZ
          value
        when Time
          SQLTimeTZ.from_time(value)
        when DateTime
          SQLTimeTZ.from_time(value.to_time)
        when String
          SQLTimeTZ.from_time(Time.parse(typecast_check_string_length(value, 100)))
        else
          raise Sequel::InvalidValue, "invalid value for TIMETZ: #{value.inspect}"
        end
      end

      # Map DuckDB data types to Sequel types
      #
      # @param duckdb_type [String] DuckDB data type
      # @return [Symbol] Sequel type symbol
      # information_schema.columns reports a REAL column as FLOAT, a TIMESTAMPTZ column as
      # TIMESTAMP WITH TIME ZONE, and a VARINT column as BIGNUM. An unsigned type maps by
      # the signed type that holds all its values.
      def map_duckdb_type_to_sequel(duckdb_type)
        case duckdb_type.upcase
        when "INTEGER", "INT", "INT4", "SMALLINT", "INT2", "TINYINT", "INT1", "USMALLINT", "UTINYINT"
          :integer
        when "BIGINT", "INT8", "UINTEGER", "UBIGINT", "HUGEINT", "UHUGEINT", "BIGNUM", "VARINT"
          :bigint
        when "FLOAT", "REAL", "FLOAT4", "DOUBLE", "FLOAT8"
          :float
        when /^DECIMAL/, /^NUMERIC/
          :decimal
        when "BOOLEAN", "BOOL"
          :boolean
        when "DATE"
          :date
        when "TIMESTAMP", "DATETIME", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ",
             "TIMESTAMP_S", "TIMESTAMP_MS", "TIMESTAMP_US", "TIMESTAMP_NS"
          :datetime
        when "TIME", "TIME_NS"
          :time
        when "TIME WITH TIME ZONE", "TIMETZ"
          :time_timezone
        when "INTERVAL"
          :interval
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

      # Find the table or view that a name refers to, as DuckDB resolves it in a query.
      # The information_schema and duckdb_indexes() queries need the exact catalog, schema
      # and table, and DuckDB has no function that returns them. So the candidates are the
      # tables and views with that name, and the one whose EXPLAIN plan equals the plan for
      # the name is the match. Two candidates have equal plans only when a view is
      # SELECT * FROM the other candidate. DuckDB's search order then decides.
      #
      # @param table_name [Symbol, String, Sequel::SQL::Identifier, Sequel::SQL::QualifiedIdentifier] Name of the table
      # @param opts [Hash] Options (:schema qualifies an unqualified name)
      # @return [Hash, nil] :catalog, :schema and :table, or nil if the name does not resolve
      def resolve_table(table_name, opts = {})
        table_name = qualify_with_schema_option(table_name, opts)
        keep_transaction_on_bind_error do
          target = explain_plan(table_name)
          next unless target

          candidates = fetch("SELECT table_catalog, table_schema, table_name FROM information_schema.tables " \
                             "WHERE lower(table_name) = lower(?)", bare_table_name(table_name)).all
          matches = candidates.select do |c|
            explain_plan(qualified_name(c[:table_catalog], c[:table_schema], c[:table_name])) == target
          end
          match = matches.size > 1 ? first_in_search_order(matches) : matches.first
          match && { catalog: match[:table_catalog], schema: match[:table_schema], table: match[:table_name] }
        end
      end

      # resolve_table, but a name that does not resolve raises.
      def resolve_table!(table_name, opts = {})
        resolve_table(table_name, opts) ||
          raise(Sequel::DatabaseError, "Table #{literal(qualify_with_schema_option(table_name, opts))} does not exist")
      end

      def qualify_with_schema_option(table_name, opts)
        return table_name unless opts[:schema] && !table_name.is_a?(Sequel::SQL::QualifiedIdentifier)

        Sequel.qualify(opts[:schema], table_name)
      end

      def bare_table_name(table_name)
        case table_name
        when Sequel::SQL::QualifiedIdentifier then bare_table_name(table_name.column)
        when Sequel::SQL::Identifier then table_name.value.to_s
        else table_name.to_s
        end
      end

      def qualified_name(catalog, schema, table)
        Sequel::SQL::QualifiedIdentifier.new(
          Sequel::SQL::QualifiedIdentifier.new(Sequel.identifier(catalog), Sequel.identifier(schema)),
          Sequel.identifier(table)
        )
      end

      # The EXPLAIN plan of SELECT * FROM the name, or nil if DuckDB cannot bind the name.
      # Call it inside keep_transaction_on_bind_error.
      def explain_plan(table_name)
        plan = []
        execute("EXPLAIN (FORMAT JSON) #{from(table_name).sql}") { |row| plan << row }
        plan
      rescue Sequel::DatabaseConnectionError, Sequel::DatabaseDisconnectError
        raise
      rescue Sequel::DatabaseError
        nil
      end

      # Runs the block so that a binder or catalog error in it, such as a missing table,
      # does not abort an open transaction.
      #
      # DuckDB 1.x never aborts a transaction on those errors. DuckDB 2.x aborts it by
      # default, and its current_transaction_invalidation_policy setting turns that off
      # for the open transaction only. DuckDB 1.x does not know the value, and a failed
      # SET aborts the transaction, so the setting is used only where
      # bind_errors_abort_transactions? is true. The block must handle its own errors:
      # if it raises, the policy is not restored, and the transaction ends anyway.
      def keep_transaction_on_bind_error
        return yield unless bind_errors_abort_transactions? && in_transaction?

        setting = "current_transaction_invalidation_policy"
        previous = get(Sequel.function(:current_setting, setting))
        run("SET #{setting} = 'SYNTACTIC_ERRORS_DO_NOT_INVALIDATE'")
        result = yield
        run("SET #{setting} = #{literal(previous)}")
        result
      end

      # True when a binder or catalog error aborts an open transaction. The native adapter
      # answers from the loaded DuckDB library. Other adapters run no queries, so false.
      def bind_errors_abort_transactions?
        false
      end

      # DuckDB looks in the temp catalog first, then in the search_path schemas, then in the
      # default catalog.
      def first_in_search_order(candidates)
        schemas = get(Sequel.function(:current_schemas, false))
        database = get(Sequel.function(:current_database))
        candidates.min_by do |c|
          [ c[:table_catalog] == "temp" ? 0 : 1,
            schemas.index(c[:table_schema]) || schemas.size,
            c[:table_catalog] == database ? 0 : 1 ]
        end
      end

      # The SQL condition that selects the resolved table in a catalog query, given the
      # names of that query's catalog, schema and table columns.
      def resolved_table_filter(resolved, catalog:, schema:, table:)
        literal(Sequel.&({ catalog => resolved[:catalog] }, { schema => resolved[:schema] }, { table => resolved[:table] }))
      end

      # Update primary key information in column schema
      #
      # @param resolved [Hash] The table, from resolve_table
      # @param columns [Array] Array of column information
      def update_primary_key_info(resolved, columns)
        # Query for primary key constraints
        sql = <<~SQL
          SELECT column_name
          FROM information_schema.table_constraints tc
          JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_catalog = kcu.table_catalog
            AND tc.table_schema = kcu.table_schema
            AND tc.table_name = kcu.table_name
          WHERE tc.constraint_type = 'PRIMARY KEY'
            AND #{resolved_table_filter(resolved, catalog: Sequel[:tc][:table_catalog], schema: Sequel[:tc][:table_schema], table: Sequel[:tc][:table_name])}
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

      # create_table? and drop_table? emit IF NOT EXISTS / IF EXISTS instead of
      # querying information_schema first.
      def supports_create_table_if_not_exists?
        true
      end

      def supports_drop_table_if_exists?
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
      # Called after schema modifications to ensure cache consistency. It empties the
      # caches in place, because a frozen Database cannot replace them.
      #
      # @return [void]
      def remove_all_cached_schemas
        Sequel.synchronize do
          @schemas&.clear
          @primary_keys&.clear
          @primary_key_sequences&.clear
        end
      end

      # List all schemas in the database
      #
      # @param opts [Hash] Options
      # @option opts [String] :catalog Catalog name to filter by
      # @return [Array<Symbol>] Array of schema names as symbols
      #
      def schemas(opts = OPTS)
        sql = "SELECT schema_name FROM information_schema.schemata"
        sql += " WHERE catalog_name = #{literal(opts[:catalog].to_s)}" if opts[:catalog]

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
      def schema_exists?(name, _opts = OPTS)
        sql = "SELECT 1 FROM information_schema.schemata WHERE schema_name = #{literal(name.to_s)} LIMIT 1"

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
        options = source if source.is_a?(Hash)
        some_paths = options[:path] || options[:paths] || options.dig(:options, :path) || options.dig(:options, :paths)
        # Handle DuckDB-specific source patterns
        source = if options[:using] || some_paths
                   # Build read_parquet or similar function call
                   read_something_sql(some_paths, options).then do |read_stmt|
                     from(read_stmt)
                   end.sql
        elsif source.is_a?(Dataset)
                   source.sql
        elsif source.is_a?(String)
                   source
        else
                   raise Sequel::Error, "Unsupported source type: #{source.class}"
        end

        sql = String.new
        sql << "#{create_view_prefix_sql(name, options)} AS #{source}"

        if (check = options[:check])
          sql << " WITH#{" LOCAL" if check == :local} CHECK OPTION"
        end

        sql
      end

      # Runs a DuckDB ATTACH, which opens the database file at +path+ as the catalog
      # +as+. The catalog is part of the DuckDB database, so every pool connection sees
      # it. It lasts until Database#disconnect closes the last connection; attach again
      # after that. See attach_sql for the options.
      def attach(path, as:, **options)
        run(attach_sql(path, as:, **options))
      end

      # The ATTACH statement that #attach runs. The alias +as+ is required: an alias
      # that DuckDB derives from the file name can equal a schema name in the file,
      # which makes schema.table ambiguous.
      #
      # @param path [String] The database file; a string literal in the SQL
      # @param as [Symbol, String] The catalog name
      # @param if_not_exists [Boolean] Do nothing if the catalog is already attached
      # @param options [Hash] DuckDB ATTACH options, for example read_only: true or
      #   type: :sqlite. A true value renders the bare keyword (READ_ONLY).
      # @return [String]
      def attach_sql(path, as:, if_not_exists: false, **options)
        sql = +"ATTACH "
        sql << "IF NOT EXISTS " if if_not_exists
        sql << "#{literal(path.to_s)} AS #{Sequel::DuckDB::Helpers::SqlOptions.quoted_identifier(as)}"
        list = Sequel::DuckDB::Helpers::SqlOptions.list(options)
        sql << " #{list}" unless list.empty?
        sql
      end

      # Runs a DuckDB DETACH of the catalog +name+.
      def detach(name, if_exists: false)
        run(detach_sql(name, if_exists:))
      end

      # The DETACH statement that #detach runs.
      def detach_sql(name, if_exists: false)
        "DETACH DATABASE #{"IF EXISTS " if if_exists}#{Sequel::DuckDB::Helpers::SqlOptions.quoted_identifier(name)}"
      end

      # Runs a DuckDB COPY that writes +source+ (a Dataset or a SQL string) to
      # +path+. For a Dataset this is the SQL from Dataset#copy_to_sql.
      def copy_to(source, path, options = Sequel::OPTS)
        if source.is_a?(Sequel::Dataset)
          run(source.copy_to_sql(path, **options))
        else
          run(Sequel::DuckDB::Helpers::Copier.new(source, path, options).to_sql)
        end
      end

      private

      def read_something_sql(paths, options = OPTS)
        Sequel::DuckDB::Helpers::Pathifier.new(paths, options).to_sql
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

      # Units that date_add_sql_append emits as a subtraction, and units it emits with the
      # interval first (see the comment there)
      MONTH_BASED_UNITS = %w[YEAR MONTH].freeze
      TIME_BASED_UNITS = %w[HOUR MINUTE SECOND].freeze

      # Clause order of each statement. A SELECT can be a VALUES list, and every
      # statement can start with a WITH clause.
      Dataset.def_sql_method(self, :select, [ [ "if opts[:values]", %w[values compounds order limit] ], [ "else", %w[with select distinct columns from join where group having window compounds order limit lock] ] ])
      Dataset.def_sql_method(self, :insert, %w[with insert into columns values])
      Dataset.def_sql_method(self, :update, %w[with update table set where])
      Dataset.def_sql_method(self, :delete, %w[with delete from where])

      # Returns the DuckDB COPY statement that writes this dataset to +path+,
      # without running it. Options are DuckDB COPY options, for example
      # format: :parquet, partition_by: [:a, :b], compression: :zstd.
      def copy_to_sql(path, **options)
        Sequel::DuckDB::Helpers::Copier.new(self, path, options).to_sql
      end

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

      # DuckDB doesn't support "schema"."table".* syntax.
      # Strip the schema qualifier so it generates "table".* instead.
      def column_all_sql_append(sql, ca)
        table = ca.table
        table = table.column if table.is_a?(Sequel::SQL::QualifiedIdentifier)
        qualified_identifier_sql_append(sql, table, Sequel::LiteralString.new("*"))
      end

      # DuckDB capability flags
      def supports_window_functions?
        true
      end

      # DuckDB supports the FILTER clause for aggregate functions natively.
      # Without this, Sequel emulates FILTER with CASE, which gives wrong
      # results for order-sensitive aggregates such as first(x ORDER BY ...).
      def supports_filtered_aggregates?
        true
      end

      # DuckDB supports IS DISTINCT FROM natively. Sequel's is_distinct_from
      # extension uses this flag; without it the extension emits a CASE.
      def supports_is_distinct_from?
        true
      end

      # DuckDB runs a WITH clause before SELECT, INSERT, UPDATE and DELETE.
      def supports_cte?(_type = :select)
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

      # Features that Sequel refuses, or leaves out of the SQL, without these flags.
      # test/capability_flags_test.rb runs each one against a real DuckDB.
      # supports_cte_in_subqueries? is left undeclared on purpose: declaring it stops
      # Sequel from moving a subquery's CTE to the outer query, which changes the SQL
      # that existing callers get.
      def supports_distinct_on?
        true
      end

      def supports_ordered_distinct_on?
        true
      end

      def supports_group_rollup?
        true
      end

      def supports_group_cube?
        true
      end

      def supports_grouping_sets?
        true
      end

      def supports_lateral_subqueries?
        true
      end

      def supports_merge?
        true
      end

      def supports_window_clause?
        true
      end

      # complex_expression_sql_append writes all four regexp operators.
      def supports_regexp?
        true
      end

      # DuckDB requires WITH RECURSIVE if any CTE is recursive
      # This follows the same pattern as PostgreSQL
      def select_with_sql_base
        opts[:with].any? { |w| w[:recursive] } ? "WITH RECURSIVE " : "WITH "
      end

      # Support VALUES clause instead of SELECT to return rows.
      def select_values_sql(sql)
        sql << "VALUES "
        expression_list_append(sql, opts[:values])
      end

      # Return true if this dataset uses a VALUES clause (not a real table query).
      def empty?
        return false if @opts[:values]

        super
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
        when :"!~", :"!~*"
          # Not Sequel's default: DuckDB's !~ is a full match, so !~ 'bc' is true for 'abc'.
          sql << "NOT "
          complex_expression_sql_append(sql, operator == :"!~" ? :~ : :"~*", args)
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
        when SQLTimeTZ
          sql << "'#{value.strftime("%H:%M:%S.%6N%:z")}'"
        when Sequel::SQLTime
          # A time of day: Sequel writes it as 'HH:MM:SS.ffffff'
          super
        when Time, DateTime
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
      def date_add_sql_append(sql, date_arith)
        expr = date_arith.expr
        interval_hash = date_arith.interval
        cast_type = date_arith.cast_type

        # Build expression with chained interval additions
        result = expr
        interval_hash.each do |unit, value|
          sql_unit = DUCKDB_DURATION_UNITS[unit]
          next unless sql_unit

          # DuckDB judges INTERVAL 1 MONTH, INTERVAL 30 DAY and INTERVAL 720 HOUR equal, and
          # its optimizer then merges d + INTERVAL 30 DAY with d + INTERVAL 1 MONTH (or 1 DAY
          # with 24 HOUR) into one expression, so both return the same value. Each kind of
          # unit gets its own expression form, so two kinds never compare equal:
          #   month-based: d - INTERVAL (-n) MONTH
          #   days:        d + INTERVAL n DAY
          #   time-based:  INTERVAL n HOUR + d
          # A subtraction for time units would merge them with months.
          interval = build_interval_literal(value, sql_unit)
          result = if MONTH_BASED_UNITS.include?(sql_unit)
            Sequel.-(result, build_interval_literal(negate_interval_value(value), sql_unit))
          elsif TIME_BASED_UNITS.include?(sql_unit)
            Sequel.+(interval, result)
          else
            Sequel.+(result, interval)
          end
        end

        # Apply cast if specified or default to Time (TIMESTAMP)
        # Note: DuckDB returns TIMESTAMP when adding intervals to DATE
        result = Sequel.cast(result, cast_type || Time)

        literal_append(sql, result)
      end

      private

      def negate_interval_value(value)
        value.is_a?(Numeric) ? -value : Sequel.*(value, -1)
      end

      def build_interval_literal(value, unit)
        # If value is numeric, use direct syntax
        # If value is expression, wrap in parentheses
        if value.is_a?(Numeric)
          # Direct numeric: INTERVAL 5 HOUR or INTERVAL (-5) HOUR for negatives
          # DuckDB requires parentheses around negative numbers
          if value.negative?
            Sequel.lit("INTERVAL (#{value}) #{unit}")
          else
            Sequel.lit([ "INTERVAL ", " #{unit}" ], value)
          end
        else
          # Expression: INTERVAL (column_name) HOUR
          # Note: expressions already include negation from date_sub
          Sequel.lit([ "INTERVAL (", ") #{unit}" ], value)
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
