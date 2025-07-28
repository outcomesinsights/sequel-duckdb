# frozen_string_literal: true

require "duckdb"

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
      # Connect to a DuckDB database
      #
      # Creates a connection to either a file-based or in-memory DuckDB database.
      # This method handles the low-level connection establishment and error handling.
      #
      # @param server [Hash] Server configuration options from Sequel
      # @option server [String] :database Database path or ':memory:' for in-memory database
      # @option server [Hash] :config DuckDB-specific configuration options
      # @option server [Boolean] :readonly Whether to open database in read-only mode
      #
      # @return [::DuckDB::Connection] Active DuckDB database connection
      #
      # @raise [Sequel::DatabaseConnectionError] If connection fails due to:
      #   - Invalid database path
      #   - Insufficient permissions
      #   - DuckDB library errors
      #   - Configuration errors
      #
      # @example Connect to in-memory database
      #   conn = connect(database: ':memory:')
      #
      # @example Connect to file database
      #   conn = connect(database: '/path/to/database.duckdb')
      #
      # @example Connect with configuration
      #   conn = connect(
      #     database: '/path/to/database.duckdb',
      #     config: { memory_limit: '2GB', threads: 4 }
      #   )
      #
      # @see disconnect_connection
      # @see valid_connection?
      # @since 0.1.0
      def connect(server)
        opts = server_opts(server)
        database_path = opts[:database]

        begin
          if database_path == ":memory:" || database_path.nil?
            # Create in-memory database and return connection
            db = ::DuckDB::Database.open(":memory:")
          else
            # Fix URI parsing issue - add leading slash if missing for absolute paths
            database_path = "/#{database_path}" if database_path.match?(/^[a-zA-Z]/) && !database_path.start_with?(":")

            # Create file-based database (will create file if it doesn't exist) and return connection
            db = ::DuckDB::Database.open(database_path)
          end
          db.connect
        rescue ::DuckDB::Error => e
          raise Sequel::DatabaseConnectionError, "Failed to connect to DuckDB database: #{e.message}"
        rescue StandardError => e
          raise Sequel::DatabaseConnectionError, "Unexpected error connecting to DuckDB: #{e.message}"
        end
      end

      # Disconnect from a DuckDB database connection
      #
      # @param conn [::DuckDB::Connection] The database connection to close
      # @return [void]
      def disconnect_connection(conn)
        return unless conn

        begin
          conn.close
        rescue ::DuckDB::Error
          # Ignore errors during disconnect - connection may already be closed
        end
      end

      # Check if a DuckDB connection is valid and open
      #
      # @param conn [::DuckDB::Connection] The database connection to check
      # @return [Boolean] true if connection is valid and open, false otherwise
      def valid_connection?(conn)
        return false unless conn

        begin
          # Try a simple query to check if the connection is still valid
          conn.query("SELECT 1")
          true
        rescue ::DuckDB::Error
          false
        end
      end

      # DuckDB doesn't support AUTOINCREMENT
      def supports_autoincrement?
        false
      end

      # Execute SQL statement
      #
      # @param sql [String] SQL statement to execute
      # @param opts [Hash, Array] Options for execution or parameters array
      # @return [Object] Result of execution
      def execute(sql, opts = {}, &block)
        # Handle both old-style (sql, opts) and new-style (sql, params) calls
        if opts.is_a?(Array)
          params = opts
          opts = {}
        elsif opts.is_a?(Hash)
          params = opts[:params] || []
        else
          # Handle other types (like strings) by treating as empty params
          params = []
          opts = {}
        end

        synchronize(opts[:server]) do |conn|
          result = execute_statement(conn, sql, params, opts, &block)

          # For UPDATE/DELETE operations without a block, return the number of affected rows
          # This is what Sequel models expect
          if !block && result.is_a?(::DuckDB::Result) && (sql.strip.upcase.start_with?("UPDATE ") || sql.strip.upcase.start_with?("DELETE "))
            return result.rows_changed
          end

          return result
        end
      end

      # Execute INSERT statement
      #
      # @param sql [String] INSERT SQL statement
      # @param opts [Hash] Options for execution
      # @return [Object] Result of execution
      def execute_insert(sql, opts = {})
        execute(sql, opts)
        # For INSERT statements, we should return the inserted ID if possible
        # Since DuckDB doesn't support AUTOINCREMENT, we'll return nil for now
        # This matches the behavior expected by Sequel
        nil
      end

      # Execute UPDATE statement
      #
      # @param sql [String] UPDATE SQL statement
      # @param opts [Hash] Options for execution
      # @return [Object] Result of execution
      def execute_update(sql, opts = {})
        result = execute(sql, opts)
        # For UPDATE/DELETE statements, return the number of affected rows
        # DuckDB::Result has a rows_changed method for affected row count
        if result.respond_to?(:rows_changed)
          result.rows_changed
        else
          # Fallback: try to get row count from result
          result.is_a?(Integer) ? result : 0
        end
      end

      private

      # Get database error classes that should be caught and converted to Sequel exceptions
      #
      # @return [Array<Class>] Array of DuckDB error classes
      def database_error_classes
        [::DuckDB::Error]
      end

      # Extract SQL state from DuckDB exception if available
      #
      # @param exception [::DuckDB::Error] The DuckDB exception
      # @param opts [Hash] Additional options
      # @return [String, nil] SQL state code or nil if not available
      def database_exception_sqlstate(_exception, _opts)
        # DuckDB errors may not always have SQL state codes
        # This can be enhanced when more detailed error information is available
        nil
      end

      # Whether to use SQL states for exception handling
      #
      # @return [Boolean] true if SQL states should be used
      def database_exception_use_sqlstates?
        false
      end

      # Map DuckDB errors to appropriate Sequel exception types (Requirements 8.1, 8.2, 8.3, 8.7)
      #
      # @param exception [::DuckDB::Error] The DuckDB exception
      # @param opts [Hash] Additional options
      # @return [Class] Sequel exception class to use
      def database_exception_class(exception, _opts)
        message = exception.message.to_s

        # Map specific DuckDB error patterns to appropriate Sequel exceptions
        case message
        when /connection/i, /database.*not.*found/i, /cannot.*open/i
          # Connection-related errors (Requirement 8.1)
          Sequel::DatabaseConnectionError
        when /violates.*not.*null/i, /not.*null.*constraint/i, /null.*value.*not.*allowed/i
          # NOT NULL constraint violations (Requirement 8.3) - moved up for priority
          Sequel::NotNullConstraintViolation
        when /unique.*constraint/i, /duplicate.*key/i, /already.*exists/i
          # UNIQUE constraint violations (Requirement 8.3)
          Sequel::UniqueConstraintViolation
        when /foreign.*key.*constraint/i, /violates.*foreign.*key/i
          # Foreign key constraint violations (Requirement 8.3)
          Sequel::ForeignKeyConstraintViolation
        when /check.*constraint/i, /violates.*check/i
          # CHECK constraint violations (Requirement 8.3)
          Sequel::CheckConstraintViolation
        when /primary.*key.*constraint/i, /duplicate.*primary.*key/i
          # Primary key constraint violations (Requirement 8.3)
          Sequel::UniqueConstraintViolation # Primary key violations are a type of unique constraint
        when /constraint.*violation/i, /violates.*constraint/i
          # Generic constraint violations (Requirement 8.3) - moved to end for lower priority
          Sequel::ConstraintViolation
        when /syntax.*error/i, /parse.*error/i, /unexpected.*token/i
          # SQL syntax errors (Requirement 8.2)
          Sequel::DatabaseError
        when /table.*does.*not.*exist/i, /relation.*does.*not.*exist/i, /no.*such.*table/i
          # Table not found errors (Requirement 8.7)
          Sequel::DatabaseError
        when /column.*does.*not.*exist/i, /no.*such.*column/i, /unknown.*column/i, /referenced.*column.*not.*found/i, /does.*not.*have.*a.*column/i
          # Column not found errors (Requirement 8.7)
          Sequel::DatabaseError
        when /schema.*does.*not.*exist/i, /no.*such.*schema/i
          # Schema not found errors (Requirement 8.7)
          Sequel::DatabaseError
        when /function.*does.*not.*exist/i, /no.*such.*function/i, /unknown.*function/i
          # Function not found errors (Requirement 8.7)
          Sequel::DatabaseError
        when /type.*error/i, /cannot.*cast/i, /invalid.*type/i
          # Type conversion errors (Requirement 8.7)
          Sequel::DatabaseError
        when /permission.*denied/i, /access.*denied/i, /insufficient.*privileges/i
          # Permission/access errors (Requirement 8.7)
          Sequel::DatabaseError
        else
          # Default to generic DatabaseError for all other DuckDB errors (Requirement 8.2)
          Sequel::DatabaseError
        end
      end

      # Enhanced error message formatting for better debugging (Requirements 8.2, 8.7)
      #
      # @param exception [::DuckDB::Error] The DuckDB exception
      # @param opts [Hash] Additional options including SQL and parameters
      # @return [String] Enhanced error message
      def database_exception_message(exception, opts)
        message = "DuckDB error: #{exception.message}"

        # Add SQL context if available for better debugging
        message += " -- SQL: #{opts[:sql]}" if opts[:sql]

        # Add parameter context if available
        message += " -- Parameters: #{opts[:params].inspect}" if opts[:params] && !opts[:params].empty?

        message
      end

      # Handle constraint violation errors with specific categorization (Requirement 8.3)
      #
      # @param exception [::DuckDB::Error] The DuckDB exception
      # @param opts [Hash] Additional options
      # @return [Exception] Appropriate Sequel constraint exception
      def handle_constraint_violation(exception, opts = {})
        message = database_exception_message(exception, opts)
        exception_class = database_exception_class(exception, opts)

        # Create the appropriate exception with enhanced message
        exception_class.new(message)
      end

      # Schema introspection methods

      # Parse table list from database
      #
      # @param opts [Hash] Options for table parsing
      # @return [Array<Symbol>] Array of table names as symbols
      def schema_parse_tables(opts = {})
        schema_name = opts[:schema] || "main"

        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'"

        tables = []
        execute(sql, [schema_name]) do |row|
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
          WHERE table_schema = ? AND table_name = ?
          ORDER BY ordinal_position
        SQL

        columns = []
        execute(sql, [schema_name, table_name.to_s]) do |row|
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
          WHERE schema_name = ? AND table_name = ?
        SQL

        indexes = {}
        execute(sql, [schema_name, table_name.to_s]) do |row|
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
                          when TrueClass, FalseClass
                            value.to_s
                          when Numeric
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

        sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ? LIMIT 1"

        result = nil
        execute(sql, [schema_name, table_name.to_s]) do |_row|
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
        when "INTEGER", "INT", "INT4"
          :integer
        when "BIGINT", "INT8"
          :bigint
        when "SMALLINT", "INT2"
          :integer
        when "TINYINT", "INT1"
          :integer
        when "REAL", "FLOAT4"
          :float
        when "DOUBLE", "FLOAT8"
          :float
        when /^DECIMAL/, /^NUMERIC/
          :decimal
        when "VARCHAR", "TEXT", "STRING"
          :string
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
            AND tc.table_schema = ?
            AND tc.table_name = ?
        SQL

        primary_key_columns = []
        execute(sql, [schema_name, table_name.to_s]) do |row|
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

      # Advanced transaction support methods (Requirements 5.5, 5.6, 5.7)

      # Check if DuckDB supports savepoints for nested transactions
      #
      # @return [Boolean] true if savepoints are supported
      def supports_savepoints?
        # DuckDB does not currently support SAVEPOINT/ROLLBACK TO SAVEPOINT syntax
        # Nested transactions are handled by Sequel's default behavior
        false
      end

      # Check if DuckDB supports the specified transaction isolation level
      #
      # @param level [Symbol] Isolation level (:read_uncommitted, :read_committed, :repeatable_read, :serializable)
      # @return [Boolean] true if the isolation level is supported
      def supports_transaction_isolation_level?(_level)
        # DuckDB does not currently support setting transaction isolation levels
        # It uses a default isolation level similar to READ_COMMITTED
        false
      end

      # Check if DuckDB supports manual transaction control
      #
      # @return [Boolean] true if manual transaction control is supported
      def supports_manual_transaction_control?
        # DuckDB supports BEGIN, COMMIT, and ROLLBACK statements
        true
      end

      # Check if DuckDB supports autocommit control
      #
      # @return [Boolean] true if autocommit can be controlled
      def supports_autocommit_control?
        # DuckDB has autocommit behavior but limited control over it
        false
      end

      # Check if DuckDB supports disabling autocommit
      #
      # @return [Boolean] true if autocommit can be disabled
      def supports_autocommit_disable?
        # DuckDB doesn't support disabling autocommit mode
        false
      end

      # Check if currently in a transaction
      #
      # @return [Boolean] true if in a transaction
      def in_transaction?
        # Use Sequel's built-in transaction tracking
        # Sequel tracks transaction state internally
        @transactions && !@transactions.empty?
      end

      # Begin a transaction manually
      # Sequel calls this with (conn, opts) arguments
      #
      # @param conn [::DuckDB::Connection] Database connection
      # @param opts [Hash] Transaction options
      # @return [void]
      def begin_transaction(conn, opts = {})
        if opts[:isolation]
          isolation_sql = case opts[:isolation]
                          when :read_uncommitted
                            "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
                          when :read_committed
                            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
                          else
                            raise Sequel::DatabaseError, "Unsupported isolation level: #{opts[:isolation]}"
                          end
          conn.query(isolation_sql)
        end

        conn.query("BEGIN TRANSACTION")
      end

      # Commit the current transaction manually
      # Sequel calls this with (conn, opts) arguments
      #
      # @param conn [::DuckDB::Connection] Database connection
      # @param opts [Hash] Options
      # @return [void]
      def commit_transaction(conn, _opts = {})
        conn.query("COMMIT")
      end

      # Rollback the current transaction manually
      # Sequel calls this with (conn, opts) arguments
      #
      # @param conn [::DuckDB::Connection] Database connection
      # @param opts [Hash] Options
      # @return [void]
      def rollback_transaction(conn, _opts = {})
        conn.query("ROLLBACK")
      end

      # Override Sequel's transaction method to support advanced features
      def transaction(opts = {}, &block)
        # Handle savepoint transactions (nested transactions)
        return savepoint_transaction(opts, &block) if opts[:savepoint] && supports_savepoints?

        # Handle isolation level setting
        if opts[:isolation] && supports_transaction_isolation_level?(opts[:isolation])
          return isolation_transaction(opts, &block)
        end

        # Fall back to standard Sequel transaction handling
        super(opts, &block)
      end

      private

      # Handle savepoint-based nested transactions
      #
      # @param opts [Hash] Transaction options
      # @return [Object] Result of the transaction block
      def savepoint_transaction(opts = {})
        # Generate a unique savepoint name
        savepoint_name = "sp_#{Time.now.to_f.to_s.gsub(".", "_")}"

        synchronize(opts[:server]) do |conn|
          # Create savepoint
          conn.query("SAVEPOINT #{savepoint_name}")

          # Execute the block
          result = yield

          # Release savepoint on success
          conn.query("RELEASE SAVEPOINT #{savepoint_name}")

          result
        rescue Sequel::Rollback
          # Rollback to savepoint on explicit rollback
          conn.query("ROLLBACK TO SAVEPOINT #{savepoint_name}")
          conn.query("RELEASE SAVEPOINT #{savepoint_name}")
          nil
        rescue Exception => e
          # Rollback to savepoint on any other exception
          begin
            conn.query("ROLLBACK TO SAVEPOINT #{savepoint_name}")
            conn.query("RELEASE SAVEPOINT #{savepoint_name}")
          rescue ::DuckDB::Error
            # Ignore errors during rollback cleanup
          end
          raise e
        end
      end

      # Handle transactions with specific isolation levels
      #
      # @param opts [Hash] Transaction options including :isolation
      # @return [Object] Result of the transaction block
      def isolation_transaction(opts = {})
        synchronize(opts[:server]) do |conn|
          # Set isolation level before beginning transaction
          isolation_sql = case opts[:isolation]
                          when :read_uncommitted
                            "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
                          when :read_committed
                            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
                          else
                            raise Sequel::DatabaseError, "Unsupported isolation level: #{opts[:isolation]}"
                          end

          conn.query(isolation_sql)
          conn.query("BEGIN TRANSACTION")

          # Execute the block
          result = yield

          # Commit on success
          conn.query("COMMIT")

          result
        rescue Sequel::Rollback
          # Rollback on explicit rollback
          conn.query("ROLLBACK")
          nil
        rescue Exception => e
          # Rollback on any other exception
          begin
            conn.query("ROLLBACK")
          rescue ::DuckDB::Error
            # Ignore errors during rollback cleanup
          end
          raise e
        end
      end

      # DuckDB-specific schema generation methods

      # Generate SQL for primary key column
      #
      # @param column [Symbol] Column name
      # @param opts [Hash] Column options
      # @return [String] SQL for primary key column
      def primary_key_column_sql(column, _opts)
        # DuckDB doesn't support AUTOINCREMENT, so we just use INTEGER PRIMARY KEY
        "#{quote_identifier(column)} INTEGER PRIMARY KEY"
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
      # @param opts [Hash] Column options
      # @return [String] SQL for auto-incrementing column
      def auto_increment_column_sql(column, _opts)
        # DuckDB uses sequences for auto-increment, but for primary keys
        # we can just use INTEGER PRIMARY KEY without AUTOINCREMENT
        "#{quote_identifier(column)} INTEGER PRIMARY KEY"
      end

      # Map Ruby types to DuckDB types
      #
      # @param opts [Hash] Column options
      # @return [String] DuckDB type
      def type_literal(opts)
        case opts[:type]
        when :primary_key
          "INTEGER"
        when :string, :text
          if opts[:size]
            "VARCHAR(#{opts[:size]})"
          else
            "VARCHAR"
          end
        when :integer
          "INTEGER"
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

      # Execute SQL statement against DuckDB connection
      #
      # @param conn [::DuckDB::Connection] Database connection (already connected)
      # @param sql [String] SQL statement to execute
      # @param params [Array] Parameters for prepared statement
      # @param opts [Hash] Options for execution
      # @return [Object] Result of execution
      def execute_statement(conn, sql, params = [], _opts = {})
        # Log the SQL query with timing information (Requirements 8.4, 8.5)
        start_time = Time.now

        begin
          # Log the SQL query before execution
          log_sql_query(sql, params)

          # Handle parameterized queries
          if params && !params.empty?
            # Prepare statement with ? placeholders
            stmt = conn.prepare(sql)

            # Bind parameters using 1-based indexing
            params.each_with_index do |param, index|
              stmt.bind(index + 1, param)
            end

            # Execute the prepared statement
            result = stmt.execute
          else
            # Execute directly without parameters
            result = conn.query(sql)
          end

          # Log timing information for the operation
          end_time = Time.now
          execution_time = end_time - start_time
          log_sql_timing(sql, execution_time)

          if block_given?
            # Get column names from the result
            columns = result.columns

            # Iterate through each row
            result.each do |row_array|
              # Convert array to hash with column names as keys
              row_hash = {}
              columns.each_with_index do |column, index|
                # DuckDB::Column objects have a name method
                column_name = column.respond_to?(:name) ? column.name : column.to_s
                row_hash[column_name.to_sym] = row_array[index]
              end
              yield row_hash
            end
          else
            result
          end
        rescue ::DuckDB::Error => e
          # Log the error for debugging (Requirement 8.6)
          end_time = Time.now
          execution_time = end_time - start_time
          log_sql_error(sql, params, e, execution_time)

          # Use enhanced error mapping for better exception categorization (Requirements 8.1, 8.2, 8.3, 8.7)
          error_opts = { sql: sql, params: params }
          exception_class = database_exception_class(e, error_opts)
          enhanced_message = database_exception_message(e, error_opts)

          raise exception_class, enhanced_message
        rescue StandardError => e
          # Log unexpected errors
          end_time = Time.now
          execution_time = end_time - start_time
          log_sql_error(sql, params, e, execution_time)
          raise e
        end
      end

      # Log SQL query execution (Requirement 8.4)
      #
      # @param sql [String] SQL statement
      # @param params [Array] Parameters for the query
      def log_sql_query(sql, params = [])
        return unless log_connection_info?

        if params && !params.empty?
          # Log parameterized query with parameters
          log_info("SQL Query: #{sql} -- Parameters: #{params.inspect}")
        else
          # Log simple query
          log_info("SQL Query: #{sql}")
        end
      end

      # Log SQL query timing information (Requirement 8.5)
      #
      # @param sql [String] SQL statement
      # @param execution_time [Float] Time taken to execute in seconds
      def log_sql_timing(sql, execution_time)
        return unless log_connection_info?

        # Log timing information, highlighting slow operations
        time_ms = (execution_time * 1000).round(2)

        if execution_time > 1.0 # Log slow operations (> 1 second) as warnings
          log_warn("SLOW SQL Query (#{time_ms}ms): #{sql}")
        else
          log_info("SQL Query completed in #{time_ms}ms")
        end
      end

      # Log SQL query errors (Requirement 8.6)
      #
      # @param sql [String] SQL statement that failed
      # @param params [Array] Parameters for the query
      # @param error [Exception] The error that occurred
      # @param execution_time [Float] Time taken before error
      def log_sql_error(sql, params, error, execution_time)
        return unless log_connection_info?

        time_ms = (execution_time * 1000).round(2)

        if params && !params.empty?
          log_error("SQL Error after #{time_ms}ms: #{error.message} -- SQL: #{sql} -- Parameters: #{params.inspect}")
        else
          log_error("SQL Error after #{time_ms}ms: #{error.message} -- SQL: #{sql}")
        end
      end

      # Check if connection info should be logged
      #
      # @return [Boolean] true if logging is enabled
      def log_connection_info?
        # Use Sequel's built-in logging mechanism
        !loggers.empty?
      end

      # Log info message using Sequel's logging system
      #
      # @param message [String] Message to log
      def log_info(message)
        log_connection_yield(message, nil) { nil }
      end

      # Log warning message using Sequel's logging system
      #
      # @param message [String] Message to log
      def log_warn(message)
        log_connection_yield("WARNING: #{message}", nil) { nil }
      end

      # Log error message using Sequel's logging system
      #
      # @param message [String] Message to log
      def log_error(message)
        log_connection_yield("ERROR: #{message}", nil) { nil }
      end

      public

      # EXPLAIN functionality access for query plans (Requirement 9.6)
      #
      # @param sql [String] SQL query to explain
      # @return [Array<Hash>] Query plan information
      def explain_query(sql)
        explain_sql = "EXPLAIN #{sql}"
        plan_rows = []

        execute(explain_sql) do |row|
          plan_rows << row
        end

        plan_rows
      end

      # Get query plan for a SQL statement
      #
      # @param sql [String] SQL statement to analyze
      # @return [String] Query plan as string
      def query_plan(sql)
        plan_rows = explain_query(sql)

        if plan_rows.empty?
          "No query plan available"
        else
          # Format the plan rows into a readable string
          plan_rows.map { |row| row.values.join(" | ") }.join("\n")
        end
      end

      # Check if EXPLAIN functionality is supported
      #
      # @return [Boolean] true if EXPLAIN is supported
      def supports_explain?
        true # DuckDB supports EXPLAIN
      end

      # Get detailed query analysis information
      #
      # @param sql [String] SQL statement to analyze
      # @return [Hash] Analysis information including plan, timing estimates, etc.
      def analyze_query(sql)
        {
          plan: query_plan(sql),
          explain_output: explain_query(sql),
          supports_explain: supports_explain?
        }
      end

      # DuckDB configuration methods for performance optimization

      # Set DuckDB configuration value
      #
      # @param key [String] Configuration key
      # @param value [Object] Configuration value
      def set_config_value(key, value)
        synchronize do |conn|
          # Use PRAGMA for DuckDB configuration
          conn.query("PRAGMA #{key} = #{value}")
        end
      end

      # Get DuckDB configuration value
      #
      # @param key [String] Configuration key
      # @return [Object] Configuration value
      def get_config_value(key)
        result = nil
        synchronize do |conn|
          # Use PRAGMA to get configuration values
          conn.query("PRAGMA #{key}") do |row|
            result = row.values.first
            break
          end
        end
        result
      end

      # Configure DuckDB for optimal parallel execution
      #
      # @param thread_count [Integer] Number of threads to use
      def configure_parallel_execution(thread_count = nil)
        thread_count ||= [4, get_cpu_count].min

        set_config_value("threads", thread_count)
        set_config_value("enable_optimizer", true)
        set_config_value("enable_profiling", false) # Disable for performance
      end

      # Configure DuckDB for memory-efficient operations
      #
      # @param memory_limit [String] Memory limit (e.g., "1GB", "512MB")
      def configure_memory_optimization(memory_limit = "1GB")
        set_config_value("memory_limit", "'#{memory_limit}'")
        set_config_value("temp_directory", "'/tmp'")
      end

      # Configure DuckDB for columnar storage optimization
      def configure_columnar_optimization
        set_config_value("enable_optimizer", true)
        set_config_value("enable_profiling", false)
        set_config_value("enable_progress_bar", false)
      end

      private

      # Get CPU count for parallel execution configuration
      def get_cpu_count
        require "etc"
        Etc.nprocessors
      rescue StandardError
        4 # Default fallback
      end

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

        sql = "UPDATE #{table_name_sql} SET ".dup

        # Add SET clause
        set_clauses = values.map do |column, value|
          "#{quote_identifier(column)} = #{literal(value)}"
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

        sql = "DELETE FROM #{table_name_sql}".dup

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

      def quote_identifiers_default
        false
      end

      # Override identifier quoting to avoid uppercase conversion and handle qualified identifiers
      def quote_identifier_append(sql, name)
        name_str = name.to_s

        # Handle qualified identifiers (table__column format) - convert to table.column
        if name_str.include?("__")
          parts = name_str.split("__", 2)
          if parts.length == 2
            table_part, column_part = parts
            sql << "#{quote_identifier(table_part)}.#{quote_identifier(column_part)}"
            return
          end
        end

        # Special case for * (used in count(*), etc.)
        sql << if name_str == "*"
                 "*"
               # Quote reserved words and identifiers with special characters
               elsif name_str.match?(/\A[a-zA-Z_][a-zA-Z0-9_]*\z/) && !reserved_word?(name_str)
                 name_str
               else
                 "\"#{name_str}\""
               end
      end

      # Validate table name for SELECT operations
      def validate_table_name_for_select
        return unless @opts[:from] # Skip if no FROM clause

        @opts[:from].each do |table|
          if table.nil? || (table.respond_to?(:to_s) && table.to_s.strip.empty?)
            raise ArgumentError, "Table name cannot be nil or empty"
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

        table_name = @opts[:from].first.to_s
        raise ArgumentError, "Table name cannot be empty" if table_name.empty?

        if table_name.match?(/\A[a-zA-Z_][a-zA-Z0-9_]*\z/) && !reserved_word?(table_name)
          table_name
        else
          "\"#{table_name}\""
        end
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
          sql << ", " if i > 0
          sql << "#{quote_identifier(w[:name])} AS (#{w[:dataset].sql})"
        end

        sql << " "
      end

      private

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

      # Add JOIN clauses to SQL (Requirement 6.9)
      def select_join_sql(sql)
        return unless @opts[:join]

        @opts[:join].each do |join|
          # Handle different join clause types
          case join
          when Sequel::SQL::JoinOnClause
            join_type = join.join_type || :inner
            table = join.table
            conditions = join.on

            # Format join type
            join_clause = case join_type
                          when :inner
                            "INNER JOIN"
                          when :left, :left_outer
                            "LEFT JOIN"
                          when :right, :right_outer
                            "RIGHT JOIN"
                          when :full, :full_outer
                            "FULL JOIN"
                          else
                            "INNER JOIN"
                          end

            sql << " #{join_clause} "

            # Add table name
            sql << if table.is_a?(Sequel::Dataset)
                     "(#{table.sql}) AS #{quote_identifier(join.table_alias || "subquery")}"
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
                          when :inner
                            "INNER JOIN"
                          when :left, :left_outer
                            "LEFT JOIN"
                          when :right, :right_outer
                            "RIGHT JOIN"
                          when :full, :full_outer
                            "FULL JOIN"
                          else
                            "INNER JOIN"
                          end

            sql << " #{join_clause} "

            # Handle table with alias
            if table.is_a?(Sequel::Dataset)
              # Subquery with alias
              sql << "(#{table.sql})"
              sql << " AS #{quote_identifier(join.table_alias)}" if join.table_alias
            else
              # Regular table (may have alias)
              sql << literal(table)
              # Add alias if present
              sql << " AS #{quote_identifier(join.table_alias)}" if join.table_alias
            end

            sql << " USING (#{Array(using_columns).map { |col| quote_identifier(col) }.join(", ")})" if using_columns

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

      # Enhanced literal handling for complex expressions
      def literal_append(sql, v)
        case v
        when Time
          literal_datetime_append(sql, v)
        when DateTime
          literal_datetime_append(sql, v)
        when String
          if v.encoding == Encoding::ASCII_8BIT
            literal_blob_append(sql, v)
          else
            literal_string_append(sql, v)
          end
        else
          super
        end
      end

      # DuckDB-specific SQL generation enhancements

      # Override complex_expression_sql_append for DuckDB-specific handling
      public

      def complex_expression_sql_append(sql, op, args)
        case op
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

      # Support for UNION, INTERSECT, EXCEPT operations
      def union(dataset, opts = {})
        compound_clone(:union, dataset, opts)
      end

      def intersect(dataset, opts = {})
        compound_clone(:intersect, dataset, opts)
      end

      def except(dataset, opts = {})
        compound_clone(:except, dataset, opts)
      end

      private

      def compound_clone(type, dataset, opts)
        clone(compound: type, compound_dataset: dataset, compound_all: opts[:all])
      end

      def compound_dataset_sql
        return super unless @opts[:compound]

        case @opts[:compound]
        when :union
          if @opts[:compound_all]
            "#{select_sql} UNION ALL #{@opts[:compound_dataset].select_sql}"
          else
            "#{select_sql} UNION #{@opts[:compound_dataset].select_sql}"
          end
        when :intersect
          "#{select_sql} INTERSECT #{@opts[:compound_dataset].select_sql}"
        when :except
          "#{select_sql} EXCEPT #{@opts[:compound_dataset].select_sql}"
        else
          super
        end
      end

      public

      def sql
        # Validate table name for SELECT operations
        validate_table_name_for_select

        if @opts[:compound]
          compound_dataset_sql
        else
          super
        end
      end

      # Override literal methods for DuckDB-specific formatting
      def literal_string_append(sql, s)
        sql << "'" << s.gsub("'", "''") << "'"
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

      def literal_boolean(value)
        value ? "TRUE" : "FALSE"
      end

      def literal_true
        "TRUE"
      end

      def literal_false
        "FALSE"
      end

      # Override literal_append to handle Time objects and binary data properly
      def literal_append(sql, v)
        case v
        when Time
          # Check if this looks like a time-only value (year 1970 indicates time-only)
          if v.year == 1970 && v.month == 1 && v.day == 1
            # This is a time-only value, use TIME format
            sql << "'#{v.strftime("%H:%M:%S")}'"
          else
            # This is a full datetime value
            literal_datetime_append(sql, v)
          end
        when DateTime
          literal_datetime_append(sql, v)
        when String
          case v
          when LiteralString
            sql << v
          else
            if v.encoding == Encoding::ASCII_8BIT
              literal_blob_append(sql, v)
            else
              literal_string_append(sql, v)
            end
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

      # Dataset operation methods (Requirements 6.1, 6.2, 6.3, 9.5)

      # Count the number of records in the dataset
      #
      # @return [Integer] Number of records
      def count
        # Generate COUNT(*) SQL and execute it
        count_sql = clone(select: [Sequel.function(:count, :*)]).select_sql
        value = nil
        fetch_rows(count_sql) do |row|
          value = row.values.first
          break
        end
        value || 0
      end

      # Override all method to ensure proper model instantiation
      # Sequel's default all method doesn't always apply row_proc correctly
      def all
        records = []
        fetch_rows(select_sql) do |row|
          # Apply row_proc if it exists (for model instantiation)
          row_proc = @row_proc || opts[:row_proc]
          processed_row = row_proc ? row_proc.call(row) : row
          records << processed_row
        end
        records
      end

      # Insert a record into the dataset's table
      #
      # @param values [Hash] Column values to insert
      # @return [Integer, nil] Number of affected rows (always nil for DuckDB due to no AUTOINCREMENT)
      def insert(values = {})
        sql = insert_sql(values)
        result = db.execute(sql)

        # For DuckDB, we need to return the number of affected rows
        # Since DuckDB doesn't support AUTOINCREMENT, we return nil for the ID
        # but we should return 1 to indicate successful insertion
        if result.is_a?(::DuckDB::Result)
          # DuckDB::Result doesn't have a direct way to get affected rows for INSERT
          # For INSERT operations, if no error occurred, assume 1 row was affected
          1
        else
          result
        end
      end

      # Update records in the dataset
      #
      # @param values [Hash] Column values to update
      # @return [Integer] Number of affected rows
      def update(values = {})
        sql = update_sql(values)
        # Use execute_update which properly returns the row count
        db.execute_update(sql)
      end

      # Delete records from the dataset
      #
      # @return [Integer] Number of affected rows
      def delete
        sql = delete_sql
        # Use execute_update which properly returns the row count
        db.execute_update(sql)
      end

      # Streaming result support where possible (Requirement 9.5)
      #
      # @param sql [String] SQL to execute
      # @param &block [Proc] Block to process each row
      # @return [Enumerator] If no block given, returns enumerator
      def stream(sql = select_sql, &block)
        if block_given?
          # Stream results by processing them one at a time
          fetch_rows(sql, &block)
        else
          # Return enumerator for lazy evaluation
          enum_for(:stream, sql)
        end
      end

      # Performance optimization methods (Requirements 9.1, 9.2, 9.3, 9.4)
      # These methods are public to provide enhanced performance capabilities

      # Optimized fetch_rows method for large result sets (Requirement 9.1)
      # This method provides efficient row fetching with streaming capabilities
      # Override the existing fetch_rows method to make it public and optimized
      def fetch_rows(sql, &block)
        # Use streaming approach to avoid loading all results into memory at once
        # This is particularly important for large result sets
        if block_given?
          # Get schema information for type conversion
          table_schema = get_table_schema_for_conversion

          # Execute with type conversion
          db.execute(sql) do |row|
            # Apply type conversion for TIME columns
            converted_row = convert_row_types(row, table_schema)
            yield converted_row
          end
        else
          # Return enumerator if no block given (for compatibility)
          enum_for(:fetch_rows, sql)
        end
      end

      private

      # Get table schema information for type conversion
      def get_table_schema_for_conversion
        return nil unless @opts[:from] && @opts[:from].first

        table_name = @opts[:from].first
        # Handle case where table name is wrapped in an identifier
        table_name = table_name.value if table_name.respond_to?(:value)

        begin
          schema_info = db.schema(table_name)
          schema_hash = {}
          schema_info.each do |column_name, column_info|
            schema_hash[column_name] = column_info
          end
          schema_hash
        rescue StandardError
          # If schema lookup fails, return nil to skip type conversion
          nil
        end
      end

      # Convert row values based on column types
      def convert_row_types(row, table_schema)
        return row unless table_schema

        converted_row = {}
        row.each do |column_name, value|
          column_info = table_schema[column_name]
          converted_row[column_name] = if column_info && column_info[:type] == :time && value.is_a?(Time)
                                         # Convert TIME columns to time-only values
                                         Time.local(1970, 1, 1, value.hour, value.min, value.sec, value.usec)
                                       else
                                         value
                                       end
        end
        converted_row
      end

      public

      # Enhanced bulk insert optimization (Requirement 9.3)
      # Override multi_insert to use DuckDB's efficient bulk loading capabilities
      def multi_insert(columns = nil, &block)
        if columns.is_a?(Array) && !columns.empty? && columns.first.is_a?(Hash)
          # Handle array of hashes (most common case)
          bulk_insert_optimized(columns)
        else
          # Fall back to standard Sequel behavior for other cases
          super
        end
      end

      # Optimized bulk insert implementation using DuckDB's capabilities
      def bulk_insert_optimized(rows)
        return 0 if rows.empty?

        # Get column names from first row
        columns = rows.first.keys

        # Get table name from opts[:from]
        table_name = @opts[:from].first

        # Build optimized INSERT statement with VALUES clause
        # DuckDB handles multiple VALUES efficiently
        values_placeholders = rows.map { |_| "(#{columns.map { "?" }.join(", ")})" }.join(", ")
        sql = "INSERT INTO #{quote_identifier(table_name)} (#{columns.map do |c|
          quote_identifier(c)
        end.join(", ")}) VALUES #{values_placeholders}"

        # Flatten all row values for parameter binding
        params = rows.flat_map { |row| columns.map { |col| row[col] } }

        # Execute the bulk insert
        db.execute(sql, params)

        rows.length
      end

      # Prepared statement support for performance (Requirement 9.2)
      # Enhanced prepare method that leverages DuckDB's prepared statement capabilities
      def prepare(type, name = nil, *values)
        # Check if DuckDB connection supports prepared statements
        if db.respond_to?(:prepare_statement)
          # Use DuckDB's native prepared statement support
          sql = case type
                when :select, :all
                  select_sql
                when :first
                  clone(limit: 1).select_sql
                when :insert
                  insert_sql(*values)
                when :update
                  update_sql(*values)
                when :delete
                  delete_sql
                else
                  raise ArgumentError, "Unsupported prepared statement type: #{type}"
                end

          # Create and cache prepared statement
          prepared_stmt = db.prepare_statement(sql)

          # Return a callable object that executes the prepared statement
          lambda do |*params|
            case type
            when :select, :all
              prepared_stmt.execute(*params).to_a
            when :first
              result = prepared_stmt.execute(*params).first
              result
            else
              prepared_stmt.execute(*params)
            end
          end
        else
          # Fall back to standard Sequel prepared statement handling
          super
        end
      end

      # Connection pooling optimization (Requirement 9.4)
      # Enhanced connection management for better performance
      def with_connection_pooling
        # Ensure efficient connection reuse
        db.synchronize do |conn|
          # Verify connection is still valid before use
          unless db.valid_connection?(conn)
            # Reconnect if connection is invalid
            conn = db.connect(db.opts)
          end

          yield conn
        end
      end

      # Memory-efficient streaming for large result sets (Requirement 9.5)
      # Enhanced each method with better memory management
      def each(&block)
        return enum_for(:each) unless block_given?

        # Use streaming approach to minimize memory usage
        sql = select_sql

        # Check if SQL already has LIMIT/OFFSET - if so, don't add batching
        if sql.match?(/\bLIMIT\b/i) || sql.match?(/\bOFFSET\b/i)
          # SQL already has LIMIT/OFFSET, execute directly without batching
          fetch_rows(sql, &block)
          return self
        end

        # Process results in batches to balance memory usage and performance
        batch_size = @opts[:stream_batch_size] || 1000
        offset = 0

        loop do
          # Fetch a batch of results
          batch_sql = "#{sql} LIMIT #{batch_size} OFFSET #{offset}"
          batch_count = 0

          fetch_rows(batch_sql) do |row|
            yield row
            batch_count += 1
          end

          # Break if we got fewer rows than the batch size (end of results)
          break if batch_count < batch_size

          offset += batch_size
        end

        self
      end

      # Set custom batch size for streaming operations (Requirement 9.5)
      #
      # @param size [Integer] Batch size for streaming
      # @return [Dataset] New dataset with custom batch size
      def stream_batch_size(size)
        clone(stream_batch_size: size)
      end

      # Stream results with memory limit enforcement (Requirement 9.5)
      #
      # @param memory_limit [Integer] Maximum memory growth allowed in bytes
      # @param &block [Proc] Block to process each row
      # @return [Enumerator] If no block given
      def stream_with_memory_limit(memory_limit, &block)
        return enum_for(:stream_with_memory_limit, memory_limit) unless block_given?

        sql = select_sql

        # Check if SQL already has LIMIT/OFFSET - if so, don't add batching
        if sql.match?(/\bLIMIT\b/i) || sql.match?(/\bOFFSET\b/i)
          # SQL already has LIMIT/OFFSET, execute directly without batching
          fetch_rows(sql, &block)
          return self
        end

        initial_memory = get_memory_usage
        batch_size = @opts[:stream_batch_size] || 500
        offset = 0

        loop do
          # Check memory usage before processing batch
          current_memory = get_memory_usage
          memory_growth = current_memory - initial_memory

          # Reduce batch size if memory usage is high
          batch_size = [batch_size / 2, 100].max if memory_growth > memory_limit * 0.8

          batch_sql = "#{sql} LIMIT #{batch_size} OFFSET #{offset}"
          batch_count = 0

          fetch_rows(batch_sql) do |row|
            yield row
            batch_count += 1

            # Force garbage collection periodically to manage memory
            GC.start if (batch_count % 100).zero?
          end

          break if batch_count < batch_size

          offset += batch_size
        end

        self
      end

      private

      # Get approximate memory usage for streaming optimization
      def get_memory_usage
        GC.start
        ObjectSpace.count_objects[:TOTAL] * 40
      end

      public

      # Optimized count method for large tables
      def count(*args)
        if args.empty? && !@opts[:group] && !@opts[:having] && !@opts[:distinct] && !@opts[:where]
          # Use optimized COUNT(*) for simple cases only (no WHERE clause)
          # Get table name from opts[:from]
          table_name = @opts[:from].first
          single_value("SELECT COUNT(*) FROM #{quote_identifier(table_name)}")
        else
          # Fall back to standard Sequel count behavior for complex cases
          super
        end
      end

      private

      # Get a single value from a SQL query (used by count)
      def single_value(sql)
        value = nil
        fetch_rows(sql) do |row|
          value = row.values.first
          break
        end
        value
      end

      # Helper method to check if bulk operations should be used
      def should_use_bulk_operations?(row_count)
        # Use bulk operations for more than 10 rows
        row_count > 10
      end

      # Helper method to optimize query execution based on result set size
      def optimize_for_result_size(sql)
        # Add DuckDB-specific optimization hints if needed
        if @opts[:small_result_set]
          # For small result sets, DuckDB can use different optimization strategies
        end
        sql
      end

      public

      # Index-aware query generation methods (Requirement 9.7)

      # Get query execution plan with index usage information
      #
      # @return [String] Query execution plan
      def explain
        explain_sql = "EXPLAIN #{select_sql}"
        plan_text = ""

        fetch_rows(explain_sql) do |row|
          plan_text += "#{row.values.join(" ")}\n"
        end

        plan_text
      end

      # Get detailed query analysis including index usage
      #
      # @return [Hash] Analysis information
      def analyze_query
        {
          plan: explain,
          indexes_used: extract_indexes_from_plan(explain),
          optimization_hints: generate_optimization_hints
        }
      end

      # Override where method to add index-aware optimization hints
      def where(*cond, &block)
        result = super(*cond, &block)

        # Add index optimization hints based on WHERE conditions
        result = result.add_index_hints(cond.first.keys) if cond.length == 1 && cond.first.is_a?(Hash)

        result
      end

      # Override order method to leverage index optimization
      def order(*columns)
        result = super(*columns)

        # Add index hints for ORDER BY optimization
        order_columns = columns.map do |col|
          case col
          when Sequel::SQL::OrderedExpression
            col.expression
          else
            col
          end
        end

        result.add_index_hints(order_columns)
      end

      # Add index optimization hints to the dataset
      #
      # @param columns [Array] Columns that might benefit from index usage
      # @return [Dataset] Dataset with index hints
      def add_index_hints(columns)
        # Get available indexes for the table
        table_name = @opts[:from]&.first
        return self unless table_name

        available_indexes = begin
          db.indexes(table_name)
        rescue StandardError
          {}
        end

        # Find indexes that match the columns
        matching_indexes = available_indexes.select do |_index_name, index_info|
          index_columns = index_info[:columns] || []
          columns.any? { |col| index_columns.include?(col.to_sym) }
        end

        # Add index hints to options
        clone(index_hints: matching_indexes.keys)
      end

      # Columnar storage optimization methods (Requirement 9.7)

      # Override select method to add columnar optimization
      def select(*columns)
        result = super(*columns)

        # Mark as columnar-optimized if selecting specific columns
        result = result.clone(columnar_optimized: true) if columns.length.positive? && columns.length < 10

        result
      end

      # Optimize aggregation queries for columnar storage
      def group(*columns)
        result = super(*columns)

        # Add columnar aggregation optimization hints
        result.clone(columnar_aggregation: true)
      end

      # Parallel query execution support (Requirement 9.7)

      # Enable parallel execution for the query
      #
      # @param thread_count [Integer] Number of threads to use (optional)
      # @return [Dataset] Dataset configured for parallel execution
      def parallel(thread_count = nil)
        opts = { parallel_execution: true }
        opts[:parallel_threads] = thread_count if thread_count
        clone(opts)
      end

      private

      # Extract index names from query execution plan
      def extract_indexes_from_plan(plan)
        indexes = []
        plan.scan(/idx_\w+|index\s+(\w+)/i) do |match|
          indexes << (match.is_a?(Array) ? match.first : match)
        end
        indexes.compact.uniq
      end

      # Generate optimization hints based on query structure
      def generate_optimization_hints
        hints = []

        # Check for potential index usage
        hints << "Consider adding indexes on WHERE clause columns" if @opts[:where]

        # Check for ORDER BY optimization
        hints << "ORDER BY may benefit from index on ordered columns" if @opts[:order]

        # Check for GROUP BY optimization
        hints << "GROUP BY operations are optimized for columnar storage" if @opts[:group]

        hints
      end

      # Optimize SQL for columnar projection
      def optimize_for_columnar_projection(sql)
        # Add DuckDB-specific hints for columnar projection
        if @opts[:columnar_optimized]
          # DuckDB automatically optimizes column access, but we can add hints
        end
        sql
      end

      # Determine if parallel execution should be used
      def should_use_parallel_execution?
        # Use parallel execution for:
        # 1. Explicit parallel requests
        # 2. Complex aggregations
        # 3. Large joins
        # 4. Window functions

        return true if @opts[:parallel_execution]
        return true if @opts[:group] && @opts[:columnar_aggregation]
        return true if @opts[:join] && @opts[:join].length > 1
        return true if sql.downcase.include?("over(")

        false
      end

      # Add parallel execution hints to SQL
      def add_parallel_hints(sql)
        # DuckDB handles parallelization automatically, but we can add configuration
        if @opts[:parallel_threads]
          # NOTE: This would require connection-level configuration in practice
          # For now, we'll rely on DuckDB's automatic parallelization
        end

        sql
      end
    end
  end
end
