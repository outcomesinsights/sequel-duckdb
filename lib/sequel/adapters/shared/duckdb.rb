# frozen_string_literal: true

require "duckdb"

module Sequel
  module DuckDB
    # DatabaseMethods module provides shared database functionality for DuckDB adapter
    # This module is included by the main Database class to provide connection management,
    # schema introspection, and SQL execution capabilities.
    module DatabaseMethods
      # Connect to a DuckDB database
      #
      # @param server [Hash] Server configuration options
      # @return [::DuckDB::Connection] DuckDB database connection
      # @raise [Sequel::DatabaseConnectionError] If connection fails
      def connect(server)
        opts = server_opts(server)
        database_path = opts[:database]

        begin
          if database_path == ":memory:" || database_path.nil?
            # Create in-memory database and return connection
            db = ::DuckDB::Database.open(":memory:")
            db.connect
          else
            # Fix URI parsing issue - add leading slash if missing for absolute paths
            if database_path.match?(/^[a-zA-Z]/) && !database_path.start_with?(':')
              database_path = '/' + database_path
            end

            # Create file-based database (will create file if it doesn't exist) and return connection
            db = ::DuckDB::Database.open(database_path)
            db.connect
          end
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
        else
          params = opts[:params] || []
        end

        synchronize(opts[:server]) do |conn|
          return execute_statement(conn, sql, params, opts, &block)
        end
      end

      # Execute INSERT statement
      #
      # @param sql [String] INSERT SQL statement
      # @param opts [Hash] Options for execution
      # @return [Object] Result of execution
      def execute_insert(sql, opts = {})
        result = execute(sql, opts)
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
        execute(sql, opts)
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
      def database_exception_sqlstate(exception, opts)
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

      # Schema introspection methods

      # Parse table list from database
      #
      # @param opts [Hash] Options for table parsing
      # @return [Array<Symbol>] Array of table names as symbols
      def schema_parse_tables(opts = {})
        schema_name = opts[:schema] || 'main'

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
        schema_name = opts[:schema] || 'main'

        # First check if table exists
        unless table_exists?(table_name, opts)
          raise Sequel::DatabaseError, "Table '#{table_name}' does not exist"
        end

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
          allow_null = row[:is_nullable] == 'YES'

          # Parse default value
          default_value = parse_default_value(row[:column_default])

          column_info = {
            type: sequel_type,
            db_type: row[:data_type],
            allow_null: allow_null,
            default: default_value,
            primary_key: false  # Will be updated below
          }

          # Add size information for string types
          if row[:character_maximum_length]
            column_info[:max_length] = row[:character_maximum_length]
          end

          # Add precision/scale for numeric types
          if row[:numeric_precision]
            column_info[:precision] = row[:numeric_precision]
          end
          if row[:numeric_scale]
            column_info[:scale] = row[:numeric_scale]
          end

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
        schema_name = opts[:schema] || 'main'

        # First check if table exists
        unless table_exists?(table_name, opts)
          raise Sequel::DatabaseError, "Table '#{table_name}' does not exist"
        end

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

      # Check if table exists
      #
      # @param table_name [Symbol, String] Name of the table
      # @param opts [Hash] Options
      # @return [Boolean] true if table exists
      def table_exists?(table_name, opts = {})
        schema_name = opts[:schema] || 'main'

        sql = "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ? LIMIT 1"

        result = nil
        execute(sql, [schema_name, table_name.to_s]) do |row|
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
      # @param table_name [Symbol, String] Name of the table
      # @param opts [Hash] Options
      # @return [Array<Array>] Schema information
      def schema(table_name, opts = {})
        schema_parse_table(table_name, opts)
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
        when 'INTEGER', 'INT', 'INT4'
          :integer
        when 'BIGINT', 'INT8'
          :bigint
        when 'SMALLINT', 'INT2'
          :integer
        when 'TINYINT', 'INT1'
          :integer
        when 'REAL', 'FLOAT4'
          :float
        when 'DOUBLE', 'FLOAT8'
          :float
        when 'DECIMAL', 'NUMERIC'
          :decimal
        when 'VARCHAR', 'TEXT', 'STRING'
          :string
        when 'BOOLEAN', 'BOOL'
          :boolean
        when 'DATE'
          :date
        when 'TIMESTAMP', 'DATETIME'
          :datetime
        when 'TIME'
          :time
        when 'BLOB', 'BYTEA'
          :blob
        when 'UUID'
          :uuid
        else
          :string  # Default fallback
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
          $1 == 't'
        when /^'(.+)'$/
          $1  # String literal
        when /^\d+$/
          default_str.to_i  # Integer literal
        when /^\d+\.\d+$/
          default_str.to_f  # Float literal
        when 'NULL'
          nil
        else
          default_str  # Return as-is for complex expressions
        end
      end

      # Update primary key information in column schema
      #
      # @param table_name [Symbol, String] Table name
      # @param columns [Array] Array of column information
      # @param opts [Hash] Options
      def update_primary_key_info(table_name, columns, opts = {})
        schema_name = opts[:schema] || 'main'

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
            column_info[:allow_null] = false  # Primary keys cannot be null
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
        cleaned = expressions_str.gsub(/^\[|\]$/, '').gsub(/['"]/, '')
        columns = cleaned.split(',').map(&:strip).map(&:to_sym)

        columns
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
      def supports_transaction_isolation_level?(level)
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
      def commit_transaction(conn, opts = {})
        conn.query("COMMIT")
      end

      # Rollback the current transaction manually
      # Sequel calls this with (conn, opts) arguments
      #
      # @param conn [::DuckDB::Connection] Database connection
      # @param opts [Hash] Options
      # @return [void]
      def rollback_transaction(conn, opts = {})
        conn.query("ROLLBACK")
      end

      # Override Sequel's transaction method to support advanced features
      def transaction(opts = {})
        # Handle savepoint transactions (nested transactions)
        if opts[:savepoint] && supports_savepoints?
          return savepoint_transaction(opts) { yield }
        end

        # Handle isolation level setting
        if opts[:isolation] && supports_transaction_isolation_level?(opts[:isolation])
          return isolation_transaction(opts) { yield }
        end

        # Fall back to standard Sequel transaction handling
        super { yield }
      end

      private

      # Handle savepoint-based nested transactions
      #
      # @param opts [Hash] Transaction options
      # @return [Object] Result of the transaction block
      def savepoint_transaction(opts = {})
        # Generate a unique savepoint name
        savepoint_name = "sp_#{Time.now.to_f.to_s.gsub('.', '_')}"

        synchronize(opts[:server]) do |conn|
          begin
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
      end

      # Handle transactions with specific isolation levels
      #
      # @param opts [Hash] Transaction options including :isolation
      # @return [Object] Result of the transaction block
      def isolation_transaction(opts = {})
        synchronize(opts[:server]) do |conn|
          begin
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
      end

      # DuckDB-specific schema generation methods

      # Generate SQL for primary key column
      #
      # @param column [Symbol] Column name
      # @param opts [Hash] Column options
      # @return [String] SQL for primary key column
      def primary_key_column_sql(column, opts)
        # DuckDB doesn't support AUTOINCREMENT, so we just use INTEGER PRIMARY KEY
        sql = "#{quote_identifier(column)} INTEGER PRIMARY KEY"
        # Don't add AUTOINCREMENT for DuckDB
        sql
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
      def auto_increment_column_sql(column, opts)
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
            "DECIMAL(#{Array(opts[:size]).join(',')})"
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
      def execute_statement(conn, sql, params = [], opts = {}, &block)
        begin
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
          raise Sequel::DatabaseError, "DuckDB error: #{e.message}"
        end
      end
    end

    # DatasetMethods module provides shared dataset functionality for DuckDB adapter
    # This module is included by the main Dataset class to provide SQL generation
    # and query execution capabilities.
    module DatasetMethods
      # Generate SELECT SQL statement
      #
      # @return [String] The SELECT SQL statement
      def select_sql
        return @opts[:sql] if @opts[:sql]

        sql = "SELECT ".dup

        # Add column selection
        if @opts[:select]
          sql << select_columns_sql
        else
          sql << "*"
        end

        # Add FROM clause
        sql << " FROM #{table_name_sql}"

        # Add JOIN clauses
        select_join_sql(sql) if @opts[:join]

        # Add WHERE clause
        select_where_sql(sql) if @opts[:where]

        # Add GROUP BY clause
        select_group_sql(sql) if @opts[:group]

        # Add HAVING clause
        select_having_sql(sql) if @opts[:having]

        # Add ORDER BY clause
        select_order_sql(sql) if @opts[:order]

        # Add LIMIT and OFFSET clauses
        select_limit_sql(sql) if @opts[:limit]

        sql
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

          return "INSERT INTO #{table_name_sql} #{column_list} VALUES #{values_lists.join(', ')}"
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

      def supports_returning?
        false
      end

      def supports_select_all_and_offset?
        true
      end

      def quote_identifiers_default
        false
      end

      # Execute SQL and yield each row to the block
      #
      # @param sql [String] SQL statement to execute
      # @param &block [Proc] Block to process each row
      # @return [Enumerator] If no block given, returns enumerator
      def fetch_rows(sql, &block)
        db.execute(sql, &block)
      end

      # Override identifier quoting to avoid uppercase conversion
      def quote_identifier_append(sql, name)
        name_str = name.to_s
        # Special case for * (used in count(*), etc.)
        if name_str == "*"
          sql << "*"
        # Quote reserved words and identifiers with special characters
        elsif name_str.match?(/\A[a-zA-Z_][a-zA-Z0-9_]*\z/) && !reserved_word?(name_str)
          sql << name_str
        else
          sql << "\"#{name_str}\""
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

      # Generate SELECT columns SQL
      def select_columns_sql
        if @opts[:select]
          @opts[:select].map { |col| literal(col) }.join(", ")
        else
          "*"
        end
      end

      # Add LIMIT and OFFSET clauses to SQL
      def select_limit_sql(sql)
        if limit = @opts[:limit]
          sql << " LIMIT #{literal(limit)}"
          if offset = @opts[:offset]
            sql << " OFFSET #{literal(offset)}"
          end
        end
      end

      # Override literal methods for DuckDB-specific formatting
      def literal_string_append(sql, s)
        sql << "'" << s.gsub("'", "''") << "'"
      end

      def literal_date(date)
        "'#{date.strftime('%Y-%m-%d')}'"
      end

      def literal_datetime(datetime)
        "'#{datetime.strftime('%Y-%m-%d %H:%M:%S')}'"
      end

      def literal_time(time)
        "'#{time.strftime('%H:%M:%S')}'"
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

      # Helper method for datetime literal appending
      def literal_datetime_append(sql, datetime)
        sql << "'#{datetime.strftime('%Y-%m-%d %H:%M:%S')}'"
      end

      # Helper method for binary data literal appending
      def literal_blob_append(sql, blob)
        # DuckDB expects BLOB literals in hex format without \x prefix
        sql << "'#{blob.unpack1('H*')}'"
      end

      # Literal conversion for binary data (BLOB type)
      def literal_blob(blob)
        "'#{blob.unpack1('H*')}'"
      end
    end
  end
end