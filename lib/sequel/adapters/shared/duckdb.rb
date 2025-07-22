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
      # @return [::DuckDB::Database] DuckDB database connection
      # @raise [Sequel::DatabaseConnectionError] If connection fails
      def connect(server)
        opts = server_opts(server)
        database_path = opts[:database]

        begin
          if database_path == ":memory:" || database_path.nil?
            # Create in-memory database
            ::DuckDB::Database.open(":memory:")
          else
            # Create file-based database (will create file if it doesn't exist)
            ::DuckDB::Database.open(database_path)
          end
        rescue ::DuckDB::Error => e
          raise Sequel::DatabaseConnectionError, "Failed to connect to DuckDB database: #{e.message}"
        rescue StandardError => e
          raise Sequel::DatabaseConnectionError, "Unexpected error connecting to DuckDB: #{e.message}"
        end
      end

      # Disconnect from a DuckDB database connection
      #
      # @param conn [::DuckDB::Database] The database connection to close
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
      # @param conn [::DuckDB::Database] The database connection to check
      # @return [Boolean] true if connection is valid and open, false otherwise
      def valid_connection?(conn)
        return false unless conn

        begin
          # DuckDB doesn't have a closed? method, so we try a simple operation
          # to check if the connection is still valid
          conn.connect
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
      # @param opts [Hash] Options for execution
      # @return [Object] Result of execution
      def execute(sql, opts = {}, &block)
        synchronize(opts[:server]) do |conn|
          return execute_statement(conn, sql, opts, &block)
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
      # @param conn [::DuckDB::Database] Database connection
      # @param sql [String] SQL statement to execute
      # @param opts [Hash] Options for execution
      # @return [Object] Result of execution
      def execute_statement(conn, sql, opts, &block)
        begin
          db_conn = conn.connect
          result = db_conn.query(sql)

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
        ensure
          db_conn&.close if db_conn
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
        "'#{date}'"
      end

      def literal_datetime(datetime)
        "'#{datetime.strftime('%Y-%m-%d %H:%M:%S')}'"
      end

      def literal_time(time)
        # For Time objects with date information, format as datetime
        if time.respond_to?(:year) && time.year != 2000
          "'#{time.strftime('%Y-%m-%d %H:%M:%S')}'"
        else
          "'#{time.strftime('%H:%M:%S')}'"
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
    end
  end
end