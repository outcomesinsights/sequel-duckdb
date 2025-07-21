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
            ::DuckDB::Database.new
          else
            # Create file-based database (will create file if it doesn't exist)
            ::DuckDB::Database.new(database_path)
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
    end

    # DatasetMethods module provides shared dataset functionality for DuckDB adapter
    # This module is included by the main Dataset class to provide SQL generation
    # and query execution capabilities.
    module DatasetMethods
      # Placeholder for dataset methods - will be implemented in later tasks
    end
  end
end