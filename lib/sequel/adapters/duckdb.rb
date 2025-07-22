# frozen_string_literal: true

require "sequel"
require_relative "shared/duckdb"

module Sequel
  module DuckDB
    # Database class for DuckDB adapter
    #
    # This class extends Sequel::Database to provide DuckDB-specific functionality.
    # It includes DatabaseMethods from the shared module for connection management
    # and other database operations.
    class Database < Sequel::Database
      include Sequel::DuckDB::DatabaseMethods

      # Set the adapter scheme for DuckDB
      set_adapter_scheme :duckdb

      # Return the default dataset class for this database
      #
      # @return [Class] The Dataset class to use for this database
      def dataset_class_default
        Dataset
      end
    end

    # Dataset class for DuckDB adapter
    #
    # This class extends Sequel::Dataset to provide DuckDB-specific SQL generation
    # and query execution functionality. It includes DatasetMethods from the shared
    # module for SQL generation and other dataset operations.
    class Dataset < Sequel::Dataset
      include Sequel::DuckDB::DatasetMethods
    end
  end

  # Register the DuckDB adapter with Sequel
  # This allows Sequel.connect("duckdb://...") to work
end