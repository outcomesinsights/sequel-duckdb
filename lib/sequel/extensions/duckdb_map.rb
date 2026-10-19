# frozen_string_literal: true

# The duckdb_map extension writes Ruby hashes to DuckDB MAP columns and reads them
# back as Sequel::DuckDB::DuckDBMap objects. Sequel's pg_hstore extension is the
# PostgreSQL counterpart, but a DuckDB MAP has typed keys and values, not only
# strings. It is opt-in: without it, the adapter's SQL and values do not change.
#
# Load the constructor globally:
#
#   Sequel.extension :duckdb_map
#
# Load it into a Database, which also converts results, schema types and model
# typecasting for that Database:
#
#   DB.extension :duckdb_map
#
# Write a map:
#
#   DB[:t].insert(m: Sequel.duckdb_map("k" => 1))  # INSERT ... VALUES (MAP {'k': 1})
#   Sequel.duckdb_map({})                          # MAP {}
#
# A plain Ruby Hash still means a condition in Sequel. Only a DuckDBMap is written as
# a DuckDB map. A struct or list inside a map must itself be a wrapper.
#
# With the extension loaded into a Database:
#
# * MAP result values are DuckDBMap objects. The keys keep their Ruby type: a
#   MAP(VARCHAR, INTEGER) has String keys. Only the top-level value is wrapped. A
#   value nested in it stays as ruby-duckdb returns it.
# * db.schema reports a MAP column, for example MAP(VARCHAR, INTEGER), as :map.
# * A model typecasts a Hash assigned to a MAP column to a DuckDBMap.

require "delegate"

module Sequel
  module DuckDB
    # A Hash that Sequel writes as a DuckDB map literal.
    class DuckDBMap < DelegateClass(Hash)
      # A MAP db_type. MAP(VARCHAR, INTEGER)[] is a list, so it does not match.
      DB_TYPE = /\AMAP\(.*\)\z/im

      module DatabaseMethods
        def self.extended(db)
          db.instance_exec do
            @schema_type_classes[:map] = DuckDBMap
            add_conversion_proc(:map, DuckDBMap.method(:new)) if respond_to?(:add_conversion_proc)
          end
        end

        private

        def map_duckdb_type_to_sequel(duckdb_type)
          duckdb_type.match?(DB_TYPE) ? :map : super
        end

        def typecast_value_map(value)
          raise Sequel::InvalidValue, "invalid value for map type: #{value.inspect}" unless value.is_a?(Hash) || value.is_a?(DuckDBMap)

          DuckDBMap.new(value)
        end
      end

      def initialize(hash)
        super(hash.to_hash)
      end

      # MAP {'k': 1}. Each key and each value goes through the dataset's
      # literal_append.
      def sql_literal_append(ds, sql)
        sql << "MAP {"
        each_with_index do |(key, value), i|
          sql << ", " unless i.zero?
          ds.literal_append(sql, key)
          sql << ": "
          ds.literal_append(sql, value)
        end
        sql << "}"
      end
    end
  end

  module SQL
    module Builders
      # Wraps a Hash, so that Sequel writes it as a DuckDB map. The keys keep their
      # type.
      def duckdb_map(hash)
        Sequel::DuckDB::DuckDBMap.new(hash)
      end
    end
  end

  Database.register_extension(:duckdb_map, DuckDB::DuckDBMap::DatabaseMethods)
end
