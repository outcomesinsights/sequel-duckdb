# frozen_string_literal: true

# The duckdb_struct extension writes Ruby hashes to DuckDB STRUCT columns and reads
# them back as Sequel::DuckDB::DuckDBStruct objects. Sequel's pg_row extension is the
# PostgreSQL counterpart. PostgreSQL row types are named and DuckDB structs are not,
# so no type registration is needed. It is opt-in: without it, the adapter's SQL and
# values do not change.
#
# Load the constructor globally:
#
#   Sequel.extension :duckdb_struct
#
# Load it into a Database, which also converts results, schema types and model
# typecasting for that Database:
#
#   DB.extension :duckdb_struct
#
# Write a struct:
#
#   DB[:t].insert(s: Sequel.duckdb_struct(a: 1, b: "x"))  # INSERT ... VALUES ({'a': 1, 'b': 'x'})
#
# A plain Ruby Hash still means a condition in Sequel. Only a DuckDBStruct is written
# as a DuckDB struct. A struct or list inside a struct must itself be a wrapper.
#
# With the extension loaded into a Database:
#
# * STRUCT result values are DuckDBStruct objects with Symbol keys. Only the
#   top-level value is wrapped. A value nested in it stays as ruby-duckdb returns it.
# * db.schema reports a STRUCT column, for example STRUCT(a INTEGER), as :struct.
# * A model typecasts a Hash assigned to a STRUCT column to a DuckDBStruct.

require "delegate"

module Sequel
  module DuckDB
    # A Hash with Symbol keys that Sequel writes as a DuckDB struct literal.
    class DuckDBStruct < DelegateClass(Hash)
      # A STRUCT db_type. STRUCT(a INTEGER)[] is a list, so it does not match.
      DB_TYPE = /\ASTRUCT\(.*\)\z/im

      module DatabaseMethods
        def self.extended(db)
          db.instance_exec do
            @schema_type_classes[:struct] = DuckDBStruct
            add_conversion_proc(:struct, DuckDBStruct.method(:new)) if respond_to?(:add_conversion_proc)
          end
        end

        private

        def map_duckdb_type_to_sequel(duckdb_type)
          duckdb_type.match?(DB_TYPE) ? :struct : super
        end

        def typecast_value_struct(value)
          raise Sequel::InvalidValue, "invalid value for struct type: #{value.inspect}" unless value.is_a?(Hash) || value.is_a?(DuckDBStruct)

          DuckDBStruct.new(value)
        end
      end

      def initialize(hash)
        super(hash.to_hash.transform_keys(&:to_sym))
      end

      # {'a': 1, 'b': 'x'}. Each key is a string literal, and each value goes through
      # the dataset's literal_append.
      def sql_literal_append(ds, sql)
        sql << "{"
        each_with_index do |(key, value), i|
          sql << ", " unless i.zero?
          ds.literal_append(sql, key.to_s)
          sql << ": "
          ds.literal_append(sql, value)
        end
        sql << "}"
      end
    end
  end

  module SQL
    module Builders
      # Wraps a Hash, so that Sequel writes it as a DuckDB struct. The keys become
      # Symbols.
      def duckdb_struct(hash)
        Sequel::DuckDB::DuckDBStruct.new(hash)
      end
    end
  end

  Database.register_extension(:duckdb_struct, DuckDB::DuckDBStruct::DatabaseMethods)
end
