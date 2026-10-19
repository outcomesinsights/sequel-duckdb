# frozen_string_literal: true

# The duckdb_json extension writes Ruby hashes and arrays to DuckDB JSON columns and
# reads them back as Sequel::DuckDB::DuckDBJSONHash and DuckDBJSONArray objects. It
# follows Sequel's pg_json extension. It is opt-in: without it, the adapter's SQL and
# values do not change.
#
# Load the constructor globally:
#
#   Sequel.extension :duckdb_json
#
# Load it into a Database, which also converts results, schema types and model
# typecasting for that Database:
#
#   DB.extension :duckdb_json
#
# Write a JSON value:
#
#   DB[:t].insert(j: Sequel.duckdb_json("a" => 1))  # INSERT ... VALUES ('{"a":1}'::JSON)
#   Sequel.duckdb_json([1, 2])                      # '[1,2]'::JSON
#
# A plain Ruby Hash or Array still means a condition or an IN list in Sequel. Only a
# DuckDBJSONHash or DuckDBJSONArray is written as JSON.
#
# With the extension loaded into a Database:
#
# * JSON result values are parsed. A JSON object becomes a DuckDBJSONHash with String
#   keys, and a JSON array becomes a DuckDBJSONArray. A JSON string, number, boolean
#   or null becomes the plain Ruby value. Only the top-level value is wrapped. A
#   VARCHAR column is not parsed, even when it holds JSON text.
# * db.schema reports a JSON column as :json. With the duckdb_array extension also
#   loaded, it reports a JSON[] column as :json_array.
# * A model typecasts a Hash or an Array assigned to a JSON column to the wrapper. It
#   parses a String as JSON text.

require "delegate"
require "json"

module Sequel
  module DuckDB
    # Included in DuckDBJSONHash and DuckDBJSONArray, so that one check finds both.
    module DuckDBJSONObject
      # The wrapper for a parsed JSON value. A JSON string, number, boolean or null
      # stays the plain Ruby value.
      def self.wrap(value)
        case value
        when Hash then DuckDBJSONHash.new(value)
        when Array then DuckDBJSONArray.new(value)
        else value
        end
      end

      # Not Sequel.parse_json: it passes create_additions, which json 3.0 rejects.
      # JSON.parse creates no additions by default.
      def self.parse(json)
        wrap(::JSON.parse(json))
      end

      module DatabaseMethods
        def self.extended(db)
          db.instance_exec do
            @schema_type_classes[:json] = [ DuckDBJSONObject ]
            add_conversion_proc(:json, DuckDBJSONObject.method(:parse)) if respond_to?(:add_conversion_proc)
          end
        end

        private

        def map_duckdb_type_to_sequel(duckdb_type)
          duckdb_type.casecmp?("JSON") ? :json : super
        end

        def typecast_value_json(value)
          case value
          when DuckDBJSONObject then value
          when Hash, Array then DuckDBJSONObject.wrap(value)
          when String then parse_json_for_typecast(value)
          else raise Sequel::InvalidValue, "invalid value for json type: #{value.inspect}"
          end
        end

        def parse_json_for_typecast(json)
          DuckDBJSONObject.parse(json)
        rescue ::JSON::ParserError => e
          raise Sequel.convert_exception_class(e, Sequel::InvalidValue)
        end
      end

      # '{"a":1}'::JSON. The JSON text is a string literal, cast to JSON.
      def sql_literal_append(ds, sql)
        ds.literal_append(sql, Sequel.object_to_json(__getobj__))
        sql << "::JSON"
      end
    end

    # A Hash that Sequel writes as a DuckDB JSON object.
    class DuckDBJSONHash < DelegateClass(Hash)
      include DuckDBJSONObject

      def initialize(hash)
        super(hash.to_hash)
      end
    end

    # An Array that Sequel writes as a DuckDB JSON array.
    class DuckDBJSONArray < DelegateClass(Array)
      include DuckDBJSONObject

      def initialize(array)
        super(array.to_ary)
      end
    end
  end

  module SQL
    module Builders
      # Wraps a Hash or an Array, so that Sequel writes it as DuckDB JSON. A wrapper
      # is returned as it is.
      def duckdb_json(value)
        case value
        when Sequel::DuckDB::DuckDBJSONObject then value
        when Hash, Array then Sequel::DuckDB::DuckDBJSONObject.wrap(value)
        else raise Sequel::Error, "Sequel.duckdb_json needs a Hash or an Array: #{value.inspect}"
        end
      end
    end
  end

  Database.register_extension(:duckdb_json, DuckDB::DuckDBJSONObject::DatabaseMethods)
end
