# frozen_string_literal: true

# The duckdb_array extension writes Ruby arrays to DuckDB LIST and ARRAY columns and
# reads them back as Sequel::DuckDB::DuckDBArray objects. It follows Sequel's
# pg_array extension. It is opt-in: without it, the adapter's SQL and values do not
# change.
#
# Load the constructor globally:
#
#   Sequel.extension :duckdb_array
#
# Load it into a Database, which also converts results, schema types and model
# typecasting for that Database:
#
#   DB.extension :duckdb_array
#
# Write a list:
#
#   DB[:t].insert(l: Sequel.duckdb_array([1, 2]))  # INSERT ... VALUES ([1, 2])
#   Sequel.duckdb_array([[1], []])                 # [[1], []]
#
# A plain Ruby Array still means an IN list or a condition in Sequel. Only a
# DuckDBArray is written as a DuckDB list.
#
# With the extension loaded into a Database:
#
# * LIST and ARRAY result values (INTEGER[], INTEGER[3]) are DuckDBArray objects.
#   Only the top-level value is wrapped. A value nested in it stays as ruby-duckdb
#   returns it.
# * db.schema reports an INTEGER[] or INTEGER[3] column as :integer_array. A column
#   whose element type has no Sequel type, for example a STRUCT without the
#   duckdb_struct extension, is reported as :array.
# * A model typecasts an Array assigned to such a column to a DuckDBArray, and
#   typecasts each element with the element's type.

require "delegate"

module Sequel
  module DuckDB
    # An Array that Sequel writes as a DuckDB list literal.
    class DuckDBArray < DelegateClass(Array)
      # Element types that get a :<type>_array schema type.
      ELEMENT_TYPES = %i[string integer bigint float decimal boolean date datetime time interval blob uuid struct
                         map json].freeze

      # Element types whose typecast method has another name.
      ELEMENT_TYPECASTS = { bigint: :integer }.freeze

      # The element type and the dimensions of a LIST or ARRAY db_type:
      # INTEGER[], INTEGER[3], INTEGER[][], STRUCT(a INTEGER)[].
      DB_TYPE = /\A(.+?)(?:\[\d*\])+\z/m

      # Element db_types that the adapter maps to :string only as its fallback.
      COMPOSITE_DB_TYPE = /\A(?:STRUCT|MAP|UNION)\(/i

      module DatabaseMethods
        def self.extended(db)
          db.instance_exec do
            (ELEMENT_TYPES.map { |type| :"#{type}_array" } + [ :array ]).each do |type|
              @schema_type_classes[type] = DuckDBArray
            end
            if respond_to?(:add_conversion_proc)
              to_array = DuckDBArray.method(:new)
              add_conversion_proc(:list, to_array)
              add_conversion_proc(:array, to_array)
            end
          end
        end

        ELEMENT_TYPES.each do |type|
          method_name = :"typecast_value_#{type}_array"
          element_typecast = :"typecast_value_#{ELEMENT_TYPECASTS.fetch(type, type)}"
          define_method(method_name) { |value| typecast_value_duckdb_array(value, element_typecast) }
          private method_name
        end

        private

        def typecast_value_array(value)
          typecast_value_duckdb_array(value, nil)
        end

        def map_duckdb_type_to_sequel(duckdb_type)
          element = duckdb_type[DB_TYPE, 1]
          return super unless element

          type = map_duckdb_type_to_sequel(element)
          if ELEMENT_TYPES.include?(type) && !(type == :string && element.match?(COMPOSITE_DB_TYPE))
            :"#{type}_array"
          else
            :array
          end
        end

        def typecast_value_duckdb_array(value, element_typecast)
          raise Sequel::InvalidValue, "invalid value for array type: #{value.inspect}" unless value.is_a?(Array) || value.is_a?(DuckDBArray)

          element_typecast = nil unless element_typecast && respond_to?(element_typecast, true)
          DuckDBArray.new(typecast_duckdb_array_elements(value, element_typecast))
        end

        # Nested arrays become plain Arrays. A nil element stays nil.
        def typecast_duckdb_array_elements(array, element_typecast)
          array.map do |element|
            if element.is_a?(Array) || element.is_a?(DuckDBArray)
              typecast_duckdb_array_elements(element, element_typecast)
            elsif element.nil? || element_typecast.nil?
              element
            else
              send(element_typecast, element)
            end
          end
        end
      end

      def initialize(array)
        super(array.to_ary)
      end

      # [1, 2]. A nested Array gives a nested list, and every other element goes
      # through the dataset's literal_append.
      def sql_literal_append(ds, sql)
        append_list(ds, sql, __getobj__)
      end

      private

      def append_list(ds, sql, array)
        sql << "["
        array.each_with_index do |element, i|
          sql << ", " unless i.zero?
          element.is_a?(Array) ? append_list(ds, sql, element) : ds.literal_append(sql, element)
        end
        sql << "]"
      end
    end
  end

  module SQL
    module Builders
      # Wraps an Array, so that Sequel writes it as a DuckDB list.
      def duckdb_array(array)
        Sequel::DuckDB::DuckDBArray.new(array)
      end
    end
  end

  Database.register_extension(:duckdb_array, DuckDB::DuckDBArray::DatabaseMethods)
end
