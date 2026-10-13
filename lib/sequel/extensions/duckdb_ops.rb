# frozen_string_literal: true

# The duckdb_ops extension adds DuckDB-specific expression builders to Sequel.
# It is opt-in: without it, Sequel generates the same SQL as before.
#
# Load it globally:
#
#   Sequel.extension :duckdb_ops
#
# or through a Database (which also loads it globally):
#
#   DB.extension :duckdb_ops
#
# Builders:
#
#   Sequel.try_cast(:x, :DOUBLE)  # TRY_CAST("x" AS DOUBLE)
#   Sequel.try_cast(:x, Float)    # TRY_CAST("x" AS double precision)
#
# TRY_CAST returns NULL when the cast fails, where CAST raises.

module Sequel
  module DuckDB
    # TRY_CAST(expr AS type). The type is handled exactly as in Sequel.cast:
    # a Ruby class maps through the database's cast_type_literal, and a
    # Symbol or String passes through unchanged.
    class TryCast < Sequel::SQL::Cast
      def to_s_append(ds, sql)
        sql << "TRY_CAST("
        ds.literal_append(sql, expr)
        sql << " AS " << ds.db.cast_type_literal(type).to_s << ")"
      end
    end
  end

  module SQL
    module Builders
      # Return a DuckDB TRY_CAST of expr to type.
      def try_cast(expr, type)
        Sequel::DuckDB::TryCast.new(expr, type)
      end
    end
  end

  Database.register_extension(:duckdb_ops) { }
end
