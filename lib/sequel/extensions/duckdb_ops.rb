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
#   Sequel.function(:f, :x, Sequel.duckdb_named(a: 1, b: true))
#   # f("x", a = 1, b = TRUE)
#
#   Sequel.duckdb_read_parquet("/d/**/*.parquet", hive_partitioning: true)
#   # read_parquet(['/d/**/*.parquet'], hive_partitioning = TRUE)
#
#   Sequel.duckdb_lambda(:x) { |x| x + 1 }  # lambda "x": ("x" + 1)
#   Sequel.duckdb_lambda(:x, Sequel[:x] + 1) # the same, body form
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

    # Literalizes the values of DuckDB named parameters.
    module OpsLiteral
      # Array -> list literal [v1, v2]. Hash -> struct literal {'key': value}.
      # Anything else goes through the dataset's literal_append.
      #
      # In a struct, a Symbol value is a bare type name and a String value is a
      # string literal, so hive_types: { year: :BIGINT } gives {'year': BIGINT}
      # and columns: { a: "INTEGER" } gives {'a': 'INTEGER'}. DuckDB 1.5 accepts
      # both forms for hive_types and for read_json's columns (checked on a real
      # database: {'year': BIGINT} and {'year': 'BIGINT'} both type the column).
      def self.append(ds, sql, value)
        case value
        when Array
          sql << "["
          value.each_with_index do |v, i|
            sql << ", " unless i.zero?
            append(ds, sql, v)
          end
          sql << "]"
        when Hash
          sql << "{"
          value.each_with_index do |(k, v), i|
            sql << ", " unless i.zero?
            ds.literal_append(sql, k.to_s)
            sql << ": "
            v.is_a?(Symbol) ? sql << v.to_s : append(ds, sql, v)
          end
          sql << "}"
        else
          ds.literal_append(sql, value)
        end
      end
    end

    # A DuckDB list literal, ['a', 'b'].
    class ListLiteral < Sequel::SQL::Expression
      attr_reader :values

      def initialize(values)
        super()
        @values = values.dup.freeze
        freeze
      end

      def to_s_append(ds, sql)
        OpsLiteral.append(ds, sql, values)
      end
    end

    # Named parameters, name1 = value1, name2 = value2, in the given order.
    # Use it as the last argument of Sequel.function.
    class NamedArguments < Sequel::SQL::Expression
      NAME = /\A[a-z_][a-z0-9_]*\z/

      attr_reader :args

      def initialize(args)
        super()
        raise ArgumentError, "no DuckDB named parameters given" if args.empty?

        args.each_key do |name|
          raise ArgumentError, "invalid DuckDB parameter name: #{name.inspect}" unless NAME.match?(name.to_s)
        end
        @args = args.dup.freeze
        freeze
      end

      def to_s_append(ds, sql)
        args.each_with_index do |(name, value), i|
          sql << ", " unless i.zero?
          sql << name.to_s << " = "
          OpsLiteral.append(ds, sql, value)
        end
      end
    end

    # A DuckDB lambda expression, lambda "acc", "x": body. The parameter names
    # are quoted identifiers, so they match how Sequel writes the references in
    # the body. It emits the lambda keyword syntax, never the deprecated arrow.
    class Lambda < Sequel::SQL::Expression
      attr_reader :params, :body

      def initialize(params, body)
        super()
        raise ArgumentError, "a DuckDB lambda needs at least one parameter" if params.empty?

        params.each do |name|
          unless (name.is_a?(Symbol) || name.is_a?(String)) && !name.empty?
            raise ArgumentError, "invalid DuckDB lambda parameter name: #{name.inspect}"
          end
        end
        dup = params.map(&:to_s).tally.select { |_, n| n > 1 }.keys
        raise ArgumentError, "duplicate DuckDB lambda parameter names: #{dup.inspect}" unless dup.empty?

        @params = params.dup.freeze
        @body = body
        freeze
      end

      def to_s_append(ds, sql)
        sql << "lambda "
        params.each_with_index do |name, i|
          sql << ", " unless i.zero?
          ds.quote_identifier_append(sql, name)
        end
        sql << ": "
        ds.literal_append(sql, body)
      end
    end

    # Sequel.duckdb_lambda: the block form takes parameter names and yields
    # one identifier per parameter; the body form takes a name or an Array of
    # names, then the body.
    def self.lambda_expression(args, block)
      if block
        if args.any? { |a| !(a.is_a?(Symbol) || a.is_a?(String)) }
          raise ArgumentError, "Sequel.duckdb_lambda takes parameter names with a block, not a body argument: #{args.inspect}"
        end

        Lambda.new(args, block.call(*args.map { |name| Sequel::SQL::Identifier.new(name) }))
      else
        raise ArgumentError, "Sequel.duckdb_lambda without a block takes (params, body), got #{args.size} arguments" unless args.size == 2

        Lambda.new(Array(args[0]), args[1])
      end
    end

    # name(['path', ...], named = value, ...) as a Sequel::SQL::Function.
    def self.read_function(name, paths, named)
      paths = Array(paths)
      raise ArgumentError, "paths must be one or more Strings: #{paths.inspect}" if paths.empty? || !paths.all?(String)

      args = [ ListLiteral.new(paths) ]
      args << NamedArguments.new(named) unless named.empty?
      Sequel::SQL::Function.new(name, *args)
    end
  end

  module SQL
    module Builders
      # Return a DuckDB TRY_CAST of expr to type.
      def try_cast(expr, type)
        Sequel::DuckDB::TryCast.new(expr, type)
      end

      # Return DuckDB named parameters, for the last argument of Sequel.function.
      def duckdb_named(**args)
        Sequel::DuckDB::NamedArguments.new(args)
      end

      # Return a DuckDB lambda expression, lambda "x": body. Two forms:
      #
      #   Sequel.duckdb_lambda(:acc, :x) { |acc, x| acc + x }
      #   Sequel.duckdb_lambda([:acc, :x], Sequel[:acc] + :x)
      def duckdb_lambda(*args, &block)
        Sequel::DuckDB.lambda_expression(args, block)
      end

      # read_parquet(paths, named...). paths is a String or an Array of Strings.
      def duckdb_read_parquet(paths, **named)
        Sequel::DuckDB.read_function(:read_parquet, paths, named)
      end

      # read_csv(paths, named...). paths is a String or an Array of Strings.
      def duckdb_read_csv(paths, **named)
        Sequel::DuckDB.read_function(:read_csv, paths, named)
      end

      # read_json(paths, named...). paths is a String or an Array of Strings.
      def duckdb_read_json(paths, **named)
        Sequel::DuckDB.read_function(:read_json, paths, named)
      end
    end
  end

  Database.register_extension(:duckdb_ops) { }
end
