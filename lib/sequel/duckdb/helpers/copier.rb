# frozen_string_literal: true

module Sequel
  module DuckDB
    module Helpers
      # Builds COPY SQL statements for exporting DuckDB query results to files.
      #
      # FORMAT comes from the +format:+ option when given, otherwise from the
      # destination's file extension. A destination with neither (for example a
      # directory for a partitioned COPY) raises Sequel::Error.
      class Copier
        # Option values that DuckDB accepts as a bare keyword (FORMAT PARQUET).
        BARE_WORD = /\A[A-Za-z_][A-Za-z0-9_]*\z/

        def initialize(src, dst, options = Sequel::OPTS)
          @src = src
          @dst = dst
          @options = options
        end

        def to_sql
          "COPY (#{source}) TO #{string_literal(@dst)} #{options_str}"
        end

        def source
          if @src.is_a?(Sequel::Dataset)
            @src.sql
          else
            @src
          end
        end

        def options_str
          opts_str = { format: format }.merge(@options).map { |k, v| format_option(k, v) }.compact.join(", ").strip
          opts_str.empty? ? "" : "(#{opts_str})"
        end

        def format_option(key, value)
          return "PARTITION_BY (#{Array(value).map { |name| quoted_identifier(name) }.join(", ")})" if key.to_s == "partition_by"

          key = key.to_s.upcase
          case value
          when true then key
          when false then nil
          when Integer, Float then "#{key} #{value}"
          when BARE_WORD then "#{key} #{value.to_s.upcase}"
          else "#{key} #{string_literal(value)}"
          end
        end

        def format
          @options.fetch(:format) do
            File.extname(@dst.to_s).delete_prefix(".").then do |ext|
              raise Sequel::Error, "Cannot infer COPY format from #{@dst.to_s.inspect}: it has no file extension. Pass format:, for example format: :parquet" if ext.empty?

              ext
            end
          end
        end

        private

        def string_literal(value)
          "'#{value.to_s.gsub("'", "''")}'"
        end

        def quoted_identifier(name)
          "\"#{name.to_s.gsub('"', '""')}\""
        end
      end
    end
  end
end
