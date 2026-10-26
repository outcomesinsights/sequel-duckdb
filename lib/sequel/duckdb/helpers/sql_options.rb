# frozen_string_literal: true

module Sequel
  module DuckDB
    module Helpers
      # Renders the option list of a DuckDB statement, as in COPY ... (FORMAT PARQUET)
      # and ATTACH ... (READ_ONLY, TYPE SQLITE).
      module SqlOptions
        # Option values that DuckDB accepts as a bare keyword (FORMAT PARQUET).
        BARE_WORD = /\A[A-Za-z_][A-Za-z0-9_]*\z/

        module_function

        # "(KEY value, ...)", or "" when no option renders. A true value renders the key
        # alone, and a false value leaves the option out.
        def list(options)
          rendered = options.map { |k, v| option(k, v) }.compact
          rendered.empty? ? "" : "(#{rendered.join(", ")})"
        end

        def option(key, value)
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
