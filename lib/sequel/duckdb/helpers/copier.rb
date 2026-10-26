# frozen_string_literal: true

require_relative "sql_options"

module Sequel
  module DuckDB
    module Helpers
      # Builds COPY SQL statements for exporting DuckDB query results to files.
      #
      # FORMAT comes from the +format:+ option when given, otherwise from the
      # destination's file extension. A destination with neither (for example a
      # directory for a partitioned COPY) raises Sequel::Error.
      class Copier
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
          SqlOptions.list({ format: format }.merge(@options))
        end

        def format_option(key, value)
          SqlOptions.option(key, value)
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
          SqlOptions.string_literal(value)
        end
      end
    end
  end
end
