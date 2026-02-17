# frozen_string_literal: true

module Sequel
  module DuckDB
    module Helpers
      # Builds COPY SQL statements for exporting DuckDB query results to files.
      class Copier
        def initialize(src, dst, options = Sequel::OPTS)
          @src = src
          @dst = dst
          @options = options
        end

        def to_sql
          "COPY (#{source}) TO '#{@dst}' #{options_str}"
        end

        def source
          if @src.is_a?(Sequel::Dataset)
            @src.sql
          else
            @src
          end
        end

        def destination
          Pathname.new(@dst).expand_path.to_s
        end

        def options_str
          opts_str = { format: format }.merge(@options).map { |k, v| format_option(k, v) }.compact.join(", ").strip
          opts_str.empty? ? "" : "(#{opts_str})"
        end

        def format_option(key, value)
          key = key.to_s.upcase
          case value
          when true then key
          when false then nil
          else "#{key} #{value.to_s.upcase}"
          end
        end

        def format
          File.extname(@dst).delete_prefix(".")
        end
      end
    end
  end
end
