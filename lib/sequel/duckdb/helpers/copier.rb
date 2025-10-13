module Sequel::DuckDB::Helpers
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
      opts_str = { format: format }
                 .merge(@options)
                 .map do |k, v|
        k = k.to_s.upcase
        if v.is_a?(TrueClass) || v.is_a?(FalseClass)
          v ? k : nil
        else
          "#{k} #{v.to_s.upcase}"
        end
      end
        .compact
        .join(", ")
        .strip
      opts_str.empty? ? "" : "(#{opts_str})"
    end

    def format
      File.extname(@dst).delete_prefix(".")
    end
  end
end
