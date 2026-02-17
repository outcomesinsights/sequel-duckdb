# frozen_string_literal: true

module Sequel
  module DuckDB
    module Helpers
      # Helper class for generating SQL to read data from file paths using DuckDB's read functions.
      #
      # Pathifier converts file paths into appropriate DuckDB read_* function calls based on
      # file extensions. It supports reading parquet, CSV, and JSON files, and can handle
      # single or multiple files with glob patterns.
      #
      # @example Reading a single parquet file
      #   pathifier = Pathifier.new("/data/users.parquet")
      #   pathifier.to_sql
      #   # => Sequel.function(:read_parquet, "['/data/users.parquet']")
      #
      # @example Reading multiple CSV files
      #   pathifier = Pathifier.new(["/data/2023.csv", "/data/2024.csv"])
      #   pathifier.to_sql
      #   # => Sequel.function(:read_csv, "['/data/2023.csv','/data/2024.csv']")
      #
      # @example Using glob patterns
      #   pathifier = Pathifier.new("/data/*.parquet")
      #   pathifier.to_sql
      #   # => Sequel.function(:read_parquet, "['/data/*.parquet']")
      #
      # @example Overriding format detection
      #   pathifier = Pathifier.new("/data/file.txt", using: :csv)
      #   pathifier.to_sql
      #   # => Sequel.function(:read_csv, "['/data/file.txt']")
      class Pathifier
        # Initialize a new Pathifier instance.
        #
        # @param paths [String, Array<String>] Single path or array of paths to files
        # @param options [Hash] Options hash
        # @option options [Symbol, String] :using Force a specific format (:parquet, :csv, or :json)
        #   instead of detecting from file extension
        #
        # @raise [Sequel::Error] if no paths are provided
        # @raise [Sequel::Error] if multiple different file extensions are provided
        #
        # @example Single file
        #   Pathifier.new("/data/users.parquet")
        #
        # @example Multiple files with same extension
        #   Pathifier.new(["/data/2023.csv", "/data/2024.csv"])
        #
        # @example Override format detection
        #   Pathifier.new("/data/file.txt", using: :csv)
        def initialize(paths, options = Sequel::OPTS)
          @paths = Array(paths).map { |p| Pathname.new(p) }
          @options = options
          validate!
        end

        # Validate paths and extensions.
        #
        # @raise [Sequel::Error] if no paths provided
        # @raise [Sequel::Error] if multiple different file extensions provided
        # @return [void]
        # @api private
        def validate!
          raise Sequel::Error, "No paths provided" if @paths.empty?

          return unless extnames.size > 1

          raise Sequel::Error, "Multiple different file extensions provided: #{extnames.join(", ")}"
        end

        # Get unique file extensions from all paths.
        #
        # @return [Array<String>] Array of unique file extensions (e.g., [".parquet"])
        #
        # @example
        #   pathifier = Pathifier.new(["/data/a.csv", "/data/b.csv"])
        #   pathifier.extnames
        #   # => [".csv"]
        def extnames
          @paths.map(&:extname).uniq
        end

        # Determine the format to use for reading files.
        #
        # Returns the format specified in :using option, or detects format from
        # the file extension of the first path.
        #
        # @return [Symbol] Format symbol (:parquet, :csv, or :json)
        #
        # @example From file extension
        #   pathifier = Pathifier.new("/data/users.parquet")
        #   pathifier.to_format
        #   # => :parquet
        #
        # @example From :using option
        #   pathifier = Pathifier.new("/data/file.txt", using: :csv)
        #   pathifier.to_format
        #   # => :csv
        def to_format
          @options.fetch(:using, extnames.first.delete_prefix(".")).to_sym
        end

        # Generate SQL function call for reading the files.
        #
        # Creates a Sequel function expression that calls the appropriate DuckDB
        # read function (read_parquet, read_csv, or read_json) with an array of
        # file paths.
        #
        # @return [Sequel::SQL::Function] SQL function expression
        #
        # @raise [Sequel::Error] if format is not supported (:parquet, :csv, or :json)
        #
        # @example Single file
        #   pathifier = Pathifier.new("/data/users.parquet")
        #   pathifier.to_sql
        #   # => #<Sequel::SQL::Function @name=>:read_parquet, @args=>[...]>
        #
        # @example Multiple files
        #   pathifier = Pathifier.new(["/data/a.csv", "/data/b.csv"])
        #   sql = pathifier.to_sql
        #   db.literal(sql)
        #   # => "read_csv(['/data/a.csv','/data/b.csv'])"
        def to_sql
          paths_sql = @paths.map { |p| "'#{p}'" }.join(",").then do |paths_arr|
            Sequel::LiteralString.new("[#{paths_arr}]")
          end

          Sequel.function(read_function_name, paths_sql)
        end

        def read_function_name
          case to_format
          when :parquet then :read_parquet
          when :csv then :read_csv
          when :json then :read_json
          else raise Sequel::Error, "Unsupported :using type: #{to_format}"
          end
        end
      end
    end
  end
end
