# frozen_string_literal: true

module Sequel
  module DuckDB
    # One ::DuckDB::Database per database file per process.
    #
    # Two ::DuckDB::Database instances on one file in one process do not reliably see
    # each other's writes, and DuckDB's file lock does not stop the second one, because
    # fcntl locks are per process. So every Sequel::Database on a file shares the one
    # instance held here, counted by reference. The instance closes when the last
    # reference is released.
    #
    # This does not use ::DuckDB::InstanceCache. In ruby-duckdb 1.5.5.1 it returns one
    # Ruby object to every holder, and get_or_create hangs when called again after that
    # object was closed (seed sequel-duckdb-k82).
    #
    # The first Sequel::Database to open a file fixes its DuckDB settings until the
    # instance closes. Opening it again with different settings raises SettingsConflict,
    # as the Python, JDBC and R DuckDB clients do.
    #
    # In-memory databases are never registered: each one is separate by design.
    module InstanceRegistry
      Entry = Struct.new(:database, :references, :settings)

      # Raised when a file that is open in this process is opened with different settings.
      class SettingsConflict < Sequel::DatabaseConnectionError
        def initialize(path, held, requested)
          super("#{path} is already open in this process with different DuckDB settings " \
                "(open: #{held.inspect}, requested: #{requested.inspect}). The first " \
                "Sequel::Database to open a file fixes its settings until every " \
                "Sequel::Database on it has disconnected.")
        end
      end

      @mutex = Mutex.new
      @entries = {}
      @pid = Process.pid

      class << self
        # Open a ::DuckDB::Database that is not shared. +settings+ maps DuckDB setting
        # names to values, both Strings (see normalize_settings).
        def open(path, settings)
          return ::DuckDB::Database.open(path) if settings.empty?

          config = ::DuckDB::Config.new
          settings.each { |name, value| config.set_config(name, value) }
          ::DuckDB::Database.open(path, config: config)
        end

        # The settings Hash in the one form that acquire compares.
        def normalize_settings(settings)
          (settings || {}).to_h { |name, value| [ name.to_s, value.to_s ] }
        end

        # Return the shared ::DuckDB::Database for +path+ and take one reference to it.
        # Raises SettingsConflict when +path+ is already open with other +settings+.
        def acquire(path, settings = {})
          key = key_for(path)
          @mutex.synchronize do
            forget_parent_entries
            entry = (@entries[key] ||= Entry.new(open(path, settings), 0, settings))
            raise SettingsConflict.new(path, entry.settings, settings) unless entry.settings == settings

            entry.references += 1
            entry.database
          end
        end

        # Give back one reference taken by acquire. The last one closes the instance.
        def release(path)
          key = key_for(path)
          @mutex.synchronize do
            forget_parent_entries
            entry = @entries[key]
            return unless entry

            entry.references -= 1
            return if entry.references.positive?

            @entries.delete(key)
            entry.database.close
          end
        end

        # The number of references held on +path+, 0 when it is not open.
        def references(path)
          @mutex.synchronize do
            forget_parent_entries
            @entries[key_for(path)]&.references || 0
          end
        end

        private

        # The real path of the file's directory, so two spellings of one file share an
        # entry. The file itself may not exist yet.
        def key_for(path)
          File.join(File.realpath(File.dirname(path)), File.basename(path))
        rescue SystemCallError
          File.expand_path(path)
        end

        # A forked child inherits the parent's entries, but the instances belong to the
        # parent. The child opens its own, as it did before this registry existed.
        def forget_parent_entries
          return if @pid == Process.pid

          @entries = {}
          @pid = Process.pid
        end
      end
    end
  end
end
