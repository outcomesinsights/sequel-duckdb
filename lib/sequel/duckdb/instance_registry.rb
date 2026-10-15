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
    # In-memory databases never come here: each one is separate by design.
    module InstanceRegistry
      Entry = Struct.new(:database, :references)

      @mutex = Mutex.new
      @entries = {}
      @pid = Process.pid

      class << self
        # Return the shared ::DuckDB::Database for +path+ and take one reference to it.
        def acquire(path)
          key = key_for(path)
          @mutex.synchronize do
            forget_parent_entries
            entry = (@entries[key] ||= Entry.new(::DuckDB::Database.open(path), 0))
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
