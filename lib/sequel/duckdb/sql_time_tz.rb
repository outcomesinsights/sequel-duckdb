# frozen_string_literal: true

module Sequel
  module DuckDB
    # A TIMETZ value: a time of day with a UTC offset.
    #
    # Sequel::SQLTime has no offset, so a TIMETZ value written as one gets DuckDB's
    # session time zone. This subclass keeps the offset and is written as
    # 'HH:MM:SS.ffffff+HH:MM'. Like Sequel::SQLTime, its date is Sequel::SQLTime.date,
    # so two values with the same time and offset are equal.
    class SQLTimeTZ < Sequel::SQLTime
      # The time of day and UTC offset of +time+, on Sequel::SQLTime.date.
      def self.from_time(time)
        date = Sequel::SQLTime.date
        new(date.year, date.month, date.day, time.hour, time.min,
            time.sec + Rational(time.nsec, 1_000_000_000), time.utc_offset)
      end

      # Two values with the same instant but different offsets are different TIMETZ
      # values, so a model that is assigned a new offset reports the column as changed.
      def ==(other)
        super && other.respond_to?(:utc_offset) && utc_offset == other.utc_offset
      end

      # HH:MM:SS+HH:MM
      def to_s(*args)
        args.empty? ? strftime("%H:%M:%S%:z") : super
      end
    end
  end
end
