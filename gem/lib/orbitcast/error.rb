# frozen_string_literal: true

module Orbitcast
  # Base class for every Orbitcast-raised error.
  #
  # All error classes live in this one file on purpose: they are needed by files
  # that must load without the rest of the gem (see `orbitcast/binary.rb`, which
  # the executable wrapper loads before Rails exists). Zeitwerk only requires
  # `Orbitcast::Error` to be defined here; the siblings are plain extra constants
  # and are never expected at their own paths.
  class Error < StandardError; end

  # Raised for unknown keys or invalid values in Orbitcast configuration.
  # Inherits ArgumentError so callers may rescue either.
  class ConfigurationError < ArgumentError; end

  # Raised when the resolved Orbitcast executable is unusable: it cannot be
  # found, or an `ORBITCAST_BINARY` override fails the `doctor --json` check
  # (wrong version, missing feature, unsupported envelope version).
  class BinaryError < Error; end

  # Raised when no usable Orbitcast executable can be resolved.
  class BinaryNotFoundError < BinaryError; end

  # Raised when an encoded notice exceeds PostgreSQL's NOTIFY payload limit.
  class PayloadTooLarge < Error; end

  # Raised when the publisher pool cannot hand out a connection in time.
  class PoolTimeout < Error; end

  # Raised when the overflow relay is enabled but `orbitcast_payloads` does not
  # exist. Orbitcast never creates it implicitly: install the migration.
  class OverflowTableMissing < Error; end

  # Raised when a v1 envelope is not usable: unsupported version, unknown
  # tagged value, missing payload, or a value outside the documented limits.
  # The `field` is the envelope field that failed, for logs and tests.
  class ProtocolError < Error
    attr_reader :field

    def initialize(message, field: nil)
      super(message)
      @field = field
    end
  end

  # Raised when the application asks for a channel construct the relay cannot
  # run (a stream transformation block, a periodic timer). Unsupported uses
  # are detected and fail clearly instead of pretending.
  class UnsupportedChannelFeature < Error; end

  # Raised when a private HTTP publication ingress call fails.
  class IngressError < Error; end
end
