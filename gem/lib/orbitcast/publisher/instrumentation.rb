# frozen_string_literal: true

module Orbitcast
  module Publisher
    # `ActiveSupport::Notifications` for publishers, without requiring it.
    #
    # Every publish, on every transport, emits one `publish.orbitcast` event
    # with `stream:`, `bytes:` (the encoded payload size) and `via:`
    # (`:postgres` or `:http`). A publish that raises still emits the event —
    # `ActiveSupport::Notifications` adds `:exception` / `:exception_object`
    # to the payload and the error propagates unchanged — so a subscriber sees
    # the failure path, never a silent drop:
    #
    #   ActiveSupport::Notifications.subscribe("publish.orbitcast") do |event|
    #     StatsD.timing("orbitcast.publish", event.duration, tags: ["via:#{event.payload[:via]}"])
    #     Rails.error.report(event.payload[:exception_object]) if event.payload[:exception_object]
    #   end
    #
    # The plain-ruby unit suite has no ActiveSupport; it gets a null
    # instrumenter (or injects a recording one).
    module Instrumentation
      EVENT = "publish.orbitcast"

      # Yields without recording. Same contract as
      # `ActiveSupport::Notifications.instrument`: the block's value is
      # returned, an exception propagates.
      module Null
        def self.instrument(_name, payload = {})
          yield payload
        end
      end

      class << self
        def resolve(instrumenter)
          return instrumenter if instrumenter
          return ::ActiveSupport::Notifications if defined?(::ActiveSupport::Notifications)

          Null
        end

        def publish(instrumenter, stream:, bytes:, via:, &block)
          instrumenter.instrument(EVENT, { stream: stream, bytes: bytes, via: via }, &block)
        end
      end
    end
  end
end
